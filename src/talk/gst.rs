use crate::AnyResult;
use anyhow::{anyhow, Context, Result};
use gstreamer::{
    element_error, parse::launch_full, prelude::*, Caps, ClockTime, FlowError, FlowSuccess,
    MessageView, ParseFlags, Pipeline, ResourceError, State,
};
use gstreamer_app::{AppSink, AppSinkCallbacks};
use std::path::Path;
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinSet,
};

use byte_slice_cast::*;

/// Where the sound comes from
#[derive(Clone, Copy)]
pub(super) enum Input<'a> {
    /// A sound file
    File(&'a Path),
    /// A GStreamer source, such as a microphone: "alsasrc device=hw:1"
    Source(&'a str),
}

/// Plays `input` into ADPCM as the camera wants it, which the receiver gets.
/// The task ends with the sound, or with what stopped it.
#[allow(clippy::type_complexity)]
pub(super) fn from_input(
    input: Input,
    volume: f32,
    block_align: u16,
    sample_rate: u16,
) -> Result<(JoinSet<AnyResult<()>>, Receiver<Vec<u8>>)> {
    let pipeline = create_pipeline(input, volume, block_align, sample_rate)?;
    start(pipeline)
}

#[allow(clippy::type_complexity)]
fn start(pipeline: Pipeline) -> Result<(JoinSet<AnyResult<()>>, Receiver<Vec<u8>>)> {
    let appsink = get_sink(&pipeline)?;
    let (tx, rx) = channel(30);
    let mut set = JoinSet::<AnyResult<()>>::new();
    set_data_channel(&appsink, tx);

    set.spawn_blocking(move || start_pipeline(pipeline));

    Ok((set, rx))
}

fn start_pipeline(pipeline: Pipeline) -> Result<()> {
    pipeline.set_state(State::Playing)?;

    let bus = pipeline
        .bus()
        .expect("Pipeline without bus. Shouldn't happen!");

    let mut result = Ok(());
    for msg in bus.iter_timed(ClockTime::NONE) {
        match msg.view() {
            MessageView::Eos(..) => break,
            MessageView::Error(err) => {
                // Nothing comes after an error, the end of the stream either:
                // waiting for it hung talk for good (#318)
                result = Err(anyhow!("{} ({:?})", err.error(), err.debug()));
                break;
            }
            _ => (),
        }
    }

    pipeline
        .set_state(State::Null)
        .context("Error in gstreamer when setting state to Null")?;

    result
}

fn get_sink(pipeline: &Pipeline) -> Result<AppSink> {
    let sink = pipeline
        .by_name("thesink")
        .expect("There shoud be a `thesink`");
    sink.dynamic_cast::<AppSink>()
        .map_err(|_| anyhow!("Cannot find appsink in gstreamer, check your gstreamer plugins"))
}

fn set_data_channel(appsink: &AppSink, tx: Sender<Vec<u8>>) {
    // Getting data out of the appsink is done by setting callbacks on it.
    // The appsink will then call those handlers, as soon as data is available.
    appsink.set_callbacks(
        AppSinkCallbacks::builder()
            // Add a handler to the "new-sample" signal.
            .new_sample(move |appsink| {
                // Pull the sample in question out of the appsink's buffer.
                let sample = appsink.pull_sample().map_err(|_| FlowError::Eos)?;
                let buffer = sample.buffer().ok_or_else(|| {
                    element_error!(
                        appsink,
                        ResourceError::Failed,
                        ("Failed to get buffer from appsink")
                    );

                    FlowError::Error
                })?;

                // At this point, buffer is only a reference to an existing memory region somewhere.
                // When we want to access its content, we have to map it while requesting the required
                // mode of access (read, read/write).
                // This type of abstraction is necessary, because the buffer in question might not be
                // on the machine's main memory itself, but rather in the GPU's memory.
                // So mapping the buffer makes the underlying memory region accessible to us.
                // See: https://gstreamer.freedesktop.org/documentation/plugin-development/advanced/allocation.html
                let map = buffer.map_readable().map_err(|_| {
                    element_error!(
                        appsink,
                        ResourceError::Failed,
                        ("Failed to map buffer readable")
                    );

                    FlowError::Error
                })?;

                // We know what format the data in the memory region has, since we requested
                // it by setting the appsink's caps. So what we do here is interpret the
                // memory region we mapped as an array of signed 8 bit integers.
                let samples = map.as_slice_of::<u8>().map_err(|_| {
                    element_error!(
                        appsink,
                        ResourceError::Failed,
                        ("Failed to interprete buffer as u8 ADPCM")
                    );

                    FlowError::Error
                })?;

                // Ready! Waits while talk is behind, which paces the
                // pipeline, and ends it once talk stopped
                tx.blocking_send(samples.to_vec())
                    .map_err(|_| FlowError::Eos)?;

                Ok(FlowSuccess::Ok)
            })
            .build(),
    );
}

fn create_pipeline(
    input: Input,
    volume: f32,
    block_align: u16,
    sample_rate: u16,
) -> Result<Pipeline> {
    gstreamer::init()
        .context("Unable to start gstreamer ensure it and all plugins are installed")?;

    let source = match input {
        // Its location is set below. In the text of the pipeline, a path
        // with a space or a quote in it broke the pipeline.
        Input::File(_) => "filesrc name=thesource",
        Input::Source(source) => source,
    };

    let launch_str = format!(
        "{} \
        ! decodebin \
        ! audioconvert \
        ! audioresample \
        ! audio/x-raw,rate={},channels=1 \
        ! volume volume={:.2} \
        ! queue  \
        ! adpcmenc blockalign={} layout=dvi \
        ! appsink name=thesink",
        source, sample_rate, volume, block_align
    );

    log::info!("{}", launch_str);

    // Parse the pipeline we want to probe from a static in-line string.
    // Here we give our audiotestsrc a name, so we can retrieve that element
    // from the resulting pipeline.
    let pipeline = launch_full(&launch_str, None, ParseFlags::empty())
        .context("Unable to load gstreamer pipeline ensure all gstramer plugins are installed")?;
    let pipeline = pipeline.dynamic_cast::<Pipeline>().map_err(|_| {
        anyhow!("Unable to create gstreamer pipeline ensure all gstramer plugins are installed")
    })?;

    if let Input::File(path) = input {
        let location = path
            .to_str()
            .ok_or_else(|| anyhow!("The file path {path:?} is not UTF-8"))?;
        pipeline
            .by_name("thesource")
            .expect("There should be `thesource`")
            .set_property("location", location);
    }

    let appsink = get_sink(&pipeline)?;

    // Tell the appsink what format we want. It will then be the audiotestsrc's job to
    // provide the format we request.
    // This can be set after linking the two objects, because format negotiation between
    // both elements will happen during pre-rolling of the pipeline.
    appsink.set_caps(Some(
        &Caps::builder("audio/x-adpcm")
            .field("layout", "dvi")
            .field("block_align", block_align as i32)
            .field("channels", 1i32)
            .field("rate", sample_rate as i32)
            .build(),
    ));

    Ok(pipeline)
}
