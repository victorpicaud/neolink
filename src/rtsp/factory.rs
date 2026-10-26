use gstreamer::ClockTime;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use gstreamer::{prelude::*, Bin, Caps, Element, ElementFactory, FlowError, GhostPad};
use gstreamer_app::{AppSrc, AppSrcCallbacks, AppStreamType};
use neolink_core::{
    bc_protocol::StreamKind,
    bcmedia::model::{
        BcMedia, BcMediaIframe, BcMediaInfoV1, BcMediaInfoV2, BcMediaPframe, VideoType,
    },
};
use tokio::{sync::mpsc::channel as mpsc, task::JoinHandle};

use crate::{common::NeoInstance, rtsp::gst::NeoMediaFactory, AnyResult};

#[derive(Clone, Debug)]
pub enum AudioType {
    Aac,
    Adpcm(u32),
}

#[derive(Clone, Debug)]
struct StreamConfig {
    #[allow(dead_code)]
    resolution: [u32; 2],
    bitrate: u32,
    fps: u32,
    bitrate_table: Vec<u32>,
    fps_table: Vec<u32>,
    vid_type: Option<VideoType>,
    aud_type: Option<AudioType>,
}
impl StreamConfig {
    async fn new(instance: &NeoInstance, name: StreamKind) -> AnyResult<Self> {
        let (resolution, bitrate, fps, fps_table, bitrate_table) = instance
            .run_passive_task(|cam| {
                Box::pin(async move {
                    let infos = cam
                        .get_stream_info()
                        .await?
                        .stream_infos
                        .iter()
                        .flat_map(|info| info.encode_tables.clone())
                        .collect::<Vec<_>>();
                    if let Some(encode) =
                        infos.iter().find(|encode| encode.name == name.to_string())
                    {
                        let bitrate_table = encode
                            .bitrate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();
                        let framerate_table = encode
                            .framerate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();

                        Ok((
                            [encode.resolution.width, encode.resolution.height],
                            bitrate_table
                                .get(encode.default_bitrate as usize)
                                .copied()
                                .unwrap_or(encode.default_bitrate)
                                * 1024,
                            framerate_table
                                .get(encode.default_framerate as usize)
                                .copied()
                                .unwrap_or(encode.default_framerate),
                            framerate_table.clone(),
                            bitrate_table.clone(),
                        ))
                    } else {
                        Ok(([0, 0], 0, 0, vec![], vec![]))
                    }
                })
            })
            .await?;

        Ok(StreamConfig {
            resolution,
            bitrate,
            fps,
            fps_table,
            bitrate_table,
            vid_type: None,
            aud_type: None,
        })
    }

    /// Frame rate to assume when the camera did not report a usable one
    const DEFAULT_FPS: u32 = 25;

    /// The frame rate, never zero
    fn effective_fps(&self) -> u32 {
        if self.fps > 0 {
            self.fps
        } else {
            Self::DEFAULT_FPS
        }
    }

    /// Time between two video frames
    fn frame_period(&self) -> Duration {
        Duration::from_micros(1_000_000 / u64::from(self.effective_fps()))
    }

    fn update_fps(&mut self, fps: u32) {
        let new_fps = self.fps_table.get(fps as usize).copied().unwrap_or(fps);
        self.fps = new_fps;
    }
    #[allow(dead_code)]
    fn update_bitrate(&mut self, bitrate: u32) {
        let new_bitrate = self
            .bitrate_table
            .get(bitrate as usize)
            .copied()
            .unwrap_or(bitrate);
        self.bitrate = new_bitrate;
    }

    fn update_from_media(&mut self, media: &BcMedia) {
        match media {
            BcMedia::InfoV1(BcMediaInfoV1 { fps, .. })
            | BcMedia::InfoV2(BcMediaInfoV2 { fps, .. }) => self.update_fps(*fps as u32),
            BcMedia::Aac(_) => {
                self.aud_type = Some(AudioType::Aac);
            }
            BcMedia::Adpcm(adpcm) => {
                self.aud_type = Some(AudioType::Adpcm(adpcm.block_size()));
            }
            BcMedia::Iframe(BcMediaIframe { video_type, .. })
            | BcMedia::Pframe(BcMediaPframe { video_type, .. }) => {
                self.vid_type = Some(*video_type);
            }
        }
    }
}

pub(super) async fn make_dummy_factory(
    use_splash: bool,
    pattern: String,
) -> AnyResult<NeoMediaFactory> {
    NeoMediaFactory::new_with_callback(move |element| {
        clear_bin(&element)?;
        if !use_splash {
            Ok(None)
        } else {
            build_unknown(&element, &pattern)?;
            Ok(Some(element))
        }
    })
    .await
}

enum ClientMsg {
    NewClient {
        element: Element,
        reply: tokio::sync::oneshot::Sender<Element>,
    },
}

pub(super) async fn make_factory(
    camera: NeoInstance,
    stream: StreamKind,
) -> AnyResult<(NeoMediaFactory, JoinHandle<AnyResult<()>>)> {
    let (client_tx, mut client_rx) = mpsc(100);
    // Create the task that creates the pipelines
    let thread = tokio::task::spawn(async move {
        let name = camera.config().await?.borrow().name.clone();

        while let Some(msg) = client_rx.recv().await {
            match msg {
                ClientMsg::NewClient { element, reply } => {
                    log::debug!("New client for {name}::{stream}");
                    let camera = camera.clone();
                    let name = name.clone();
                    tokio::task::spawn(async move {
                        clear_bin(&element)?;
                        log::trace!("{name}::{stream}: Starting camera");

                        // Start the camera
                        let config = camera.config().await?.borrow().clone();
                        let mut media_rx = camera.stream_while_live(stream).await?;

                        log::trace!("{name}::{stream}: Learning camera stream type");
                        // Learn the camera data type
                        let mut buffer = vec![];
                        let mut frame_count = 0usize;

                        let mut stream_config = StreamConfig::new(&camera, stream).await?;
                        while let Some(media) = media_rx.recv().await {
                            stream_config.update_from_media(&media);
                            buffer.push(media);
                            if frame_count > 10
                                || (stream_config.vid_type.is_some()
                                    && stream_config.aud_type.is_some())
                            {
                                break;
                            }
                            frame_count += 1;
                        }

                        log::trace!("{name}::{stream}: Building the pipeline");
                        // Build the right video pipeline
                        let vid_src = match stream_config.vid_type.as_ref() {
                            Some(VideoType::H264) => {
                                let src = build_h264(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            Some(VideoType::H265) => {
                                let src = build_h265(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            None => {
                                build_unknown(&element, &config.splash_pattern.to_string())?;
                                AnyResult::Ok(None)
                            }
                        }?;

                        // Build the right audio pipeline
                        let aud_src = match stream_config.aud_type.as_ref() {
                            Some(AudioType::Aac) => {
                                let src = build_aac(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            Some(AudioType::Adpcm(block_size)) => {
                                let src = build_adpcm(&element, *block_size, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            None => AnyResult::Ok(None),
                        }?;

                        if let Some(app) = vid_src.as_ref() {
                            app.set_callbacks(
                                AppSrcCallbacks::builder()
                                    .seek_data(move |_, _seek_pos| true)
                                    .build(),
                            );
                        }
                        if let Some(app) = aud_src.as_ref() {
                            app.set_callbacks(
                                AppSrcCallbacks::builder()
                                    .seek_data(move |_, _seek_pos| true)
                                    .build(),
                            );
                        }

                        log::trace!("{name}::{stream}: Sending pipeline to gstreamer");
                        // Send the pipeline back to the factory so it can start
                        let _ = reply.send(element);

                        // Run blocking code on a seperate thread
                        // This is not an async thread
                        std::thread::spawn(move || {
                            let mut state = PumpState::default();

                            log::trace!("{name}::{stream}: Sending buffered frames");
                            for buffered in buffer.drain(..) {
                                send_to_sources(
                                    buffered,
                                    &vid_src,
                                    &aud_src,
                                    &mut state,
                                    &stream_config,
                                )?;
                            }

                            log::trace!("{name}::{stream}: Sending new frames");
                            while let Some(data) = media_rx.blocking_recv() {
                                let r = send_to_sources(
                                    data,
                                    &vid_src,
                                    &aud_src,
                                    &mut state,
                                    &stream_config,
                                );
                                if let Err(r) = &r {
                                    log::info!("Failed to send to source: {r:?}");
                                }
                                r?;
                            }
                            log::trace!("All media recieved");
                            AnyResult::Ok(())
                        });
                        AnyResult::Ok(())
                    });
                }
            }
        }
        AnyResult::Ok(())
    });

    // Now setup the factory
    let factory = NeoMediaFactory::new_with_callback(move |element| {
        let (reply, new_element) = tokio::sync::oneshot::channel();
        client_tx.blocking_send(ClientMsg::NewClient { element, reply })?;

        let element = new_element.blocking_recv()?;
        Ok(Some(element))
    })
    .await?;
    Ok((factory, thread))
}

/// Per-client state of the frame pump
#[derive(Default)]
struct PumpState {
    /// Timestamp of the next video buffer in microseconds. A u32 wrapped after
    /// 71.6 minutes, and the jump back to zero froze the stream.
    vid_ts: u64,
    /// Timestamp of the next audio buffer in microseconds
    aud_ts: u64,
    /// Video was dropped, so P-frames are skipped until the next keyframe: a
    /// decoder cannot use them without the frames they reference.
    need_keyframe: bool,
}

fn send_to_sources(
    data: BcMedia,
    vid_src: &Option<AppSrc>,
    aud_src: &Option<AppSrc>,
    state: &mut PumpState,
    stream_config: &StreamConfig,
) -> AnyResult<()> {
    match data {
        BcMedia::Aac(aac) => {
            let Some(duration) = aac.duration() else {
                log::debug!("Dropping AAC frame with an unreadable header");
                return Ok(());
            };
            if let Some(aud_src) = aud_src.as_ref() {
                send_audio(aud_src, aac.data, state.aud_ts)?;
            }
            state.aud_ts += u64::from(duration);
        }
        BcMedia::Adpcm(adpcm) => {
            let Some(duration) = adpcm.duration() else {
                log::debug!("Dropping ADPCM frame with an unreadable header");
                return Ok(());
            };
            if let Some(aud_src) = aud_src.as_ref() {
                send_audio(aud_src, adpcm.data, state.aud_ts)?;
            }
            state.aud_ts += u64::from(duration);
        }
        BcMedia::Iframe(BcMediaIframe { data, .. }) => {
            send_video(vid_src, data, true, state)?;
            state.vid_ts += stream_config.frame_period().as_micros() as u64;
        }
        BcMedia::Pframe(BcMediaPframe { data, .. }) => {
            send_video(vid_src, data, false, state)?;
            state.vid_ts += stream_config.frame_period().as_micros() as u64;
        }
        _ => {}
    }
    Ok(())
}

fn send_audio(aud_src: &AppSrc, data: Vec<u8>, ts: u64) -> AnyResult<()> {
    if is_full(aud_src) {
        log::trace!("Audio source is full, dropping a frame");
        return Ok(());
    }
    log::trace!("Sending audio: {:?}", Duration::from_micros(ts));
    send_to_appsrc(aud_src, data, Duration::from_micros(ts))
}

fn send_video(
    vid_src: &Option<AppSrc>,
    data: Vec<u8>,
    keyframe: bool,
    state: &mut PumpState,
) -> AnyResult<()> {
    let Some(vid_src) = vid_src.as_ref() else {
        return Ok(());
    };
    if state.need_keyframe && !keyframe {
        log::trace!("Waiting for a keyframe, dropping a P-frame");
        return Ok(());
    }
    if is_full(vid_src) {
        log::trace!("Video source is full, dropping a frame");
        state.need_keyframe = true;
        return Ok(());
    }
    log::trace!("Sending video: {:?}", Duration::from_micros(state.vid_ts));
    send_to_appsrc(vid_src, data, Duration::from_micros(state.vid_ts))?;
    if keyframe {
        state.need_keyframe = false;
    }
    Ok(())
}

/// The appsrcs are non-blocking, so they would queue without limit when the
/// client falls behind. Frames are dropped instead once `max-bytes` is
/// reached.
fn is_full(appsrc: &AppSrc) -> bool {
    let max = appsrc.max_bytes();
    max > 0 && appsrc.current_level_bytes() >= max
}

fn send_to_appsrc(appsrc: &AppSrc, data: Vec<u8>, mut ts: Duration) -> AnyResult<()> {
    check_live(appsrc)?; // Stop if appsrc is dropped

    // In live mode we follow the advice in
    // https://gstreamer.freedesktop.org/documentation/additional/design/element-source.html?gi-language=c#live-sources
    // Only push buffers when in play state and have a clock
    // we also timestamp at the current time
    if appsrc.is_live() {
        if let Some(time) = appsrc
            .current_clock_time()
            .and_then(|t| appsrc.base_time().map(|bt| t - bt))
        {
            if matches!(appsrc.current_state(), gstreamer::State::Playing) {
                ts = Duration::from_micros(time.useconds());
            } else {
                // Not playing
                return Ok(());
            }
        } else {
            // Clock not up yet
            return Ok(());
        }
    }

    // Wrap the frame's own allocation: no copy, and it is freed as soon as the
    // pipeline is done with it. (A pool per distinct frame size used to be
    // kept here, which leaked memory and file descriptors without bound.)
    let mut buf = gstreamer::Buffer::from_mut_slice(data);
    {
        let buf = buf
            .get_mut()
            .ok_or_else(|| anyhow!("New buffer is not writable"))?;
        let time = ClockTime::from_useconds(ts.as_micros() as u64);
        buf.set_dts(time);
        buf.set_pts(time);
    }

    // Push buffer into the appsrc
    match appsrc.push_buffer(buf) {
        Ok(_) => Ok(()),
        Err(FlowError::Flushing) => {
            // Buffer is full just skip
            log::info!(
                "Buffer full on {} pausing stream until client consumes frames",
                appsrc.name()
            );
            Ok(())
        }
        Err(e) => Err(anyhow!("Error in streaming: {e:?}")),
    }
}
fn check_live(app: &AppSrc) -> Result<()> {
    app.bus().ok_or(anyhow!("App source is closed"))?;
    app.pads()
        .iter()
        .all(|pad| pad.is_linked())
        .then_some(())
        .ok_or(anyhow!("App source is not linked"))
}

fn clear_bin(bin: &Element) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    // Clear the autogenerated ones
    for element in bin.iterate_elements().into_iter().flatten() {
        bin.remove(&element)?;
    }

    Ok(())
}

fn build_unknown(bin: &Element, pattern: &str) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Unknown Pipeline");
    let source = make_element("videotestsrc", "testvidsrc")?;
    source.set_property_from_str("pattern", pattern);
    source.set_property("num-buffers", 500i32); // Send buffers then EOS
    let queue = make_queue("queue0", 1024 * 1024 * 4)?;

    let overlay = make_element("textoverlay", "overlay")?;
    overlay.set_property("text", "Stream not Ready");
    overlay.set_property_from_str("valignment", "top");
    overlay.set_property_from_str("halignment", "left");
    overlay.set_property("font-desc", "Sans, 16");
    let encoder = make_element("jpegenc", "encoder")?;
    let payload = make_element("rtpjpegpay", "pay0")?;

    bin.add_many([&source, &queue, &overlay, &encoder, &payload])?;
    source.link_filtered(
        &queue,
        &Caps::builder("video/x-raw")
            .field("format", "YUY2")
            .field("width", 896i32)
            .field("height", 512i32)
            .field("framerate", gstreamer::Fraction::new(25, 1))
            .build(),
    )?;
    Element::link_many([&queue, &overlay, &encoder, &payload])?;

    Ok(())
}

struct Linked {
    appsrc: AppSrc,
    output: Element,
}

fn pipe_h264(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    log::debug!(
        "buffer_size: {buffer_size}, bitrate: {}",
        stream_config.bitrate
    );
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H264 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(stream_config.frame_period().as_nanos() as i64);
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h264parse", "parser")?;
    // let stamper = make_element("h264timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser])?;
    Element::link_many([&source, &queue, &parser])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: parser,
    })
}

fn build_h264(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h264(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph264pay", "pay0")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_h265(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H265 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(stream_config.frame_period().as_nanos() as i64);
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h265parse", "parser")?;
    // let stamper = make_element("h265timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser])?;
    Element::link_many([&source, &queue, &parser])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: parser,
    })
}

fn build_h265(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h265(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph265pay", "pay0")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_aac(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    // Audio seems to run at about 800kbs
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Aac pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(stream_config.frame_period().as_nanos() as i64);
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let parser = make_element("aacparse", "audparser")?;
    let decoder = match make_element("faad", "auddecoder_faad") {
        Ok(ele) => Ok(ele),
        Err(_) => make_element("avdec_aac", "auddecoder_avdec_aac"),
    }?;

    // The fallback
    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
    let fallback_switch = make_element("fallbackswitch", "audfallbackswitch");
    if let Ok(fallback_switch) = fallback_switch.as_ref() {
        fallback_switch.set_property("timeout", 3u64 * 1_000_000_000u64);
        fallback_switch.set_property("immediate-fallback", true);
    }

    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &queue, &parser, &decoder, &encoder])?;
    if let Ok(fallback_switch) = fallback_switch.as_ref() {
        bin.add_many([&silence, fallback_switch])?;
        Element::link_many([
            &source,
            &queue,
            &parser,
            &decoder,
            fallback_switch,
            &encoder,
        ])?;
        Element::link_many([&silence, fallback_switch])?;
    } else {
        Element::link_many([&source, &queue, &parser, &decoder, &encoder])?;
    }

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

fn build_aac(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_aac(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtpL16pay", "pay1")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_adpcm(bin: &Element, block_size: u32, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Adpcm pipeline");
    // Original command line
    // caps=audio/x-adpcm,layout=dvi,block_align={},channels=1,rate=8000
    // ! queue silent=true max-size-bytes=10485760 min-threshold-bytes=1024
    // ! adpcmdec
    // ! audioconvert
    // ! rtpL16pay name=pay1

    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(stream_config.frame_period().as_nanos() as i64);
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    source.set_caps(Some(
        &Caps::builder("audio/x-adpcm")
            .field("layout", "dvi")
            .field("block_align", block_size as i32)
            .field("channels", 1i32)
            .field("rate", 8000i32)
            .build(),
    ));

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let decoder = make_element("decodebin", "auddecoder")?;
    let encoder = make_element("audioconvert", "audencoder")?;
    let encoder_out = encoder.clone();

    bin.add_many([&source, &queue, &decoder, &encoder])?;
    Element::link_many([&source, &queue, &decoder])?;
    // This runs on a GStreamer thread: a panic here would abort the process
    decoder.connect_pad_added(move |_element, pad| {
        let Some(sink_pad) = encoder.static_pad("sink") else {
            log::warn!("ADPCM encoder is missing its sink pad");
            return;
        };
        if sink_pad.is_linked() {
            return;
        }
        if let Err(e) = pad.link(&sink_pad) {
            log::warn!("Failed to link ADPCM decoder to encoder: {e:?}");
        }
    });

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder_out,
    })
}

fn build_adpcm(bin: &Element, block_size: u32, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_adpcm(bin, block_size, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtpL16pay", "pay1")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

#[allow(dead_code)]
fn pipe_silence(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    // Audio seems to run at about 800kbs
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Silence pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(stream_config.frame_period().as_nanos() as i64);
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let sink_queue = make_queue("audsinkqueue", buffer_size)?;
    let sink = make_element("fakesink", "silence_sink")?;

    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
    let src_queue = make_queue("audsinkqueue", buffer_size)?;
    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &sink_queue, &sink, &silence, &src_queue, &encoder])?;

    Element::link_many([&source, &sink_queue, &sink])?;

    Element::link_many([&silence, &src_queue, &encoder])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

#[allow(dead_code)]
struct AppSrcPair {
    vid: AppSrc,
    aud: Option<AppSrc>,
}

// #[allow(dead_code)]
// /// Experimental build a stream of MPEGTS
// fn build_mpegts(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrcPair> {
//     let buffer_size = buffer_size(stream_config.bitrate);
//     log::debug!(
//         "buffer_size: {buffer_size}, bitrate: {}",
//         stream_config.bitrate
//     );

//     // VID
//     let vid_link = match stream_config.vid_format {
//         VidFormat::H264 => pipe_h264(bin, stream_config)?,
//         VidFormat::H265 => pipe_h265(bin, stream_config)?,
//         VidFormat::None => unreachable!(),
//     };

//     // AUD
//     let aud_link = match stream_config.aud_format {
//         AudFormat::Aac => pipe_aac(bin, stream_config)?,
//         AudFormat::Adpcm(block) => pipe_adpcm(bin, block, stream_config)?,
//         AudFormat::None => pipe_silence(bin, stream_config)?,
//     };

//     let bin = bin
//         .clone()
//         .dynamic_cast::<Bin>()
//         .map_err(|_| anyhow!("Media source's element should be a bin"))?;

//     // MUX
//     let muxer = make_element("mpegtsmux", "mpeg_muxer")?;
//     let rtp = make_element("rtpmp2tpay", "pay0")?;

//     bin.add_many([&muxer, &rtp])?;
//     Element::link_many([&vid_link.output, &muxer, &rtp])?;
//     Element::link_many([&aud_link.output, &muxer])?;

//     Ok(AppSrcPair {
//         vid: vid_link.appsrc,
//         aud: Some(aud_link.appsrc),
//     })
// }

// Convenice funcion to make an element or provide a message
// about what plugin is missing
fn make_element(kind: &str, name: &str) -> AnyResult<Element> {
    ElementFactory::make_with_name(kind, Some(name)).with_context(|| {
        let plugin = match kind {
            "appsrc" => "app (gst-plugins-base)",
            "audioconvert" => "audioconvert (gst-plugins-base)",
            "adpcmdec" => "Required for audio",
            "h264parse" => "videoparsersbad (gst-plugins-bad)",
            "h265parse" => "videoparsersbad (gst-plugins-bad)",
            "rtph264pay" => "rtp (gst-plugins-good)",
            "rtph265pay" => "rtp (gst-plugins-good)",
            "rtpjitterbuffer" => "rtp (gst-plugins-good)",
            "aacparse" => "audioparsers (gst-plugins-good)",
            "rtpL16pay" => "rtp (gst-plugins-good)",
            "x264enc" => "x264 (gst-plugins-ugly)",
            "x265enc" => "x265 (gst-plugins-bad)",
            "avdec_h264" => "libav (gst-libav)",
            "avdec_h265" => "libav (gst-libav)",
            "videotestsrc" => "videotestsrc (gst-plugins-base)",
            "imagefreeze" => "imagefreeze (gst-plugins-good)",
            "audiotestsrc" => "audiotestsrc (gst-plugins-base)",
            "decodebin" => "playback (gst-plugins-good)",
            _ => "Unknown",
        };
        format!(
            "Missing required gstreamer plugin `{}` for `{}` element",
            plugin, kind
        )
    })
}

#[allow(dead_code)]
fn make_dbl_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    // queue.set_property(
    //     "max-size-time",
    //     std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
    //         .unwrap_or(0),
    // );

    let queue2 = make_element("queue2", &format!("queue2_{}", name))?;
    queue2.set_property("max-size-bytes", buffer_size * 2u32 / 3u32);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    queue2.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
    queue2.set_property("use-buffering", false);

    let bin = gstreamer::Bin::builder().name(name).build();
    bin.add_many([&queue, &queue2])?;
    Element::link_many([&queue, &queue2])?;

    let pad = queue
        .static_pad("sink")
        .expect("Failed to get a static pad from queue.");
    let ghost_pad = GhostPad::builder_with_target(&pad).unwrap().build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let pad = queue2
        .static_pad("src")
        .expect("Failed to get a static pad from queue2.");
    let ghost_pad = GhostPad::builder_with_target(&pad).unwrap().build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let bin = bin
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot convert bin"))?;
    Ok(bin)
}

fn make_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    queue.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
    Ok(queue)
}

fn buffer_size(bitrate: u32) -> u32 {
    // 2 seconds of data at the stream's bitrate, but at least 1 MiB so a few
    // keyframes still fit when the camera did not report a bitrate
    std::cmp::max(bitrate.saturating_mul(2) / 8, 1024 * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;
    use neolink_core::bcmedia::model::{BcMediaAac, BcMediaAdpcm, VideoType};

    fn stream_config(fps: u32) -> StreamConfig {
        StreamConfig {
            resolution: [640, 360],
            bitrate: 1_000_000,
            fps,
            bitrate_table: vec![],
            fps_table: vec![],
            vid_type: Some(VideoType::H264),
            aud_type: None,
        }
    }

    fn frame(keyframe: bool, len: usize) -> BcMedia {
        if keyframe {
            BcMedia::Iframe(BcMediaIframe {
                video_type: VideoType::H264,
                microseconds: 0,
                time: None,
                data: vec![0x5a; len],
            })
        } else {
            BcMedia::Pframe(BcMediaPframe {
                video_type: VideoType::H264,
                microseconds: 0,
                data: vec![0x5a; len],
            })
        }
    }

    /// `appsrc ! fakesink`, configured like the real video sources
    fn appsrc_pipeline(max_bytes: u64) -> (gstreamer::Pipeline, AppSrc) {
        gstreamer::init().unwrap();
        let pipeline = gstreamer::Pipeline::new();
        let src = make_element("appsrc", "src")
            .unwrap()
            .dynamic_cast::<AppSrc>()
            .unwrap();
        src.set_is_live(false);
        src.set_block(false);
        src.set_max_bytes(max_bytes);
        src.set_stream_type(AppStreamType::Stream);
        let sink = make_element("fakesink", "sink").unwrap();
        sink.set_property("sync", false);
        pipeline.add_many([src.upcast_ref(), &sink]).unwrap();
        src.link(&sink).unwrap();
        (pipeline, src)
    }

    /// Compressed frames are a different size almost every time. A buffer
    /// pool used to be created per distinct size and kept for the client's
    /// lifetime: about 71 MiB for these 2048 sizes, plus two file descriptors
    /// per pool on GStreamer before 1.26 (#422).
    #[test]
    #[cfg(target_os = "linux")]
    fn variable_frame_sizes_keep_memory_and_fds_bounded() {
        let fds = || std::fs::read_dir("/proc/self/fd").unwrap().count();
        let rss = || {
            let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
            let pages: u64 = statm.split_whitespace().nth(1).unwrap().parse().unwrap();
            pages * 4096
        };
        let (pipeline, src) = appsrc_pipeline(64 * 1024 * 1024);
        pipeline.set_state(gstreamer::State::Playing).unwrap();
        let (fds_before, rss_before) = (fds(), rss());

        let src = Some(src);
        let mut state = PumpState::default();
        for i in 0..2048 {
            let len = 257 + i * 31;
            send_to_sources(
                frame(false, len),
                &src,
                &None,
                &mut state,
                &stream_config(25),
            )
            .unwrap();
        }
        let (fds_after, rss_after) = (fds(), rss());
        pipeline.set_state(gstreamer::State::Null).unwrap();

        assert!(
            fds_after <= fds_before + 32,
            "file descriptors grew from {fds_before} to {fds_after}"
        );
        let grown = rss_after.saturating_sub(rss_before);
        assert!(
            grown < 32 * 1024 * 1024,
            "resident memory grew by {} MiB",
            grown / (1024 * 1024)
        );
    }

    #[test]
    fn timestamps_pass_the_u32_microsecond_boundary() {
        let start = u64::from(u32::MAX - 100);
        let mut state = PumpState {
            vid_ts: start,
            aud_ts: start,
            need_keyframe: false,
        };
        send_to_sources(
            frame(false, 16),
            &None,
            &None,
            &mut state,
            &stream_config(25),
        )
        .unwrap();
        assert_eq!(state.vid_ts, start + 40_000);

        let aac = BcMedia::Aac(BcMediaAac {
            data: vec![0xff, 0xf1, 0x60, 0, 0, 0, 0, 0],
        });
        send_to_sources(aac, &None, &None, &mut state, &stream_config(25)).unwrap();
        let adpcm = BcMedia::Adpcm(BcMediaAdpcm { data: vec![0; 260] });
        send_to_sources(adpcm, &None, &None, &mut state, &stream_config(25)).unwrap();
        assert!(state.aud_ts > u64::from(u32::MAX));
    }

    #[test]
    fn zero_fps_falls_back_instead_of_dividing_by_zero() {
        let mut state = PumpState::default();
        send_to_sources(frame(true, 16), &None, &None, &mut state, &stream_config(0)).unwrap();
        assert_eq!(state.vid_ts, 40_000);
    }

    #[test]
    fn a_stalled_client_bounds_the_queue_and_waits_for_a_keyframe() {
        // PAUSED: the sink prerolls the first buffer and then nothing drains
        let (pipeline, src) = appsrc_pipeline(2000);
        pipeline.set_state(gstreamer::State::Paused).unwrap();

        let vid_src = Some(src);
        let mut state = PumpState::default();
        send_to_sources(
            frame(true, 600),
            &vid_src,
            &None,
            &mut state,
            &stream_config(25),
        )
        .unwrap();
        for _ in 0..100 {
            send_to_sources(
                frame(false, 600),
                &vid_src,
                &None,
                &mut state,
                &stream_config(25),
            )
            .unwrap();
        }
        let level = vid_src.as_ref().unwrap().current_level_bytes();
        pipeline.set_state(gstreamer::State::Null).unwrap();

        assert!(level <= 2000 + 600, "queue grew to {level} bytes");
        assert!(
            state.need_keyframe,
            "dropping video must wait for a keyframe"
        );
    }

    #[test]
    fn adpcm_caps_use_the_dvi_layout() {
        gstreamer::init().unwrap();
        let bin = gstreamer::Bin::new().upcast::<Element>();
        let linked = pipe_adpcm(&bin, 512, &stream_config(25)).unwrap();
        let caps = linked.appsrc.caps().unwrap();
        let layout = caps.structure(0).unwrap().get::<&str>("layout").unwrap();
        assert_eq!(layout, "dvi");
    }
}
