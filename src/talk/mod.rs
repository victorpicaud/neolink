///
/// # Neolink Talk
///
/// This module can be used to send adpcm data for the camera to play
///
/// The adpcm data needs to be in DVI-4 layout
///
/// # Usage
///
/// ```bash
/// neolink talk --config=config.toml --adpcm-file=data.adpcm --sample-rate=16000 --block-size=512 CameraName
/// ```
///
use anyhow::{anyhow, Context, Result};
use neolink_core::bc::xml::TalkConfig;
use std::sync::Arc;
use tokio::sync::Mutex;

mod cmdline;
mod gst;

use crate::common::NeoReactor;
pub(crate) use cmdline::Opt;

/// Entry point for the talk subcommand
///
/// Opt is the command line options
pub(crate) async fn main(opt: Opt, reactor: NeoReactor) -> Result<()> {
    let camera = reactor.get(&opt.camera).await?;
    let config = camera.config().await?.borrow().clone();
    let name = config.name.clone();

    let talk_ability = camera
        .run_task(|cam| {
            Box::pin(async move {
                let talk_ability = cam.talk_ability().await?;
                Ok(talk_ability)
            })
        })
        .await
        .with_context(|| format!("Camera {} does not support talk", name))?;

    // Some cameras offer several modes; the first is what used to be the only one
    let audio_stream_mode = talk_ability
        .audio_stream_mode_list
        .first()
        .and_then(|modes| modes.audio_stream_mode.first())
        .cloned();
    let Some(audio_stream_mode) = audio_stream_mode else {
        return Err(anyhow!("Camera {} does not support talk", name));
    };
    if talk_ability.duplex_list.is_empty() || talk_ability.audio_config_list.is_empty() {
        return Err(anyhow!("Camera {} does not support talk", name));
    }

    // Just copy that data from the first talk ability in the config have never seen more
    // than one ability
    let config_id = 0;

    let talk_config = TalkConfig {
        channel_id: config.channel_id,
        duplex: talk_ability.duplex_list[config_id].duplex.clone(),
        audio_stream_mode,
        audio_config: talk_ability.audio_config_list[config_id]
            .audio_config
            .clone(),
        ..Default::default()
    };

    let block_size = (talk_config.audio_config.length_per_encoder / 2) + 4;
    let sample_rate = talk_config.audio_config.sample_rate;
    if block_size == 0 || sample_rate == 0 {
        return Err(anyhow!(
            "The camera {} does not support talk with adpcm",
            name
        ));
    }

    // The command line asks for one or the other
    let (mut set, rx) = match &opt.file_path {
        Some(path) => gst::from_input(gst::Input::File(path), opt.volume, block_size, sample_rate)
            .with_context(|| format!("Failed to setup gst with the file: {:?}", path))?,
        None => gst::from_input(
            gst::Input::Source(&opt.input_src),
            opt.volume,
            block_size,
            sample_rate,
        )
        .context("Failed to setup gst using the microphone")?,
    };

    // Shared with each try of the task, should the camera reconnect
    let rx = Arc::new(Mutex::new(rx));
    let talked = camera
        .run_task(|cam| {
            let rx = rx.clone();
            let talk_config = talk_config.clone();
            Box::pin(async move {
                cam.talk_stream(&mut *rx.lock().await, talk_config).await?;
                Ok(())
            })
        })
        .await
        .context("Talk stream ended early");

    // The pipeline ends once talk takes no more of its sound
    drop(rx);
    let mut played = Ok(());
    while let Some(ended) = set.join_next().await {
        match ended {
            Ok(Ok(())) => {}
            Ok(Err(e)) => played = Err(e.context("Could not play the sound")),
            Err(e) => played = Err(anyhow!(e).context("Could not play the sound")),
        }
    }
    talked.and(played)
}
