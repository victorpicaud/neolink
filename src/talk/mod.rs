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
use std::{process::Stdio, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::Mutex,
};

mod cmdline;
mod gst;

use crate::common::{NeoInstance, NeoReactor};
pub(crate) use cmdline::Opt;
pub(crate) use gst::Input;

/// Entry point for the talk subcommand
///
/// Opt is the command line options
pub(crate) async fn main(opt: Opt, reactor: NeoReactor) -> Result<()> {
    let camera = reactor.get(&opt.camera).await?;
    let talk = Talk::of(&camera).await?;

    // The command line asks for one or the other
    match &opt.file_path {
        Some(path) => talk
            .play(&camera, Input::File(path), opt.volume)
            .await
            .with_context(|| format!("Failed to play the file {:?}", path)),
        None => talk
            .play(&camera, Input::Source(&opt.input_src), opt.volume)
            .await
            .context("Failed to play the microphone"),
    }
}

/// How a camera talks: what it is told of the sound, and how it is encoded
pub(crate) struct Talk {
    config: TalkConfig,
    block_size: u16,
    sample_rate: u16,
}

impl Talk {
    /// Asks `camera` how it talks
    pub(crate) async fn of(camera: &NeoInstance) -> Result<Self> {
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
        Ok(Self {
            config: talk_config,
            block_size,
            sample_rate,
        })
    }

    /// Plays `input` through the camera, until it ends
    pub(crate) async fn play(
        &self,
        camera: &NeoInstance,
        input: Input<'_>,
        volume: f32,
    ) -> Result<()> {
        let (mut set, rx) = gst::from_input(input, volume, self.block_size, self.sample_rate)
            .context("Failed to setup gst")?;

        // Shared with each try of the task, should the camera reconnect
        let rx = Arc::new(Mutex::new(rx));
        let talked = camera
            .run_task(|cam| {
                let rx = rx.clone();
                let talk_config = self.config.clone();
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
}

/// The most sound taken from a say command, in bytes: ten minutes of speech,
/// as a WAV
const MOST_SPEECH: usize = 32 * 1024 * 1024;

/// Speaks `text` with `command`, which reads it on its standard input, and
/// writes the sound to its standard output, in any format GStreamer reads,
/// such as a WAV
pub(crate) async fn speech(command: &[String], text: &str) -> Result<Vec<u8>> {
    let (program, args) = command
        .split_first()
        .ok_or_else(|| anyhow!("The say command is empty"))?;
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Should its caller give up on it
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("Could not run the say command {program}"))?;
    let mut stdin = child.stdin.take().expect("stdin is piped");
    let stdout = child.stdout.take().expect("stdout is piped");
    let mut stderr = child.stderr.take().expect("stderr is piped");

    // All at once: a command may speak before it read all of the text, and
    // wait for its sound to be read
    let text = text.to_string();
    let mut sound = vec![];
    let mut errors = vec![];
    let (written, read, _) = tokio::join!(
        async move {
            let written = stdin.write_all(text.as_bytes()).await;
            drop(stdin);
            written
        },
        async {
            let mut stdout = stdout.take(MOST_SPEECH as u64 + 1);
            stdout.read_to_end(&mut sound).await
        },
        stderr.read_to_end(&mut errors),
    );
    if sound.len() > MOST_SPEECH {
        let _ = child.kill().await;
        return Err(anyhow!(
            "The say command {program} said over {MOST_SPEECH} bytes"
        ));
    }
    let status = child.wait().await?;
    if !status.success() {
        return Err(anyhow!(
            "The say command {program} failed with {status}: {}",
            String::from_utf8_lossy(&errors).trim()
        ));
    }
    read.with_context(|| format!("Could not read what the say command {program} said"))?;
    written.with_context(|| format!("The say command {program} did not take the text"))?;
    if sound.is_empty() {
        return Err(anyhow!("The say command {program} said nothing"));
    }
    Ok(sound)
}

// With sh
#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn command(script: &str) -> Vec<String> {
        ["sh", "-c", script].map(String::from).to_vec()
    }

    #[tokio::test]
    async fn the_say_command_gets_the_text_and_gives_the_sound() -> Result<()> {
        // Its sound is what it read, backwards
        let sound = speech(&command("rev"), "hello\n").await?;
        assert_eq!(sound, b"olleh\n");
        Ok(())
    }

    #[tokio::test]
    async fn a_say_command_that_fails_or_says_nothing_is_an_error() {
        let failed = speech(&command("echo no voice >&2; exit 3"), "hello")
            .await
            .unwrap_err();
        assert!(format!("{failed:#}").contains("no voice"), "{failed:#}");

        let silent = speech(&command("cat > /dev/null"), "hello")
            .await
            .unwrap_err();
        assert!(format!("{silent:#}").contains("said nothing"), "{silent:#}");

        assert!(speech(&[], "hello").await.is_err());
        assert!(speech(&["/no/such/say".to_string()], "hello")
            .await
            .is_err());
    }
}
