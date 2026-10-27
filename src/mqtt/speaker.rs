//! A camera's speaker, for `control/say` and `control/talk`
use anyhow::{anyhow, Result};

use crate::common::NeoInstance;
#[cfg(feature = "gstreamer")]
use {
    crate::talk::{speech, Input, Talk},
    tokio::{
        sync::{Mutex, OnceCell, Semaphore, SemaphorePermit},
        time::{timeout, Duration},
    },
};

/// A camera's speaker, which plays one sound at a time, each in its turn
pub(super) struct Speaker {
    /// For the sounds playing or waiting
    #[cfg(feature = "gstreamer")]
    turns: Semaphore,
    #[cfg(feature = "gstreamer")]
    playing: Mutex<()>,
    /// How the camera talks, once asked
    #[cfg(feature = "gstreamer")]
    talk: OnceCell<Talk>,
}

#[cfg(feature = "gstreamer")]
impl Speaker {
    /// How many sounds may be playing or waiting, at most: more would play
    /// for as long as anyone sends them
    const TURNS: usize = 4;
    /// How long a sound may play, at most: an endless one, such as a radio
    /// stream, would keep a battery camera awake for good
    const LONGEST: Duration = Duration::from_secs(5 * 60);
    /// How long the say command may take to speak
    const SPEAKING: Duration = Duration::from_secs(60);

    pub(super) fn new() -> Self {
        Self {
            turns: Semaphore::new(Self::TURNS),
            playing: Mutex::new(()),
            talk: OnceCell::new(),
        }
    }

    /// Speaks `text` with the say `command`
    pub(super) async fn say(
        &self,
        camera: &NeoInstance,
        command: &[String],
        text: &str,
    ) -> Result<()> {
        let _turn = self.turn()?;
        // Waking as it speaks: a battery camera takes seconds to
        let _awake = camera.permit().await?;
        let speech = timeout(Self::SPEAKING, speech(command, text))
            .await
            .map_err(|_| anyhow!("The say command took over {}s", Self::SPEAKING.as_secs()))??;
        self.play(camera, Input::Data(&speech)).await
    }

    /// Plays the sound file at `url`
    pub(super) async fn talk(&self, camera: &NeoInstance, url: &str) -> Result<()> {
        let _turn = self.turn()?;
        self.play(camera, Input::Uri(url)).await
    }

    fn turn(&self) -> Result<SemaphorePermit<'_>> {
        self.turns
            .try_acquire()
            .map_err(|_| anyhow!("The camera has {} sounds to play already", Self::TURNS))
    }

    async fn play(&self, camera: &NeoInstance, input: Input<'_>) -> Result<()> {
        let _playing = self.playing.lock().await;
        let talk = self.talk.get_or_try_init(|| Talk::of(camera)).await?;
        timeout(Self::LONGEST, talk.play(camera, input, 1.0))
            .await
            .map_err(|_| {
                anyhow!(
                    "The sound played for over {} minutes",
                    Self::LONGEST.as_secs() / 60
                )
            })?
    }
}

#[cfg(not(feature = "gstreamer"))]
impl Speaker {
    pub(super) fn new() -> Self {
        Self {}
    }

    pub(super) async fn say(&self, _: &NeoInstance, _: &[String], _: &str) -> Result<()> {
        Err(anyhow!(
            "neolink was built without GStreamer, which talk needs"
        ))
    }

    pub(super) async fn talk(&self, _: &NeoInstance, _: &str) -> Result<()> {
        Err(anyhow!(
            "neolink was built without GStreamer, which talk needs"
        ))
    }
}
