use super::{BcCamera, BcConnection, BcSubscription, Error, Result};
use crate::{bc::model::*, bc::xml::*, bcmedia::model::*};
use futures::FutureExt;
use tokio::{
    sync::mpsc::Receiver,
    time::{sleep_until, Duration, Instant},
};

/// How far ahead of its playing talk is sent. The camera plays it as it
/// comes: further ahead could overflow what it holds, and with less, a block
/// a little late leaves a gap.
const AHEAD: Duration = Duration::from_millis(500);
/// How many messages of talk the camera may leave unacknowledged
const IN_FLIGHT: usize = 16;
/// An ADPCM block starts with a header: its first sample, and the state of
/// the predictor
const BLOCK_HEADER_SIZE: usize = 4;
/// ADPCM holds two samples in a byte
const SAMPLES_PER_BYTE: usize = 2;

impl BcCamera {
    ///
    /// Finish Talk
    ///
    /// The send the talk finish to the camera.
    ///
    /// It is also sent when the request for talk config returns status code 422
    ///
    pub async fn talk_stop(&self) -> Result<()> {
        let connection = self.get_connection();

        let msg_num = self.new_message_num();
        let mut sub = connection.subscribe(MSG_ID_TALKRESET, msg_num).await?;

        let msg = Bc {
            meta: BcMeta {
                msg_id: MSG_ID_TALKRESET,
                channel_id: self.channel_id,
                msg_num,
                stream_type: 0,
                response_code: 0,
                class: 0x6414,
            },
            body: BcBody::ModernMsg(ModernMsg {
                extension: Some(Extension {
                    channel_id: Some(self.channel_id),
                    ..Default::default()
                }),
                payload: None,
            }),
        };

        sub.send(msg).await?;
        let msg = sub.recv().await?;

        if let BcMeta {
            response_code: 200, ..
        } = msg.meta
        {
        } else {
            return Err(Error::UnintelligibleReply {
                reply: std::sync::Arc::new(Box::new(msg)),
                why: "The camera did not accept the talk stop command.",
            });
        }

        Ok(())
    }

    ///
    /// Requests the [`TalkAbility`] xml
    ///
    pub async fn talk_ability(&self) -> Result<TalkAbility> {
        let connection = self.get_connection();
        let msg_num = self.new_message_num();
        let mut sub_get = connection.subscribe(MSG_ID_TALKABILITY, msg_num).await?;
        let get = Bc {
            meta: BcMeta {
                msg_id: MSG_ID_TALKABILITY,
                channel_id: self.channel_id,
                msg_num,
                response_code: 0,
                stream_type: 0,
                class: 0x6414,
            },
            body: BcBody::ModernMsg(ModernMsg {
                extension: Some(Extension {
                    channel_id: Some(self.channel_id),
                    ..Default::default()
                }),
                payload: None,
            }),
        };

        sub_get.send(get).await?;
        let msg = sub_get.recv().await?;

        if let BcBody::ModernMsg(ModernMsg {
            payload:
                Some(BcPayloads::BcXml(BcXml {
                    talk_ability: Some(talk_ability),
                    ..
                })),
            ..
        }) = msg.body
        {
            Ok(talk_ability)
        } else {
            Err(Error::UnintelligibleReply {
                reply: std::sync::Arc::new(Box::new(msg)),
                why: "Expected TalkAbility xml but it was not recieved",
            })
        }
    }

    ///
    /// Sets up talk as `talk_config` says, which needs ADPCM. Returns the size
    /// of its blocks, header included, and its sample rate.
    ///
    async fn talk_config(
        &self,
        connection: &BcConnection,
        talk_config: TalkConfig,
    ) -> Result<(usize, u16)> {
        let audio_config = &talk_config.audio_config;
        if audio_config.audio_type != "adpcm" || audio_config.sample_rate == 0 {
            return Err(Error::UnknownTalkEncoding);
        }
        let block_size = usize::from(audio_config.length_per_encoder / 2) + BLOCK_HEADER_SIZE;
        let sample_rate = audio_config.sample_rate;

        let msg_num = self.new_message_num();
        let mut sub = connection.subscribe(MSG_ID_TALKCONFIG, msg_num).await?;
        let request = || Bc {
            meta: BcMeta {
                msg_id: MSG_ID_TALKCONFIG,
                channel_id: self.channel_id,
                msg_num,
                stream_type: 0,
                response_code: 0,
                class: 0x6414,
            },
            body: BcBody::ModernMsg(ModernMsg {
                extension: Some(Extension {
                    channel_id: Some(self.channel_id),
                    ..Default::default()
                }),
                payload: Some(BcPayloads::BcXml(BcXml {
                    talk_config: Some(talk_config.clone()),
                    ..Default::default()
                })),
            }),
        };

        sub.send(request()).await?;
        let mut msg = sub.recv().await?;

        // If another client is already talking OR if we crashed before sending
        // msgid 11 the camera will reply 422
        //
        // The official client in this case just sends msgid 11 and retries so
        // we will do that too. The retry used to send the camera's reply back.
        if let BcMeta {
            response_code: 422, ..
        } = msg.meta
        {
            self.talk_stop().await?;
            sub.send(request()).await?;
            msg = sub.recv().await?;
        }

        if let BcMeta {
            response_code: 200, ..
        } = msg.meta
        {
        } else {
            return Err(Error::UnintelligibleReply {
                reply: std::sync::Arc::new(Box::new(msg)),
                why:
                    "The camera did not accept the TalkConfig xml. Audio format is likely incorrect",
            });
        }

        Ok((block_size, sample_rate))
    }

    /// A message of talk: `blocks` of ADPCM, each `block_size` long
    fn talk_message(&self, msg_num: u16, blocks: &[u8], block_size: usize) -> Result<Bc> {
        let mut payload = vec![];
        for block in blocks.chunks(block_size) {
            let bcmedia_adpcm = BcMedia::Adpcm(BcMediaAdpcm {
                data: block.to_vec(),
            });
            payload = bcmedia_adpcm.serialize(payload)?;
        }
        Ok(Bc {
            meta: BcMeta {
                msg_id: MSG_ID_TALK,
                channel_id: self.channel_id,
                msg_num,
                stream_type: 0,
                response_code: 0,
                class: 0x6414,
            },
            body: BcBody::ModernMsg(ModernMsg {
                extension: Some(Extension {
                    channel_id: Some(self.channel_id),
                    binary_data: Some(1),
                    ..Default::default()
                }),
                payload: Some(BcPayloads::Binary(payload)),
            }),
        })
    }

    ///
    /// Send sound to the camera
    ///
    /// The data should be in the format as described in `<TalkAbility>` xml
    /// This method assumes that you have set up the data in the desired format
    /// in the `<TalkAbility>` xml
    ///
    /// It also checks that it is ADPCM as the code is written to accept only that
    ///
    /// # Parameters
    ///
    /// * `adpcm` - Data must be adpcm in DVI-4 format
    ///
    /// * `talk_config` - The talk config that describes the adpcm data
    ///
    ///
    pub async fn talk(&self, adpcm: &[u8], talk_config: TalkConfig) -> Result<()> {
        let connection = self.get_connection();
        let (block_size, sample_rate) = self.talk_config(&connection, talk_config).await?;

        let msg_num = self.new_message_num();
        let mut sub = connection.subscribe(MSG_ID_TALK, msg_num).await?;
        let mut pacer = TalkPacer::new();

        const BLOCK_PER_PAYLOAD: usize = 4;
        for blocks in adpcm.chunks(block_size * BLOCK_PER_PAYLOAD) {
            pacer.ready(&mut sub).await?;
            sub.send(self.talk_message(msg_num, blocks, block_size)?)
                .await?;
            pacer.sent(play_length(blocks, block_size, sample_rate));
        }

        pacer.played().await;
        self.talk_stop().await?;

        Ok(())
    }

    ///
    /// Send sound to the camera through a channel
    ///
    /// This is similar to [`talk`] except it uses a channel to receive data,
    /// until the channel closes
    ///
    /// The data should be in the format as described in `<TalkAbility>` xml
    /// This method assumes that you have set up the data in the desired format
    /// in the `<TalkAbility>` xml
    ///
    /// It also checks that it is ADPCM as the code is written to accept only that
    ///
    /// # Parameters
    ///
    /// * `adpcm` - Data must be adpcm in DVI-4 format
    ///
    /// * `talk_config` - The talk config that describes the adpcm data
    ///
    ///
    pub async fn talk_stream(
        &self,
        adpcm: &mut Receiver<Vec<u8>>,
        talk_config: TalkConfig,
    ) -> Result<()> {
        let connection = self.get_connection();
        let (block_size, sample_rate) = self.talk_config(&connection, talk_config).await?;

        let msg_num = self.new_message_num();
        let mut sub = connection.subscribe(MSG_ID_TALK, msg_num).await?;
        let mut pacer = TalkPacer::new();

        let mut pending = vec![];
        let mut ended = false;
        loop {
            // A whole block, or what is left once the stream ended
            while pending.len() < block_size && !ended {
                match adpcm.recv().await {
                    Some(data) => pending.extend(data),
                    None => ended = true,
                }
            }
            let block: Vec<u8> = pending.drain(..pending.len().min(block_size)).collect();
            if block.len() <= BLOCK_HEADER_SIZE {
                // Nothing to play in it
                break;
            }
            pacer.ready(&mut sub).await?;
            sub.send(self.talk_message(msg_num, &block, block_size)?)
                .await?;
            pacer.sent(play_length(&block, block_size, sample_rate));
        }

        pacer.played().await;
        self.talk_stop().await?;

        Ok(())
    }
}

/// How long `blocks` of ADPCM, each `block_size` long, play: a block holds
/// one sample in its header, and two in each byte after it. The samples used
/// to be counted for whole payloads, and a short last one underflowed.
fn play_length(blocks: &[u8], block_size: usize, sample_rate: u16) -> Duration {
    let samples: usize = blocks
        .chunks(block_size)
        .filter(|block| block.len() >= BLOCK_HEADER_SIZE)
        .map(|block| (block.len() - BLOCK_HEADER_SIZE) * SAMPLES_PER_BYTE + 1)
        .sum();
    Duration::from_secs_f64(samples as f64 / f64::from(sample_rate.max(1)))
}

/// Paces talk by the time it plays, and by the camera's acknowledgements. It
/// was sent as fast as it came, or with a sleep that blocked the thread of
/// every other task, and the acknowledgements were waited for one at a time,
/// or never read.
struct TalkPacer {
    /// When all that was sent will have played
    played_at: Instant,
    /// Messages the camera has not acknowledged yet
    unacked: usize,
}

impl TalkPacer {
    fn new() -> Self {
        Self {
            played_at: Instant::now(),
            unacked: 0,
        }
    }

    /// Waits until the next message may go, reading acknowledgements meanwhile
    async fn ready(&mut self, sub: &mut BcSubscription<'_>) -> Result<()> {
        while let Some(ack) = sub.recv_untimed().now_or_never() {
            ack?;
            self.unacked = self.unacked.saturating_sub(1);
        }
        while self.unacked >= IN_FLIGHT {
            sub.recv().await?;
            self.unacked -= 1;
        }
        let send_at = self.played_at.checked_sub(AHEAD).unwrap_or(self.played_at);
        loop {
            tokio::select! {
                biased;
                _ = sleep_until(send_at) => return Ok(()),
                ack = sub.recv_untimed(), if self.unacked > 0 => {
                    ack?;
                    self.unacked -= 1;
                }
            }
        }
    }

    /// A message that plays for `length` went
    fn sent(&mut self, length: Duration) {
        self.played_at = self.played_at.max(Instant::now()) + length;
        self.unacked += 1;
    }

    /// Waits until all that was sent has played: stopping talk cuts short
    /// what still plays
    async fn played(&self) {
        sleep_until(self.played_at + Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_block_plays_its_samples_whatever_its_length() {
        // A block of 512 bytes after its header, at 16kHz
        let block_size = 516;
        let full = play_length(&[0; 516], block_size, 16000);
        assert_eq!(full, Duration::from_secs_f64(1025.0 / 16000.0));
        // Four blocks, the last one short
        let blocks = play_length(&[0; 516 * 3 + 10], block_size, 16000);
        assert_eq!(
            blocks,
            Duration::from_secs_f64((1025.0 * 3.0 + 13.0) / 16000.0)
        );
        // A last block too short for its header plays nothing
        assert_eq!(play_length(&[0; 3], block_size, 16000), Duration::ZERO);
    }
}
