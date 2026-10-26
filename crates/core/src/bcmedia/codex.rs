//! Handles sending and recieving messages as packets
//!
//! BcMediaCodex is used with a `[tokio_util::codec::Framed]` to form complete packets
//!
use crate::bcmedia::{de::find_magic, model::*};
use crate::{Error, Result};
use bytes::{Buf, BytesMut};
use log::*;
use tokio_util::codec::{Decoder, Encoder};

pub struct BcMediaCodex {
    /// If true we will not search for the start of the next packet
    /// in the event that the stream appears to be corrupted
    strict: bool,
    amount_skipped: usize,
}

impl BcMediaCodex {
    pub(crate) fn new(strict: bool) -> Self {
        Self {
            strict,
            amount_skipped: 0,
        }
    }
}

impl Encoder<BcMedia> for BcMediaCodex {
    type Error = Error;

    fn encode(&mut self, item: BcMedia, dst: &mut BytesMut) -> Result<()> {
        let buf: Vec<u8> = Default::default();
        let buf = item.serialize(buf)?;
        dst.extend_from_slice(buf.as_slice());
        Ok(())
    }
}

impl Decoder for BcMediaCodex {
    type Item = BcMedia;
    type Error = Error;

    /// Since frames can cross EOF boundaries we overload this so it doesn't error if
    /// there are bytes left on the stream
    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>> {
        match self.decode(buf)? {
            Some(frame) => Ok(Some(frame)),
            None => Ok(None),
        }
    }

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        loop {
            match BcMedia::deserialize(src) {
                Ok(bc) => {
                    if self.amount_skipped > 0 {
                        trace!("Amount skipped to restore stream: {}", self.amount_skipped);
                        self.amount_skipped = 0;
                    }
                    return Ok(Some(bc));
                }
                Err(Error::NomIncomplete(_)) => {
                    if self.amount_skipped > 0 {
                        trace!("Amount skipped to restore stream: {}", self.amount_skipped);
                        self.amount_skipped = 0;
                    }
                    return Ok(None);
                }
                Err(e) => {
                    if self.strict {
                        return Err(e);
                    }
                    if self.amount_skipped == 0 {
                        debug!("Error in stream attempting to restore");
                        trace!("   Stream Error: {:?}", e);
                    }
                    // Skip to the next thing that looks like the start of a
                    // packet. Clearing the whole buffer threw that away too,
                    // and the stream could go on failing for good.
                    let skip = match src.get(1..).and_then(find_magic) {
                        Some(at) => at + 1,
                        // A magic may still start in the last three bytes
                        None => src.len().saturating_sub(3),
                    };
                    if skip == 0 {
                        return Ok(None);
                    }
                    self.amount_skipped += skip;
                    src.advance(skip);
                    continue;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iframe(n: u8) -> Vec<u8> {
        BcMedia::Iframe(BcMediaIframe {
            video_type: VideoType::H264,
            microseconds: 0,
            time: None,
            data: vec![n; 16],
        })
        .serialize(vec![])
        .unwrap()
    }

    fn frame_number(media: Option<BcMedia>) -> u8 {
        match media {
            Some(BcMedia::Iframe(BcMediaIframe { data, .. })) => data[0],
            other => panic!("expected a frame, got {other:?}"),
        }
    }

    #[test]
    fn garbage_before_a_frame_is_skipped() {
        let mut codec = BcMediaCodex::new(false);
        let mut buf = BytesMut::from(&b"not media"[..]);
        buf.extend_from_slice(&iframe(1));
        assert_eq!(frame_number(codec.decode(&mut buf).unwrap()), 1);
    }

    #[test]
    fn an_implausible_size_is_skipped_not_waited_for() {
        let mut codec = BcMediaCodex::new(false);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&MAGIC_HEADER_BCMEDIA_IFRAME.to_le_bytes());
        buf.extend_from_slice(b"H264");
        buf.extend_from_slice(&u32::MAX.to_le_bytes()); // payload size
        buf.extend_from_slice(&[0; 12]);
        buf.extend_from_slice(&iframe(2));
        assert_eq!(frame_number(codec.decode(&mut buf).unwrap()), 2);
    }
}
