//! Create a tokio encoder/decoder for turning a AsyncRead/Write stream into
//! a Bc packet
//!
//! BcCodex is used with a `[tokio_util::codec::Framed]` to form complete packets
//!
use crate::bc::de::parse_header;
use crate::bc::model::*;
use crate::bc::xml::*;
use crate::{Credentials, Error, Result};
use bytes::{Buf, BytesMut};
use nom::AsBytes;
use tokio_util::codec::{Decoder, Encoder};

/// A header claiming a longer body is taken for corruption: the largest real
/// messages are a few hundred KiB of video
const MAX_BODY_LEN: u32 = 16 * 1024 * 1024;
/// While looking for its place again, the codec only believes a header whose
/// body is at most this long
const MAX_RESYNC_BODY_LEN: u32 = 2 * 1024 * 1024;
/// How much may be skipped looking for a header before the connection is
/// given up on
const MAX_RESYNC_BYTES: usize = 4 * 1024 * 1024;
/// How many messages in a row may fail to parse before the connection is
/// given up on
const MAX_BAD_FRAMES: usize = 8;

pub(crate) struct BcCodex {
    context: BcContext,
    /// Set when the stream lost its place: headers then need more checks
    resyncing: bool,
    /// Bytes skipped since the last message, looking for a header
    skipped: usize,
    /// Messages in a row whose body did not parse
    bad_frames: usize,
}

impl BcCodex {
    pub(crate) fn new_with_debug(credentials: Credentials) -> Self {
        let mut codex = Self::new(credentials);
        codex.context.debug_on();
        codex
    }
    pub(crate) fn new(credentials: Credentials) -> Self {
        Self {
            context: BcContext::new(credentials),
            resyncing: false,
            skipped: 0,
            bad_frames: 0,
        }
    }

    /// Drops `count` bytes that are not part of any message
    fn skip(&mut self, src: &mut BytesMut, count: usize) -> Result<()> {
        src.advance(count);
        self.skipped += count;
        if self.skipped > MAX_RESYNC_BYTES {
            return Err(Error::Other(
                "Could not find the start of the camera's next message",
            ));
        }
        Ok(())
    }

    /// Drops the bytes before the next believable header. Returns false if
    /// more bytes are needed to tell whether there is one.
    fn find_header(&mut self, src: &mut BytesMut) -> Result<bool> {
        let mut from = 0;
        loop {
            let Some(at) = find_magic(&src[from..]).map(|at| from + at) else {
                // A magic may still start in the last three bytes
                let drop = src.len().saturating_sub(3);
                self.skip(src, drop)?;
                return Ok(false);
            };
            match believable_header(&src[at..]) {
                Some(true) => {
                    self.skip(src, at)?;
                    return Ok(true);
                }
                None => {
                    self.skip(src, at)?;
                    return Ok(false);
                }
                Some(false) => from = at + 1,
            }
        }
    }

    /// Learns from a message what later ones need: the encryption and which
    /// messages are binary
    fn update_context(&mut self, bc: &Bc) -> Result<()> {
        if let Bc {
            meta:
                BcMeta {
                    msg_id: 1,
                    response_code,
                    ..
                },
            body:
                BcBody::ModernMsg(ModernMsg {
                    payload:
                        Some(BcPayloads::BcXml(BcXml {
                            encryption: Some(Encryption { nonce, .. }),
                            ..
                        })),
                    ..
                }),
        } = bc
        {
            if response_code >> 8 == 0xdd {
                // Login reply has the encryption info
                // Set that the encryption type now
                let encryption_protocol_byte = (response_code & 0xff) as usize;
                match encryption_protocol_byte {
                    0x00 => self.context.set_encrypted(EncryptionProtocol::Unencrypted),
                    0x01 => self.context.set_encrypted(EncryptionProtocol::BCEncrypt),
                    0x02 => self.context.set_encrypted(EncryptionProtocol::aes(
                        self.context.credentials.make_aeskey(nonce),
                    )),
                    0x12 => self.context.set_encrypted(EncryptionProtocol::full_aes(
                        self.context.credentials.make_aeskey(nonce),
                    )),
                    _ => {
                        return Err(Error::UnknownEncryption(encryption_protocol_byte));
                    }
                }
            }
        }

        if let BcBody::ModernMsg(ModernMsg {
            extension:
                Some(Extension {
                    binary_data: Some(on_off),
                    ..
                }),
            ..
        }) = bc.body
        {
            if on_off == 0 {
                self.context.binary_off(bc.meta.msg_id, bc.meta.msg_num);
            } else {
                self.context.binary_on(bc.meta.msg_id, bc.meta.msg_num);
            }
        }
        Ok(())
    }
}

/// Where the next magic number that starts a header is
fn find_magic(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| {
        let magic = u32::from_le_bytes([window[0], window[1], window[2], window[3]]);
        magic == MAGIC_HEADER || magic == MAGIC_HEADER_REV
    })
}

/// Whether `buf` starts with a header to believe while looking for the
/// stream's place again, or None if more bytes are needed to tell. Real
/// headers have a known class and sizes that add up, and the next message's
/// magic follows right after their body: garbage rarely manages all of that.
fn believable_header(buf: &[u8]) -> Option<bool> {
    let (header, header_len) = match parse_header(buf) {
        Ok(Some(found)) => found,
        Ok(None) => return None,
        Err(_) => return Some(false),
    };
    let plausible = matches!(header.class, 0x6514 | 0x6614 | 0x6414 | 0x0000)
        && header.body_len <= MAX_RESYNC_BODY_LEN
        && header
            .payload_offset
            .is_none_or(|offset| offset <= header.body_len)
        && header.msg_id <= 0xffff;
    if !plausible {
        return Some(false);
    }
    let next = header_len + header.body_len as usize;
    buf.get(next..next + 4)
        .map(|magic| find_magic(magic) == Some(0))
}

impl Encoder<Bc> for BcCodex {
    type Error = Error;

    fn encode(&mut self, item: Bc, dst: &mut BytesMut) -> Result<()> {
        // let context = self.context.read().unwrap();
        const BC_ENCRYPTED: EncryptionProtocol = EncryptionProtocol::BCEncrypt;
        let buf: Vec<u8> = Default::default();
        let enc_protocol: &EncryptionProtocol = match self.context.get_encrypted() {
            EncryptionProtocol::Aes { .. } | EncryptionProtocol::FullAes { .. }
                if item.meta.msg_id == 1 =>
            {
                // During login the encyption protocol cannot go higher than BCEncrypt
                // even if we support AES. (BUt it can go lower i.e. None)
                &BC_ENCRYPTED
            }
            n => n,
        };
        let buf = item.serialize(buf, enc_protocol)?;
        dst.extend_from_slice(buf.as_slice());
        Ok(())
    }
}

impl Decoder for BcCodex {
    type Item = Bc;
    type Error = Error;

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>> {
        match self.decode(buf)? {
            Some(frame) => Ok(Some(frame)),
            None => {
                if buf.is_empty() {
                    Ok(None)
                } else {
                    log::debug!(
                        "bytes remaining on BC stream: {:X?}",
                        buf.as_bytes().chunks(25).next()
                    );
                    // Right after this we seem to get an issue with the camera dropping us
                    // Needs probing
                    // F0, DE, BC, A, 3, 0, 0, 0, 88, 6, 0, 0, 0, 1, 4, 0, C8, 0, 0, 0, 0, 0, 0, 0, 30, 31, 64, 63, 48,
                    // 32, 36, 34, 6A, 6, 0, 0, 0, 0, 0, 0, D8, F5, C7, 86, 56, 0, 0, 0, 0, 0, 0, 1, 21, 9A, FC, 22, 7F, 6, AE, F6, 15, FF, E5, 71, 4, 2F, 24, 61, 15, 96, F0, BF, 83, DE, 10, BE, B4, 2E, 3
                    // 9, 76, 56, 92, 7E, 48, 79, 20, 9A, DC, 1B, BB, AC, 22, 60, 5C, 72, B5, 3D, 8, E0, 34, 43, 3F, 2E, A7, 81, A8, 11, 75, 7F, 58, 3E, 8, 54, 91, 43, 21, EC, 6B, D6, 1A, D5, CB, D5, 6C,
                    // 8C, 2E, 6E, A3, 51, C3, A4, F0, CF, 2B, 61, 81, D0, 1C, A1, 76, EE, BF, 7A, D5, D8, D1, C4, D, B0, 45, EE, 3E, 93, 9A, CE, 5F, AB, 75, 55, AC, 9D, 66, DE, 23, 6D, 5F, 25, 57, DA, F5
                    //, E, 7F, 8D, 30, A7, 66, C4, 60, 76, 41, D0, 6A, 23, E, A9, C5, 51, EE, F6, DD, 19, E7, A8, 96, 9F, 2B, AF, 31, 90, 9D, FC, BE
                    Ok(None)
                }
            }
        }
        // match self.decode(buf)? {
        //     Some(frame) => Ok(Some(frame)),
        //     None => Ok(None),
        // }
    }

    /// Splits whole messages off the stream: the header first, then exactly
    /// the body it announces. A body that cannot be parsed costs only that
    /// message, and a stream that lost its place finds the next header. Both
    /// used to drop the connection.
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        loop {
            if self.resyncing {
                if !self.find_header(src)? {
                    return Ok(None);
                }
                self.resyncing = false;
            }

            let (header, header_len) = match parse_header(src) {
                Ok(Some((header, header_len))) if header.body_len <= MAX_BODY_LEN => {
                    (header, header_len)
                }
                Ok(None) => return Ok(None),
                // Not a header where one should be
                _ => {
                    log::warn!("Lost the start of the camera's messages, looking for it");
                    self.resyncing = true;
                    self.skip(src, 1)?;
                    continue;
                }
            };
            let frame_len = header_len + header.body_len as usize;
            if src.len() < frame_len {
                src.reserve(frame_len - src.len());
                return Ok(None);
            }
            let frame = src.split_to(frame_len);

            match Bc::parse_body(&self.context, &header, &frame[header_len..]) {
                Ok(bc) => {
                    if self.skipped > 0 {
                        log::warn!(
                            "Found the camera's messages again, after skipping {} bytes",
                            self.skipped
                        );
                        self.skipped = 0;
                    }
                    self.bad_frames = 0;
                    self.update_context(&bc)?;
                    return Ok(Some(bc));
                }
                // Logging in cannot go on without it
                Err(e) if header.msg_id == MSG_ID_LOGIN => return Err(e),
                Err(e) => {
                    self.bad_frames += 1;
                    if self.bad_frames >= MAX_BAD_FRAMES {
                        return Err(e);
                    }
                    log::warn!(
                        "Skipping message {} (number {}) that could not be parsed: {e}",
                        header.msg_id,
                        header.msg_num
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(bc: Bc) -> Vec<u8> {
        bc.serialize(vec![], &EncryptionProtocol::Unencrypted)
            .unwrap()
    }

    fn meta(msg_id: u32, msg_num: u16) -> BcMeta {
        BcMeta {
            msg_id,
            channel_id: 0,
            msg_num,
            stream_type: 0,
            response_code: 200,
            class: 0x0000,
        }
    }

    fn ping(msg_num: u16) -> Vec<u8> {
        bytes(Bc::new_from_meta(meta(MSG_ID_PING, msg_num)))
    }

    /// Decodes everything in `buf`, giving each message's ID and number
    fn decode_all(codec: &mut BcCodex, buf: &mut BytesMut) -> Result<Vec<(u32, u16)>> {
        let mut decoded = vec![];
        while let Some(bc) = codec.decode(buf)? {
            decoded.push((bc.meta.msg_id, bc.meta.msg_num));
        }
        Ok(decoded)
    }

    #[test]
    fn garbage_before_a_message_is_skipped() {
        let mut codec = BcCodex::new(Default::default());
        let mut buf = BytesMut::from(&b"these bytes are not a header"[..]);
        buf.extend_from_slice(&ping(1));
        buf.extend_from_slice(&ping(2));

        assert_eq!(
            decode_all(&mut codec, &mut buf).unwrap(),
            [(MSG_ID_PING, 1), (MSG_ID_PING, 2)]
        );
    }

    #[test]
    fn a_message_that_cannot_be_parsed_costs_only_itself() {
        let mut codec = BcCodex::new(Default::default());
        // The payload would start past the end of the body
        let mut broken = ping(1);
        broken[20..24].copy_from_slice(&100u32.to_le_bytes());
        let mut buf = BytesMut::from(&broken[..]);
        buf.extend_from_slice(&ping(2));

        assert_eq!(
            decode_all(&mut codec, &mut buf).unwrap(),
            [(MSG_ID_PING, 2)]
        );
    }

    #[test]
    fn a_login_that_cannot_be_parsed_is_still_fatal() {
        let mut codec = BcCodex::new(Default::default());
        let mut broken = bytes(Bc::new_from_meta(meta(MSG_ID_LOGIN, 1)));
        broken[20..24].copy_from_slice(&100u32.to_le_bytes());
        assert!(codec.decode(&mut BytesMut::from(&broken[..])).is_err());
    }

    #[test]
    fn a_magic_in_the_garbage_is_not_taken_for_a_header() {
        let mut codec = BcCodex::new(Default::default());
        let mut buf = BytesMut::from(&b"junk"[..]);
        // A magic number and a header that adds up, but no message after it
        buf.extend_from_slice(&ping(9));
        buf.extend_from_slice(b"no magic here");
        // A magic number and a header that does not add up
        buf.extend_from_slice(&ping(8)[..8]);
        buf.extend_from_slice(&[0xff; 32]);
        buf.extend_from_slice(&ping(1));
        buf.extend_from_slice(&ping(2));

        assert_eq!(
            decode_all(&mut codec, &mut buf).unwrap(),
            [(MSG_ID_PING, 1), (MSG_ID_PING, 2)]
        );
    }

    #[test]
    fn too_much_garbage_gives_the_connection_up() {
        let mut codec = BcCodex::new(Default::default());
        let mut buf = BytesMut::from(&[0u8; 1][..]);
        let mut result = codec.decode(&mut buf);
        for _ in 0..5 {
            buf.extend_from_slice(&vec![0u8; 1024 * 1024]);
            result = codec.decode(&mut buf);
            if result.is_err() {
                break;
            }
        }
        assert!(result.is_err());
    }

    #[test]
    fn the_whole_body_of_a_legacy_message_is_consumed() {
        let mut codec = BcCodex::new(Default::default());
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&MAGIC_HEADER.to_le_bytes());
        buf.extend_from_slice(&2u32.to_le_bytes()); // msg_id
        buf.extend_from_slice(&10u32.to_le_bytes()); // body_len
        buf.extend_from_slice(&[0, 0]); // channel, stream type
        buf.extend_from_slice(&5u16.to_le_bytes()); // msg_num
        buf.extend_from_slice(&0u16.to_le_bytes()); // response code
        buf.extend_from_slice(&0x6514u16.to_le_bytes()); // legacy class
        buf.extend_from_slice(&[0xab; 10]);
        buf.extend_from_slice(&ping(6));

        assert_eq!(
            decode_all(&mut codec, &mut buf).unwrap(),
            [(2, 5), (MSG_ID_PING, 6)]
        );
    }

    #[test]
    fn a_captured_video_start_decodes_through_the_codec() {
        let mut codec = BcCodex::new(Default::default());
        codec.context.set_encrypted(EncryptionProtocol::BCEncrypt);
        let mut buf = BytesMut::from(&include_bytes!("samples/modern_video_start1.bin")[..]);
        buf.extend_from_slice(include_bytes!("samples/modern_video_start2.bin"));

        let start = codec.decode(&mut buf).unwrap().unwrap();
        let video = codec.decode(&mut buf).unwrap().unwrap();
        assert!(buf.is_empty());
        assert_eq!(start.meta.msg_num, video.meta.msg_num);
        assert!(matches!(
            video.body,
            BcBody::ModernMsg(ModernMsg {
                payload: Some(BcPayloads::Binary(data)),
                ..
            }) if data.len() == 30344
        ));
    }

    #[test]
    fn binary_mode_belongs_to_a_message_id_and_number() {
        let mut codec = BcCodex::new(Default::default());
        let video = Bc::new(
            meta(MSG_ID_VIDEO, 5),
            Some(Extension {
                binary_data: Some(1),
                ..Default::default()
            }),
            Some(BcPayloads::Binary(vec![1, 2, 3, 4])),
        );
        let mut buf = BytesMut::from(&bytes(video)[..]);
        assert!(codec.decode(&mut buf).unwrap().is_some());

        // A reply that reuses the number is still read as XML
        let reply = bytes(Bc::new_from_xml(meta(MSG_ID_PING, 5), BcXml::default()));
        let mut buf = BytesMut::from(&reply[..]);
        let reply = codec.decode(&mut buf).unwrap().unwrap();
        assert!(matches!(
            reply.body,
            BcBody::ModernMsg(ModernMsg {
                payload: Some(BcPayloads::BcXml(_)),
                ..
            })
        ));
    }
}
