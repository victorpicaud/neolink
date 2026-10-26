//! Handles sending and recieving messages as complete packets
//!
//! BcUdpCodex is used with a `[tokio_util::codec::Framed]` to form complete packets
//!
use crate::bcudp::model::*;
use crate::{Error, Result};
use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use super::xml::UdpXml;

pub(crate) struct BcUdpCodex {}

impl BcUdpCodex {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl Encoder<BcUdp> for BcUdpCodex {
    type Error = Error;

    fn encode(&mut self, item: BcUdp, dst: &mut BytesMut) -> Result<()> {
        log::trace!("Encoding: {item:?}");
        let buf: Vec<u8> = Default::default();
        let buf = item.serialize(buf)?;
        dst.extend_from_slice(buf.as_slice());
        log::trace!("  Encoding: Done: {}", buf.len());
        Ok(())
    }
}

impl Decoder for BcUdpCodex {
    type Item = BcUdp;
    type Error = Error;

    /// Each datagram stands alone: whatever is left of one that did not parse
    /// is dropped, not taken as an error, or the reader would see it again and
    /// again
    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>> {
        let decoded = self.decode(buf)?;
        if decoded.is_none() && !buf.is_empty() {
            log::debug!("Dropping {} bytes of a UDP datagram", buf.len());
            buf.clear();
        }
        Ok(decoded)
    }

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        log::trace!("Decoding:");
        if src.is_empty() {
            return Ok(None);
        }
        match BcUdp::deserialize(src) {
            Ok(BcUdp::Discovery(UdpDiscovery {
                payload: UdpXml::R2cDisc(_),
                ..
            })) => {
                log::trace!("   Decoding: Relay terminate");
                Err(Error::RelayTerminate)
            }
            Ok(BcUdp::Discovery(UdpDiscovery {
                payload: UdpXml::D2cDisc(_),
                ..
            })) => {
                log::trace!("   Decoding:Camera terminate");
                Err(Error::CameraTerminate)
            }
            Ok(bc) => {
                log::trace!("   Decoding: Ok");
                Ok(Some(bc))
            }
            Err(Error::NomIncomplete(_)) => {
                log::trace!("   Decoding: Incomplete: {:0X?}", src);
                Ok(None)
            }
            Err(e) => {
                log::trace!("   Decoding: Err");
                // So the next read moves on to the next datagram
                src.clear();
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::Arc;
    use tokio::net::UdpSocket;
    use tokio_util::udp::UdpFramed;

    #[tokio::test]
    async fn a_datagram_that_does_not_parse_is_read_only_once() {
        let receiver = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let to = receiver.local_addr().unwrap();
        let ack = BcUdp::Ack(UdpAck::empty(5)).serialize(vec![]).unwrap();
        sender.send_to(b"not a camera datagram", to).await.unwrap();
        sender.send_to(&ack, to).await.unwrap();

        let mut framed = UdpFramed::new(receiver, BcUdpCodex::new());
        assert!(framed.next().await.unwrap().is_err());
        // Not the same error again: the next datagram is read
        assert!(matches!(
            framed.next().await.unwrap(),
            Ok((BcUdp::Ack(_), _))
        ));
    }
}
