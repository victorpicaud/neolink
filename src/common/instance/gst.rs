use super::*;

use neolink_core::{bc_protocol::StreamKind, bcmedia::model::BcMedia};
use tokio::sync::mpsc::Receiver as MpscReceiver;

impl NeoInstance {
    /// Streams a camera source while not paused. The camera streams once for
    /// all the receivers of a stream, and pauses as the config asks.
    pub(crate) async fn stream_while_live(
        &self,
        stream: StreamKind,
    ) -> AnyResult<MpscReceiver<BcMedia>> {
        let (reply_tx, reply_rx) = oneshot();
        self.camera_control
            .send(NeoCamCommand::Stream(stream, reply_tx))
            .await?;
        let subscription = reply_rx
            .await?
            .ok_or_else(|| anyhow!("The camera stream has stopped"))?;
        let (media_tx, media_rx) = tokio::sync::mpsc::channel(100);
        tokio::task::spawn(subscription.forward(media_tx));
        Ok(media_rx)
    }
}
