use super::{BcCamera, Error, Overflow, Result};
use crate::{
    bc::{model::*, xml::*},
    bcmedia::model::*,
};
use futures::stream::Stream;
use futures::stream::StreamExt;
use tokio::sync::mpsc::{channel, error::TrySendError, Receiver, Sender};
use tokio::task::{self, JoinHandle};
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;

/// The stream names supported by BC
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum StreamKind {
    /// This is the HD stream
    Main,
    /// This is the SD stream
    Sub,
    /// This stream represents a balance between SD and HD
    ///
    /// It is only available on some camera. If the camera doesn't
    /// support it the stream will be the same as the SD stream
    Extern,
}

impl std::fmt::Display for StreamKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            StreamKind::Main => write!(f, "mainStream"),
            StreamKind::Sub => write!(f, "subStream"),
            StreamKind::Extern => write!(f, "externStream"),
        }
    }
}

/// How many video messages may wait unread. Video arrives fast, so this is
/// longer than for a reply.
const VIDEO_CAPACITY: usize = 512;
/// How long stopping a stream may take before it is given up on
const STOP_TIMEOUT: Duration = Duration::from_secs(2);

/// A handle on currently streaming data
///
/// The data can be pulled using `get_data` which returns raw BcMedia packets
///
/// When this object is dropped the streaming is stopped
pub struct StreamData {
    handle: Option<JoinHandle<Result<()>>>,
    rx: Receiver<Result<BcMedia>>,
    abort_handle: CancellationToken,
}

impl StreamData {
    /// Pull data from the camera's buffer
    /// This returns raw BcMedia packets
    pub async fn get_data(&mut self) -> Result<Result<BcMedia>> {
        match self.handle.take() {
            Some(handle) if handle.is_finished() => {
                // Taken, so that it is only awaited once: awaiting a finished
                // handle again panics
                self.abort_handle.cancel();
                handle.await??;
                return Err(Error::StreamFinished);
            }
            Some(handle) => self.handle = Some(handle),
            None => {
                self.abort_handle.cancel();
                return Err(Error::StreamFinished);
            }
        }
        match self.rx.recv().await {
            Some(data) => Ok(data),
            None => {
                self.abort_handle.cancel();
                Err(Error::StreamFinished)
            }
        }
    }

    /// Attempts to gracefully shutdown this will cancel the background task and send
    /// the Stop command to the camera
    pub async fn shutdown(&mut self) -> Result<()> {
        self.abort_handle.cancel();
        if let Some(handle) = self.handle.take() {
            let _ = handle.await?;
        }
        Ok(())
    }
}

impl Drop for StreamData {
    fn drop(&mut self) {
        log::trace!("Drop StreamData");
        // The task stops on its own once cancelled, so there is nothing to
        // wait for, and no runtime is needed here
        self.abort_handle.cancel();
        log::trace!("Dropped StreamData");
    }
}

/// Hands the media to the app without ever waiting on it, so that the
/// camera's messages are always read and its queue never fills. While the app
/// is behind, frames are dropped; video then resumes at the next keyframe, so
/// a decoder never sees a P-frame without the frames it depends on.
async fn forward_media<S>(media: &mut S, tx: &Sender<Result<BcMedia>>)
where
    S: Stream<Item = Result<BcMedia>> + Unpin,
{
    let mut need_keyframe = false;
    while let Some(media) = media.next().await {
        if media.is_err() {
            // The stream is over, which is worth waiting to hand over
            let _ = tx.send(media).await;
            return;
        }
        let keyframe = matches!(media, Ok(BcMedia::Iframe(_)));
        let video = keyframe || matches!(media, Ok(BcMedia::Pframe(_)));
        if need_keyframe && video && !keyframe {
            continue;
        }
        match tx.try_send(media) {
            Ok(()) if keyframe => need_keyframe = false,
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                log::trace!("The stream's reader is behind, dropping media");
                need_keyframe |= video;
            }
            Err(TrySendError::Closed(_)) => return,
        }
    }
}

impl BcCamera {
    ///
    /// Starts the video stream
    ///
    /// The returned object manages the data stream, when it is dropped
    /// the video stop signal is sent to the camera
    ///
    /// To pull frames from the camera's buffer use `recv_data` on the returned object
    ///
    /// The buffer_size represents number of compete messages so 1 would be one complete message
    /// which may be a single audio frame or a whole video key frame. If 0 a default of 100 is used
    ///
    /// A value of scrict=true will mean that the stream will error if the underlying stream is not
    /// as expected
    pub async fn start_video(
        &self,
        stream: StreamKind,
        mut buffer_size: usize,
        strict: bool,
    ) -> Result<StreamData> {
        if let Err(e) = self.has_ability_rw("preview").await {
            if self.has_ability_ro("streamTable").await.is_err() {
                return Err(e);
            }
        }

        let connection = self.get_connection();
        let msg_num = self.new_message_num();

        let abort_handle = CancellationToken::new();
        let abort_handle_thread = abort_handle.clone();

        if buffer_size == 0 {
            buffer_size = 100;
        }
        let (tx, rx) = channel(buffer_size);
        let channel_id = self.channel_id;

        let handle = task::spawn(async move {
            // If the queue still fills, the subscription ends and the stream
            // restarts cleanly at a keyframe. Dropping messages instead would
            // splice the frames they carry together.
            let mut sub_video = connection
                .subscribe_opts(MSG_ID_VIDEO, msg_num, VIDEO_CAPACITY, Overflow::Close)
                .await?;

            // On an E1 and swann cameras:
            //  - mainStream always has a value of 0
            //  - subStream always has a value of 1
            //  - There is no externStram
            // On a B800:
            //  - mainStream is 0
            //  - subStream is 0
            //  - externStream is 0
            let stream_code = match stream {
                StreamKind::Main => 0,
                StreamKind::Sub => 1,
                StreamKind::Extern => 0,
            };

            // Theses are the numbers used with the official client
            // On an E1 and swann cameras:
            //  - mainStream always has a value of 0
            //  - subStream always has a value of 1
            //  - There is no externStram
            // On a B800:
            //  - mainStream is 0
            //  - subStream is 256
            //  - externStram is 1024
            let handle = match stream {
                StreamKind::Main => 0,
                StreamKind::Sub => 256,
                StreamKind::Extern => 1024,
            };

            let stream_name = match stream {
                StreamKind::Main => "mainStream",
                StreamKind::Sub => "subStream",
                StreamKind::Extern => "externStream",
            }
            .to_string();

            let start_video = Bc::new_from_xml(
                BcMeta {
                    msg_id: MSG_ID_VIDEO,
                    channel_id,
                    msg_num,
                    stream_type: stream_code,
                    response_code: 0,
                    class: 0x6414, // IDK why
                },
                BcXml {
                    preview: Some(Preview {
                        version: xml_ver(),
                        channel_id,
                        handle,
                        stream_type: Some(stream_name),
                    }),
                    ..Default::default()
                },
            );

            sub_video.send(start_video).await?;

            let msg = sub_video.recv().await?;
            if let BcMeta {
                response_code: 200, ..
            } = msg.meta
            {
            } else {
                return Err(Error::UnintelligibleReply {
                    reply: std::sync::Arc::new(Box::new(msg)),
                    why: "The camera did not accept the stream start command.",
                });
            }

            {
                let mut media_sub = sub_video.bcmedia_stream(strict);

                tokio::select! {
                    _ = abort_handle_thread.cancelled() => {},
                    _ = forward_media(&mut media_sub, &tx) => {}
                }
            }
            // Stop listening, or the video that keeps coming while the camera
            // stops would queue up for no one
            drop(sub_video);

            let stop_video = Bc::new_from_xml(
                BcMeta {
                    msg_id: MSG_ID_VIDEO_STOP,
                    channel_id,
                    msg_num,
                    stream_type: stream_code,
                    response_code: 0,
                    class: 0x6414, // IDK why
                },
                BcXml {
                    preview: Some(Preview {
                        version: xml_ver(),
                        channel_id,
                        handle,
                        stream_type: None,
                    }),
                    ..Default::default()
                },
            );
            let stopped = timeout(STOP_TIMEOUT, async {
                let mut sub_stop = connection.subscribe(MSG_ID_VIDEO_STOP, msg_num).await?;
                sub_stop.send(stop_video).await?;
                loop {
                    let msg = sub_stop.recv().await?;
                    if let BcMeta {
                        response_code: 200,
                        msg_id: MSG_ID_VIDEO_STOP,
                        ..
                    } = msg.meta
                    {
                        return Ok(());
                    } else if let BcMeta {
                        msg_id: MSG_ID_VIDEO_STOP,
                        ..
                    } = msg.meta
                    {
                        return Err(Error::CameraServiceUnavailable {
                            id: msg.meta.msg_id,
                            code: msg.meta.response_code,
                        });
                    }
                }
            })
            .await;
            // A camera that does not answer the stop has stopped all the same
            stopped.unwrap_or(Ok(()))?;

            Ok(())
        });

        Ok(StreamData {
            handle: Some(handle),
            rx,
            abort_handle,
        })
    }

    /// Stop a camera from sending more stream data.
    pub async fn stop_video(&self, stream: StreamKind) -> Result<()> {
        if let Err(e) = self.has_ability_rw("preview").await {
            if self.has_ability_ro("streamTable").await.is_err() {
                return Err(e);
            }
        }
        let connection = self.get_connection();
        let msg_num = self.new_message_num();
        let mut sub_video = connection.subscribe(MSG_ID_VIDEO_STOP, msg_num).await?;

        // On an E1 and swann cameras:
        //  - mainStream always has a value of 0
        //  - subStream always has a value of 1
        //  - There is no externStram
        // On a B800:
        //  - mainStream is 0
        //  - subStream is 0
        //  - externStream is 0
        let stream_code = match stream {
            StreamKind::Main => 0,
            StreamKind::Sub => 1,
            StreamKind::Extern => 0,
        };

        // Theses are the numbers used with the official client
        // On an E1 and swann cameras:
        //  - mainStream always has a value of 0
        //  - subStream always has a value of 1
        //  - There is no externStram
        // On a B800:
        //  - mainStream is 0
        //  - subStream is 256
        //  - externStram is 1024
        let handle = match stream {
            StreamKind::Main => 0,
            StreamKind::Sub => 256,
            StreamKind::Extern => 1024,
        };

        let stop_video = Bc::new_from_xml(
            BcMeta {
                msg_id: MSG_ID_VIDEO_STOP,
                channel_id: self.channel_id,
                msg_num,
                stream_type: stream_code,
                response_code: 0,
                class: 0x6414, // IDK why
            },
            BcXml {
                preview: Some(Preview {
                    version: xml_ver(),
                    channel_id: self.channel_id,
                    handle,
                    stream_type: None,
                }),
                ..Default::default()
            },
        );

        sub_video.send(stop_video).await?;

        let reply = sub_video.recv().await?;
        if reply.meta.response_code != 200 {
            return Err(Error::CameraServiceUnavailable {
                id: reply.meta.msg_id,
                code: reply.meta.response_code,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bcmedia::model::{BcMediaIframe, BcMediaPframe, VideoType};
    use tokio_stream::wrappers::ReceiverStream;

    fn iframe(n: u8) -> Result<BcMedia> {
        Ok(BcMedia::Iframe(BcMediaIframe {
            video_type: VideoType::H264,
            microseconds: 0,
            time: None,
            data: vec![n],
        }))
    }

    fn pframe(n: u8) -> Result<BcMedia> {
        Ok(BcMedia::Pframe(BcMediaPframe {
            video_type: VideoType::H264,
            microseconds: 0,
            data: vec![n],
        }))
    }

    fn frame_number(media: Result<BcMedia>) -> u8 {
        match media.unwrap() {
            BcMedia::Iframe(BcMediaIframe { data, .. })
            | BcMedia::Pframe(BcMediaPframe { data, .. }) => data[0],
            _ => panic!("not video"),
        }
    }

    #[tokio::test]
    async fn video_resumes_at_a_keyframe_once_the_reader_catches_up() {
        let (camera, from_camera) = channel(16);
        let (to_app, mut app) = channel(2);
        let forwarding = tokio::spawn(async move {
            forward_media(&mut ReceiverStream::new(from_camera), &to_app).await
        });

        // The app reads nothing yet: 1 and 2 fit, 3 and 4 do not
        for frame in [iframe(1), pframe(2), pframe(3), pframe(4)] {
            camera.send(frame).await.unwrap();
        }
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(frame_number(app.recv().await.unwrap()), 1);
        assert_eq!(frame_number(app.recv().await.unwrap()), 2);

        // With room again, 5 still depends on what was dropped, 6 does not
        for frame in [pframe(5), iframe(6), pframe(7)] {
            camera.send(frame).await.unwrap();
        }
        drop(camera);
        forwarding.await.unwrap();
        assert_eq!(frame_number(app.recv().await.unwrap()), 6);
        assert_eq!(frame_number(app.recv().await.unwrap()), 7);
        assert!(app.recv().await.is_none());
    }

    #[tokio::test]
    async fn reading_past_the_end_of_a_stream_never_panics() {
        let (_tx, rx) = channel(1);
        let mut data = StreamData {
            handle: Some(tokio::spawn(async { Ok(()) })),
            rx,
            abort_handle: CancellationToken::new(),
        };
        while !data.handle.as_ref().is_some_and(|h| h.is_finished()) {
            tokio::task::yield_now().await;
        }

        // Awaiting the finished task a second time used to panic
        assert!(matches!(data.get_data().await, Err(Error::StreamFinished)));
        assert!(matches!(data.get_data().await, Err(Error::StreamFinished)));
        assert!(data.shutdown().await.is_ok());
    }
}
