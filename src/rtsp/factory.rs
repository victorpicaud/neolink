use gstreamer::{glib, ClockTime, PadProbeReturn, PadProbeType};
use std::sync::{
    atomic::{AtomicI64, Ordering},
    mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender},
    Arc, Mutex, PoisonError,
};
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
use tokio::{
    sync::mpsc::{channel as mpsc, error::TrySendError, Receiver as MpscReceiver},
    task::JoinHandle,
    time::{interval, Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::NeoInstance,
    rtsp::gst::{NeoMediaFactory, NeoRtspServer},
    AnyResult,
};

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

pub(super) fn make_dummy_factory(use_splash: bool, pattern: String) -> NeoMediaFactory {
    NeoMediaFactory::new_with_callback(move || {
        if use_splash {
            splash(&pattern)
        } else {
            Err(anyhow!("The camera is not ready yet"))
        }
    })
}

/// How long a new client waits for its pipeline. The wait holds a GStreamer
/// thread, and the factory lock that every other new client of the stream
/// needs, so it has to end even when the camera never sends video.
const BUILD_TIMEOUT: Duration = Duration::from_secs(20);

pub(super) async fn make_factory(
    camera: NeoInstance,
    stream: StreamKind,
    server: NeoRtspServer,
) -> AnyResult<(NeoMediaFactory, JoinHandle<AnyResult<()>>)> {
    let config = camera.config().await?;
    let name = config.borrow().name.clone();
    let (client_tx, mut client_rx) = mpsc::<ClientRequest>(100);
    let close: CloseClient = Arc::new(move |element: &Element| server.close_clients_of(element));
    // Create the task that creates the pipelines
    let thread = tokio::task::spawn({
        let name = name.clone();
        async move {
            while let Some(request) = client_rx.recv().await {
                log::debug!("New client for {name}::{stream}");
                tokio::task::spawn(serve_client(
                    camera.clone(),
                    name.clone(),
                    stream,
                    request,
                    close.clone(),
                ));
            }
            AnyResult::Ok(())
        }
    });

    // Now setup the factory. GStreamer calls this on one of its threads.
    let factory = NeoMediaFactory::new_with_callback(move || {
        let (request, reply) = client_request();
        let built = match client_tx.try_send(request) {
            Ok(()) => reply.wait(BUILD_TIMEOUT),
            Err(TrySendError::Full(_)) => Err(anyhow!("Too many clients are waiting")),
            Err(TrySendError::Closed(_)) => Err(anyhow!("The stream has stopped")),
        };
        built.or_else(|e| {
            let (use_splash, pattern) = {
                let config = config.borrow();
                (config.use_splash, config.splash_pattern.to_string())
            };
            if use_splash {
                log::warn!("{name}::{stream}: {e:#}, sending the splash screen");
                splash(&pattern)
            } else {
                Err(e)
            }
        })
    });
    Ok((factory, thread))
}

/// What both ends of a client's request share
struct RequestState {
    /// Taken by whichever end gets to it first: the builder to deliver the
    /// pipeline, or the waiter to give up on it. A pipeline is therefore
    /// either served and streamed into, or dropped, never both.
    reply: Mutex<Option<SyncSender<AnyResult<Element>>>>,
    /// Cancelled when the waiter gives up, to stop the builder
    abandoned: CancellationToken,
}

impl RequestState {
    fn take_reply(&self) -> Option<SyncSender<AnyResult<Element>>> {
        self.reply
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

/// A new client's request for a pipeline, handled by the factory's task
struct ClientRequest(Arc<RequestState>);

/// The end of a [`ClientRequest`] where GStreamer waits for the pipeline
struct ClientReply {
    state: Arc<RequestState>,
    built: Receiver<AnyResult<Element>>,
}

fn client_request() -> (ClientRequest, ClientReply) {
    let (reply, built) = sync_channel(1);
    let state = Arc::new(RequestState {
        reply: Mutex::new(Some(reply)),
        abandoned: CancellationToken::new(),
    });
    (ClientRequest(state.clone()), ClientReply { state, built })
}

impl ClientRequest {
    /// Resolves once the client has stopped waiting
    async fn abandoned(&self) {
        self.0.abandoned.cancelled().await
    }

    /// Answers the client. Returns false if it stopped waiting first: the
    /// pipeline must then be dropped without streaming into it.
    fn deliver(self, built: AnyResult<Element>) -> bool {
        match self.0.take_reply() {
            Some(reply) => {
                // The only message on a channel with room for one
                let _ = reply.try_send(built);
                true
            }
            None => false,
        }
    }
}

impl Drop for ClientRequest {
    fn drop(&mut self) {
        // Dropped unanswered, e.g. its task panicked: release the client now
        // rather than at the deadline
        self.0.take_reply();
    }
}

impl ClientReply {
    /// Waits up to `timeout` for the pipeline, then gives up on it
    fn wait(self, timeout: Duration) -> AnyResult<Element> {
        match self.built.recv_timeout(timeout) {
            Ok(built) => built,
            Err(RecvTimeoutError::Timeout) if self.state.take_reply().is_some() => {
                self.state.abandoned.cancel();
                Err(anyhow!(
                    "The camera sent no video within {}s",
                    timeout.as_secs()
                ))
            }
            // The builder claimed the request just in time: its answer is on
            // the way
            Err(RecvTimeoutError::Timeout) => self
                .built
                .recv()
                .unwrap_or_else(|_| Err(anyhow!("The pipeline could not be built"))),
            Err(RecvTimeoutError::Disconnected) => Err(anyhow!("The pipeline could not be built")),
        }
    }
}

/// Disconnects the clients that play a pipeline
type CloseClient = Arc<dyn Fn(&Element) + Send + Sync>;

/// Starts the camera stream for one client and builds its pipeline. Frames
/// only flow if the client is still waiting; otherwise everything is dropped,
/// which stops the camera stream again.
async fn serve_client(
    camera: NeoInstance,
    name: String,
    stream: StreamKind,
    request: ClientRequest,
    close: CloseClient,
) {
    let built = tokio::select! {
        _ = request.abandoned() => {
            log::debug!("{name}::{stream}: The client stopped waiting");
            return;
        }
        built = prepare_client(&camera, &name, stream) => built,
    };
    match built {
        Ok(client) => {
            log::trace!("{name}::{stream}: Sending pipeline to gstreamer");
            if request.deliver(Ok(client.element.clone())) {
                client.start_pump(name, stream, close);
            }
        }
        Err(e) => {
            request.deliver(Err(e));
        }
    }
}

/// A client's pipeline, ready for frames
struct ClientPipeline {
    element: Element,
    vid_src: Option<AppSrc>,
    aud_src: Option<AppSrc>,
    /// When video last left the pipeline, in GLib's monotonic microseconds.
    /// Zero until it does.
    egress: Arc<AtomicI64>,
    /// The frames received while learning the stream type
    buffered: Vec<BcMedia>,
    media_rx: MpscReceiver<BcMedia>,
}

async fn prepare_client(
    camera: &NeoInstance,
    name: &str,
    stream: StreamKind,
) -> AnyResult<ClientPipeline> {
    log::trace!("{name}::{stream}: Starting camera");

    // Start the camera
    let config = camera.config().await?.borrow().clone();
    let mut media_rx = camera.stream_while_live(stream).await?;

    log::trace!("{name}::{stream}: Learning camera stream type");
    // Learn the camera data type
    let mut buffered = vec![];
    let mut frame_count = 0usize;

    let mut stream_config = StreamConfig::new(camera, stream).await?;
    while let Some(media) = media_rx.recv().await {
        stream_config.update_from_media(&media);
        buffered.push(media);
        if frame_count > 10
            || (stream_config.vid_type.is_some() && stream_config.aud_type.is_some())
        {
            break;
        }
        frame_count += 1;
    }

    log::trace!("{name}::{stream}: Building the pipeline");
    let element = Bin::new().upcast::<Element>();
    // Build the right video pipeline
    let vid_src = match stream_config.vid_type.as_ref() {
        Some(VideoType::H264) => Some(build_h264(&element, &stream_config)?),
        Some(VideoType::H265) => Some(build_h265(&element, &stream_config)?),
        None => {
            build_unknown(&element, &config.splash_pattern.to_string())?;
            None
        }
    };

    // Build the right audio pipeline
    let aud_src = match stream_config.aud_type.as_ref() {
        Some(AudioType::Aac) => Some(build_aac(&element, &stream_config)?),
        Some(AudioType::Adpcm(block_size)) => {
            Some(build_adpcm(&element, *block_size, &stream_config)?)
        }
        None => None,
    };

    for app in vid_src.iter().chain(aud_src.iter()) {
        app.set_callbacks(
            AppSrcCallbacks::builder()
                .seek_data(move |_, _seek_pos| true)
                .build(),
        );
    }

    let egress = Arc::new(AtomicI64::new(0));
    if vid_src.is_some() {
        watch_egress(&element, &egress);
    }

    Ok(ClientPipeline {
        element,
        vid_src,
        aud_src,
        egress,
        buffered,
        media_rx,
    })
}

/// Keeps `egress` at when video last left the pipeline, from its payloader.
/// A payloader can send buffer lists, which a probe for buffers alone misses.
fn watch_egress(element: &Element, egress: &Arc<AtomicI64>) {
    let payloader = element
        .downcast_ref::<Bin>()
        .and_then(|bin| bin.by_name("pay0"))
        .and_then(|payloader| payloader.static_pad("src"));
    let Some(pad) = payloader else {
        log::debug!("No video payloader to watch");
        return;
    };
    let egress = egress.clone();
    pad.add_probe(
        PadProbeType::BUFFER | PadProbeType::BUFFER_LIST,
        move |_, _| {
            egress.store(glib::monotonic_time(), Ordering::Relaxed);
            PadProbeReturn::Ok
        },
    );
}

/// How often a client's pump checks that the client is still there while no
/// frames come. It used to wait for the next frame to notice, and a paused
/// stream sends none: the pump, its thread and the camera stream it held
/// stayed behind for every client that left meanwhile.
const LIVENESS_CHECK: Duration = Duration::from_secs(1);
/// How long a client may look gone before its pump stops: GStreamer adds the
/// pipeline to the client's media only after the pump has started
const GONE_GRACE: Duration = Duration::from_secs(3);
/// How long a playing client's pipeline may hold back the frames that keep
/// coming before the client is disconnected, so that it starts again. Its
/// stream froze for good otherwise.
const EGRESS_TIMEOUT: Duration = Duration::from_secs(20);
/// Frames count as coming if one came this recently
const FRAMES_COMING: Duration = Duration::from_secs(5);

/// Whether a playing client's pipeline holds back the frames that keep
/// coming. All in GLib's monotonic microseconds.
fn egress_stalled(now: i64, playing_since: Option<i64>, last_out: i64, last_in: i64) -> bool {
    let timeout = EGRESS_TIMEOUT.as_micros() as i64;
    playing_since.is_some_and(|since| now - since >= timeout)
        && now - last_out >= timeout
        && now - last_in < FRAMES_COMING.as_micros() as i64
}

impl ClientPipeline {
    /// Sends the camera's frames into the pipeline until the client is gone,
    /// which lets go of the camera stream
    fn start_pump(self, name: String, stream: StreamKind, close: CloseClient) {
        tokio::task::spawn(self.pump(name, stream, close));
    }

    async fn pump(self, name: String, stream: StreamKind, close: CloseClient) {
        let ClientPipeline {
            element,
            vid_src,
            aud_src,
            egress,
            buffered,
            mut media_rx,
        } = self;
        // GStreamer holds the pipeline while the client is there. Holding it
        // here too would keep the client looking connected after it left.
        let pipeline = element.downgrade();
        drop(element);
        let close_client = || {
            if let Some(element) = pipeline.upgrade() {
                close(&element);
            }
        };
        if vid_src.is_none() && aud_src.is_none() {
            // The splash screen: nothing to send it
            return;
        }
        let mut state = PumpState {
            // A decoder can only start at a keyframe (#427)
            need_keyframe: true,
            ..Default::default()
        };
        let mut liveness = interval(LIVENESS_CHECK);
        liveness.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut gone_since = None;
        let mut playing_since = None;
        let mut last_in = 0;
        let mut buffered = buffered.into_iter();
        loop {
            let frame = match buffered.next() {
                Some(frame) => Some(frame),
                None => tokio::select! {
                    frame = media_rx.recv() => match frame {
                        Some(frame) => Some(frame),
                        None => {
                            log::debug!("{name}::{stream}: The camera stream ended");
                            // So that the client knows, and can start again
                            for app in vid_src.iter().chain(aud_src.iter()) {
                                let _ = app.end_of_stream();
                            }
                            close_client();
                            return;
                        }
                    },
                    _ = liveness.tick() => None,
                },
            };
            if let Some(frame) = frame {
                last_in = glib::monotonic_time();
                if let Err(e) =
                    send_to_sources(frame, &vid_src, &aud_src, &mut state, Instant::now())
                {
                    log::warn!("{name}::{stream}: Could not stream to a client: {e:#}");
                    close_client();
                    return;
                }
            }
            let connected = vid_src
                .iter()
                .chain(aud_src.iter())
                .all(|app| check_live(app).is_ok());
            if connected {
                gone_since = None;
            } else if gone_since.get_or_insert_with(Instant::now).elapsed() >= GONE_GRACE {
                log::debug!("{name}::{stream}: A client left");
                return;
            }
            if let Some(vid_src) = vid_src.as_ref() {
                let now = glib::monotonic_time();
                if vid_src.current_state() == gstreamer::State::Playing {
                    playing_since.get_or_insert(now);
                } else {
                    playing_since = None;
                }
                let last_out = egress.load(Ordering::Relaxed);
                if egress_stalled(now, playing_since, last_out, last_in) {
                    log::warn!(
                        "{name}::{stream}: A client's video stopped moving, disconnecting it so that it can start again"
                    );
                    close_client();
                    return;
                }
            }
        }
    }
}

/// The least time between two video frames, in microseconds: frames that came
/// together still need timestamps a client can tell apart
const MIN_FRAME_GAP: u64 = 1_000;
/// How far audio may drift from the arrival of its frames, in microseconds,
/// before it follows them again
const MAX_AUDIO_DRIFT: u64 = 200_000;

/// Per-client state of the frame pump
#[derive(Default)]
struct PumpState {
    /// When the client's clock started: when the first frame it took arrived.
    /// Frames are stamped with when they arrive. Counting them at the frame
    /// rate the camera claimed drifted from real time as soon as it sent more
    /// or fewer, and clients fell further and further behind (#209, #378).
    epoch: Option<Instant>,
    /// Timestamp of the last video buffer in microseconds. A u32 wrapped
    /// after 71.6 minutes, and the jump back to zero froze the stream.
    vid_ts: Option<u64>,
    /// Where the next audio buffer starts, in microseconds
    aud_ts: Option<u64>,
    /// Video was dropped, so P-frames are skipped until the next keyframe: a
    /// decoder cannot use them without the frames they reference.
    need_keyframe: bool,
    /// The video source refused the last frame, as it does until the client
    /// plays
    refused: bool,
}

impl PumpState {
    /// Microseconds since the client's clock started, starting it if need be
    fn arrival(&mut self, now: Instant) -> u64 {
        let epoch = *self.epoch.get_or_insert(now);
        now.saturating_duration_since(epoch).as_micros() as u64
    }

    /// The timestamp of a video frame: when it arrived, and after the frame
    /// before. The frames kept for a new client all arrive at once, and play
    /// at once.
    fn video_ts(&self, arrival: u64) -> u64 {
        match self.vid_ts {
            Some(last) => arrival.max(last + MIN_FRAME_GAP),
            None => arrival,
        }
    }

    /// The timestamp of an audio frame, or None to drop it. Audio keeps its
    /// own clock, which a gap or a jittery arrival would only break up, unless
    /// it drifts too far from the arrival of the frames.
    fn audio_ts(&mut self, arrival: u64, duration: u32) -> Option<u64> {
        let mut ts = self.aud_ts.unwrap_or(arrival);
        if ts + MAX_AUDIO_DRIFT < arrival {
            // Audio went missing for a while
            ts = arrival;
        } else if ts > arrival + MAX_AUDIO_DRIFT {
            // A burst of old audio, more than there was time for
            return None;
        }
        self.aud_ts = Some(ts + u64::from(duration));
        Some(ts)
    }

    /// The client is not playing. When it does, its media starts at zero, and
    /// at a keyframe.
    fn restart(&mut self) {
        self.epoch = None;
        self.vid_ts = None;
        self.aud_ts = None;
        self.need_keyframe = true;
    }
}

fn send_to_sources(
    data: BcMedia,
    vid_src: &Option<AppSrc>,
    aud_src: &Option<AppSrc>,
    state: &mut PumpState,
    now: Instant,
) -> AnyResult<()> {
    match data {
        BcMedia::Aac(aac) => {
            let Some(duration) = aac.duration() else {
                log::debug!("Dropping AAC frame with an unreadable header");
                return Ok(());
            };
            send_audio(aud_src, aac.data, duration, state, now)?;
        }
        BcMedia::Adpcm(adpcm) => {
            let Some(duration) = adpcm.duration() else {
                log::debug!("Dropping ADPCM frame with an unreadable header");
                return Ok(());
            };
            send_audio(aud_src, adpcm.data, duration, state, now)?;
        }
        BcMedia::Iframe(BcMediaIframe { data, .. }) => {
            send_video(vid_src, data, true, state, now)?;
        }
        BcMedia::Pframe(BcMediaPframe { data, .. }) => {
            send_video(vid_src, data, false, state, now)?;
        }
        _ => {}
    }
    Ok(())
}

fn send_audio(
    aud_src: &Option<AppSrc>,
    data: Vec<u8>,
    duration: u32,
    state: &mut PumpState,
    now: Instant,
) -> AnyResult<()> {
    let Some(aud_src) = aud_src.as_ref() else {
        return Ok(());
    };
    if is_full(aud_src) {
        log::trace!("Audio source is full, dropping a frame");
        return Ok(());
    }
    let arrival = state.arrival(now);
    let Some(ts) = state.audio_ts(arrival, duration) else {
        log::trace!("Audio is ahead of time, dropping a frame");
        return Ok(());
    };
    log::trace!("Sending audio: {:?}", Duration::from_micros(ts));
    if !send_to_appsrc(aud_src, data, Duration::from_micros(ts))? {
        state.restart();
    }
    Ok(())
}

fn send_video(
    vid_src: &Option<AppSrc>,
    data: Vec<u8>,
    keyframe: bool,
    state: &mut PumpState,
    now: Instant,
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
    let arrival = state.arrival(now);
    let ts = state.video_ts(arrival);
    log::trace!("Sending video: {:?}", Duration::from_micros(ts));
    if send_to_appsrc(vid_src, data, Duration::from_micros(ts))? {
        state.vid_ts = Some(ts);
        state.refused = false;
        if keyframe {
            state.need_keyframe = false;
        }
    } else {
        state.restart();
        if !state.refused {
            state.refused = true;
            log::debug!(
                "{}: The client is not playing, dropping video until it does",
                vid_src.name()
            );
        }
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

/// Pushes a frame. Returns false if the source refused it, as it does until
/// the client plays.
fn send_to_appsrc(appsrc: &AppSrc, data: Vec<u8>, mut ts: Duration) -> AnyResult<bool> {
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
                return Ok(false);
            }
        } else {
            // Clock not up yet
            return Ok(false);
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
        Ok(_) => Ok(true),
        // Not playing: between a client's DESCRIBE and PLAY the media is
        // reset. This was logged as "Buffer full", for every frame (#349).
        Err(FlowError::Flushing) => Ok(false),
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

/// A new pipeline showing the "Stream not Ready" splash screen
fn splash(pattern: &str) -> AnyResult<Element> {
    let element = Bin::new().upcast::<Element>();
    build_unknown(&element, pattern)?;
    Ok(element)
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

    // The text needs the pango plugin, which not every install has. The
    // pattern alone still tells the client that the stream exists.
    let overlay = match make_element("textoverlay", "overlay") {
        Ok(overlay) => {
            overlay.set_property("text", "Stream not Ready");
            overlay.set_property_from_str("valignment", "top");
            overlay.set_property_from_str("halignment", "left");
            overlay.set_property("font-desc", "Sans, 16");
            Some(overlay)
        }
        Err(e) => {
            log::debug!("Splash screen without text: {e:#}");
            None
        }
    };
    let encoder = make_element("jpegenc", "encoder")?;
    let payload = make_element("rtpjpegpay", "pay0")?;

    bin.add_many([&source, &queue, &encoder, &payload])?;
    source.link_filtered(
        &queue,
        &Caps::builder("video/x-raw")
            .field("format", "YUY2")
            .field("width", 896i32)
            .field("height", 512i32)
            .field("framerate", gstreamer::Fraction::new(25, 1))
            .build(),
    )?;
    if let Some(overlay) = overlay.as_ref() {
        bin.add(overlay)?;
        Element::link_many([&queue, overlay, &encoder])?;
    } else {
        queue.link(&encoder)?;
    }
    encoder.link(&payload)?;

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
            "textoverlay" => "pango (gst-plugins-base)",
            "jpegenc" => "jpeg (gst-plugins-good)",
            "rtpjpegpay" => "rtp (gst-plugins-good)",
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
            send_to_sources(frame(false, len), &src, &None, &mut state, Instant::now()).unwrap();
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
        let start = Instant::now();
        let mut state = PumpState::default();
        state.arrival(start);
        let later = state.arrival(start + Duration::from_secs(72 * 60));
        assert!(state.video_ts(later) > u64::from(u32::MAX));
        assert!(state.audio_ts(later, 64_000).unwrap() > u64::from(u32::MAX));
    }

    #[test]
    fn zero_fps_falls_back_instead_of_dividing_by_zero() {
        assert_eq!(stream_config(0).frame_period(), Duration::from_millis(40));
    }

    /// The timestamps of the buffers that leave `src`, in microseconds
    fn timestamps(src: &AppSrc) -> Arc<Mutex<Vec<u64>>> {
        let seen = Arc::new(Mutex::new(vec![]));
        let probe_seen = seen.clone();
        src.static_pad("src").unwrap().add_probe(
            gstreamer::PadProbeType::BUFFER,
            move |_, info| {
                if let Some(buffer) = info.buffer() {
                    let pts = buffer.pts().map_or(u64::MAX, |pts| pts.useconds());
                    probe_seen.lock().unwrap().push(pts);
                }
                gstreamer::PadProbeReturn::Ok
            },
        );
        seen
    }

    /// The first `n` timestamps seen, waiting a little for them
    fn first(seen: &Mutex<Vec<u64>>, n: usize) -> Vec<u64> {
        for _ in 0..200 {
            if seen.lock().unwrap().len() >= n {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        seen.lock().unwrap().iter().take(n).copied().collect()
    }

    #[test]
    fn video_is_stamped_with_when_it_arrived() {
        let (pipeline, src) = appsrc_pipeline(1024 * 1024);
        let seen = timestamps(&src);
        pipeline.set_state(gstreamer::State::Playing).unwrap();
        let vid_src = Some(src);
        let mut state = PumpState::default();
        // The camera claims 25 fps, and sends 10
        let start = Instant::now();
        for n in 0..4 {
            let arrival = start + Duration::from_millis(100 * n);
            send_to_sources(frame(n == 0, 16), &vid_src, &None, &mut state, arrival).unwrap();
        }
        let stamps = first(&seen, 4);
        pipeline.set_state(gstreamer::State::Null).unwrap();
        assert_eq!(stamps, [0, 100_000, 200_000, 300_000]);
    }

    #[test]
    fn video_that_arrives_at_once_keeps_its_order() {
        let (pipeline, src) = appsrc_pipeline(1024 * 1024);
        let seen = timestamps(&src);
        pipeline.set_state(gstreamer::State::Playing).unwrap();
        let vid_src = Some(src);
        let mut state = PumpState::default();
        let now = Instant::now();
        for n in 0..4 {
            send_to_sources(frame(n == 0, 16), &vid_src, &None, &mut state, now).unwrap();
        }
        let stamps = first(&seen, 4);
        pipeline.set_state(gstreamer::State::Null).unwrap();
        assert_eq!(stamps, [0, 1_000, 2_000, 3_000]);
    }

    #[test]
    fn audio_keeps_its_own_clock_near_the_arrival_of_its_frames() {
        let start = Instant::now();
        let mut state = PumpState::default();
        let mut at = |ms: u64| {
            let arrival = state.arrival(start + Duration::from_millis(ms));
            state.audio_ts(arrival, 64_000)
        };
        // A little early or late: its own clock
        assert_eq!(at(0), Some(0));
        assert_eq!(at(70), Some(64_000));
        assert_eq!(at(120), Some(128_000));
        // After a gap: when it came
        assert_eq!(at(1_000), Some(1_000_000));
        // A burst of late audio: what gets too far ahead is dropped
        let burst: Vec<_> = (0..5).map(|_| at(1_010)).collect();
        assert_eq!(
            burst,
            [
                Some(1_064_000),
                Some(1_128_000),
                Some(1_192_000),
                None,
                None
            ]
        );
    }

    #[test]
    fn audio_frames_follow_one_another() {
        let (pipeline, src) = appsrc_pipeline(1024 * 1024);
        let seen = timestamps(&src);
        pipeline.set_state(gstreamer::State::Playing).unwrap();
        let aud_src = Some(src);
        let mut state = PumpState::default();
        // AAC at 16 kHz, and IMA ADPCM at 8 kHz
        let aac = BcMediaAac {
            data: vec![0xff, 0xf1, 0x60, 0, 0, 0, 0, 0],
        };
        let adpcm = BcMediaAdpcm { data: vec![0; 260] };
        let aac_duration = u64::from(aac.duration().unwrap());
        let start = Instant::now();
        send_to_sources(
            BcMedia::Aac(aac.clone()),
            &None,
            &aud_src,
            &mut state,
            start,
        )
        .unwrap();
        let later = start + Duration::from_millis(10);
        send_to_sources(BcMedia::Aac(aac), &None, &aud_src, &mut state, later).unwrap();
        send_to_sources(BcMedia::Adpcm(adpcm), &None, &aud_src, &mut state, later).unwrap();
        let stamps = first(&seen, 3);
        pipeline.set_state(gstreamer::State::Null).unwrap();
        assert_eq!(stamps, [0, aac_duration, 2 * aac_duration]);
    }

    #[test]
    fn a_client_that_plays_again_starts_from_zero() {
        // Prepared, then reset, as between a client's DESCRIBE and PLAY
        let (pipeline, src) = appsrc_pipeline(1024 * 1024);
        let seen = timestamps(&src);
        pipeline.set_state(gstreamer::State::Paused).unwrap();
        pipeline.set_state(gstreamer::State::Null).unwrap();
        let vid_src = Some(src);
        let mut state = PumpState::default();
        let start = Instant::now();
        send_to_sources(frame(true, 16), &vid_src, &None, &mut state, start).unwrap();

        pipeline.set_state(gstreamer::State::Playing).unwrap();
        let later = start + Duration::from_secs(5);
        send_to_sources(frame(true, 16), &vid_src, &None, &mut state, later).unwrap();
        let stamps = first(&seen, 1);
        pipeline.set_state(gstreamer::State::Null).unwrap();
        assert_eq!(stamps, [0]);
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
            Instant::now(),
        )
        .unwrap();
        for _ in 0..100 {
            send_to_sources(
                frame(false, 600),
                &vid_src,
                &None,
                &mut state,
                Instant::now(),
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
    fn a_client_that_is_not_playing_yet_starts_at_a_later_keyframe() {
        // Prepared, then reset, as between a client's DESCRIBE and PLAY
        let (pipeline, src) = appsrc_pipeline(1024 * 1024);
        pipeline.set_state(gstreamer::State::Paused).unwrap();
        pipeline.set_state(gstreamer::State::Null).unwrap();
        let vid_src = Some(src);
        let mut state = PumpState::default();
        send_to_sources(frame(true, 16), &vid_src, &None, &mut state, Instant::now()).unwrap();
        assert!(state.need_keyframe, "the client missed that keyframe");

        pipeline.set_state(gstreamer::State::Playing).unwrap();
        send_to_sources(frame(true, 16), &vid_src, &None, &mut state, Instant::now()).unwrap();
        pipeline.set_state(gstreamer::State::Null).unwrap();
        assert!(!state.need_keyframe);
    }

    fn client(
        pipeline: &gstreamer::Pipeline,
        src: AppSrc,
    ) -> (ClientPipeline, tokio::sync::mpsc::Sender<BcMedia>) {
        let (media_tx, media_rx) = mpsc(10);
        let client = ClientPipeline {
            element: pipeline.clone().upcast(),
            vid_src: Some(src),
            aud_src: None,
            egress: Arc::new(AtomicI64::new(0)),
            buffered: vec![],
            media_rx,
        };
        (client, media_tx)
    }

    /// Counts the clients it disconnects
    fn counting_close() -> (CloseClient, Arc<std::sync::atomic::AtomicUsize>) {
        let closed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = closed.clone();
        let close: CloseClient = Arc::new(move |_: &Element| {
            counted.fetch_add(1, Ordering::SeqCst);
        });
        (close, closed)
    }

    #[tokio::test(start_paused = true)]
    async fn a_client_that_left_is_let_go_while_no_frames_come() {
        let (pipeline, src) = appsrc_pipeline(1024 * 1024);
        pipeline.set_state(gstreamer::State::Playing).unwrap();
        let (client, media_tx) = client(&pipeline, src);
        let (close, closed) = counting_close();
        let pump = tokio::spawn(client.pump("cam".to_string(), StreamKind::Main, close));
        tokio::time::sleep(GONE_GRACE * 3).await;
        assert!(!pump.is_finished(), "the client is still there");

        // The client goes while the stream is paused
        pipeline.set_state(gstreamer::State::Null).unwrap();
        drop(pipeline);
        tokio::time::timeout(GONE_GRACE * 3, pump)
            .await
            .expect("the pump stops once its client is gone")
            .unwrap();
        assert!(media_tx.is_closed(), "and lets go of the camera stream");
        assert_eq!(closed.load(Ordering::SeqCst), 0, "nothing left to close");
    }

    #[tokio::test]
    async fn a_client_whose_camera_stream_ended_is_told() {
        let (pipeline, src) = appsrc_pipeline(1024 * 1024);
        pipeline.set_state(gstreamer::State::Playing).unwrap();
        let (client, media_tx) = client(&pipeline, src.clone());
        let (close, closed) = counting_close();
        let pump = tokio::spawn(client.pump("cam".to_string(), StreamKind::Main, close));

        drop(media_tx);
        tokio::time::timeout(Duration::from_secs(5), pump)
            .await
            .expect("the pump stops with the camera stream")
            .unwrap();
        let after = src.push_buffer(gstreamer::Buffer::new());
        pipeline.set_state(gstreamer::State::Null).unwrap();
        assert_eq!(
            after,
            Err(FlowError::Eos),
            "the client got an end of stream"
        );
        assert_eq!(closed.load(Ordering::SeqCst), 1, "and was disconnected");
    }

    #[test]
    fn a_playing_client_that_holds_back_its_video_is_stalled() {
        let s = |seconds: i64| seconds * 1_000_000;
        // Playing for 30s, frames coming, nothing out for 25s
        assert!(egress_stalled(s(100), Some(s(70)), s(75), s(99)));
        // Not playing, or only just
        assert!(!egress_stalled(s(100), None, 0, s(99)));
        assert!(!egress_stalled(s(100), Some(s(90)), 0, s(99)));
        // Video left recently
        assert!(!egress_stalled(s(100), Some(s(70)), s(95), s(99)));
        // No frames came to send: nothing to hold back
        assert!(!egress_stalled(s(100), Some(s(70)), s(75), s(80)));
    }

    #[test]
    fn video_leaving_the_pipeline_is_seen() {
        gstreamer::init().unwrap();
        let pipeline = gstreamer::Pipeline::new();
        let bin = gstreamer::parse::bin_from_description(
            "videotestsrc num-buffers=3 ! jpegenc ! rtpjpegpay name=pay0 ! fakesink",
            false,
        )
        .unwrap();
        pipeline.add(&bin).unwrap();
        let egress = Arc::new(AtomicI64::new(0));
        watch_egress(bin.upcast_ref(), &egress);
        pipeline.set_state(gstreamer::State::Playing).unwrap();
        let _ = pipeline.bus().unwrap().timed_pop_filtered(
            ClockTime::from_seconds(10),
            &[gstreamer::MessageType::Eos, gstreamer::MessageType::Error],
        );
        pipeline.set_state(gstreamer::State::Null).unwrap();
        assert!(egress.load(Ordering::SeqCst) > 0);
    }

    #[test]
    fn the_splash_screen_plays() {
        gstreamer::init().unwrap();
        let bin = gstreamer::Bin::new();
        build_unknown(bin.upcast_ref(), "snow").unwrap();
        let sink = make_element("fakesink", "sink").unwrap();
        bin.add(&sink).unwrap();
        bin.by_name("pay0").unwrap().link(&sink).unwrap();
        let pipeline = gstreamer::Pipeline::new();
        pipeline.add(&bin).unwrap();

        pipeline.set_state(gstreamer::State::Playing).unwrap();
        let message = pipeline.bus().unwrap().timed_pop_filtered(
            ClockTime::from_seconds(10),
            &[
                gstreamer::MessageType::AsyncDone,
                gstreamer::MessageType::Error,
            ],
        );
        pipeline.set_state(gstreamer::State::Null).unwrap();

        let message = message.expect("the splash screen started within 10s");
        assert!(
            matches!(message.view(), gstreamer::MessageView::AsyncDone(_)),
            "{message:?}"
        );
    }

    fn empty_pipeline() -> Element {
        gstreamer::init().unwrap();
        gstreamer::Bin::new().upcast()
    }

    #[test]
    fn a_pipeline_built_in_time_is_served() {
        let (request, reply) = client_request();
        let element = empty_pipeline();
        let builder = std::thread::spawn(move || request.deliver(Ok(element)));

        assert!(reply.wait(Duration::from_secs(5)).is_ok());
        assert!(
            builder.join().unwrap(),
            "a served pipeline must be streamed into"
        );
    }

    #[tokio::test]
    async fn a_late_pipeline_is_dropped_and_its_builder_stopped() {
        let (request, reply) = client_request();

        assert!(reply.wait(Duration::from_millis(10)).is_err());
        tokio::time::timeout(Duration::from_secs(1), request.abandoned())
            .await
            .expect("the builder must be told to stop");
        assert!(
            !request.deliver(Ok(empty_pipeline())),
            "a late pipeline must not be streamed into"
        );
    }

    #[test]
    fn a_failed_builder_releases_the_client_at_once() {
        let (request, reply) = client_request();
        drop(request);

        let started = std::time::Instant::now();
        assert!(reply.wait(Duration::from_secs(10)).is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn a_pipeline_delivered_at_the_deadline_is_served_or_dropped_never_both() {
        for i in 0..100u64 {
            let (request, reply) = client_request();
            let element = empty_pipeline();
            let delay = Duration::from_micros(500 + (i % 10) * 100);
            let builder = std::thread::spawn(move || {
                std::thread::sleep(delay);
                request.deliver(Ok(element))
            });

            let served = reply.wait(Duration::from_millis(1)).is_ok();
            let streamed = builder.join().unwrap();
            assert_eq!(served, streamed, "attempt {i}");
        }
    }

    #[test]
    fn a_dummy_factory_serves_the_splash_screen_or_refuses() {
        use gstreamer_rtsp_server::prelude::*;
        gstreamer::init().unwrap();
        let url = gstreamer_rtsp::RTSPUrl::parse("rtsp://127.0.0.1:8554/cam/main")
            .1
            .unwrap();

        assert!(make_dummy_factory(false, "snow".to_string())
            .construct(&url)
            .is_err());
        let media = make_dummy_factory(true, "snow".to_string())
            .construct(&url)
            .expect("the splash screen");
        // GStreamer hands it over locked, for the client to configure
        media.unlock();
        assert_eq!(media.n_streams(), 1);
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
