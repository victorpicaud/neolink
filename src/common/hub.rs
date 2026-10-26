//! One camera stream, shared by every client of it
//!
//! Every RTSP client used to start a video stream of its own on the camera,
//! and to pause and resume it on its own. A hub runs one stream per kind while
//! anyone subscribes, pauses it as the config asks, and hands its frames to
//! every subscriber. It keeps the frames since the last keyframe, so that a new
//! client can show a picture at once.
use anyhow::anyhow;
use futures::{future::BoxFuture, FutureExt};
use neolink_core::{
    bc_protocol::StreamKind,
    bcmedia::model::{BcMedia, VideoType},
};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use tokio::{
    sync::{broadcast, mpsc, watch},
    task::JoinHandle,
    time::{sleep, sleep_until, Duration, Instant},
};

#[cfg(feature = "pushnoti")]
use super::PushNoti;
use super::{Backoff, MdState, NeoInstance};
use crate::{config::CameraConfig, AnyResult};

/// How long the camera keeps streaming after the last subscriber leaves, for
/// a client that reconnects at once
const LINGER: Duration = Duration::from_secs(10);
/// How long a paused stream runs on for a new subscriber once it has sent a
/// keyframe: long enough to learn the stream type and show a picture
const WARM_UP: Duration = Duration::from_secs(5);
/// How long a motion alert unpauses the stream
const ALERT_WINDOW: Duration = Duration::from_secs(30);
/// A camera stream that ran this long worked: if it fails, it is restarted
/// without waiting long
const HEALTHY_RUN: Duration = Duration::from_secs(60);
/// How many frames a subscriber may fall behind before it skips to the next
/// keyframe
const EVENT_CAPACITY: usize = 128;
/// The most the frames kept for new subscribers may take. A longer GOP is not
/// kept: new subscribers then wait for the next keyframe.
const MAX_CACHED_BYTES: usize = 8 * 1024 * 1024;
/// How long after the last frame the kept ones still count as live. A camera
/// that stopped sending frames would otherwise show new clients a picture from
/// before it stopped as if it were live.
const STALE_AFTER: Duration = Duration::from_secs(3);
/// How long a stream that someone watches may go without video before it is
/// restarted. A camera can stop sending video while it answers everything
/// else, and its clients then froze until they reconnected.
const VIDEO_TIMEOUT: Duration = Duration::from_secs(15);
/// After this many restarts in a row that brought no video back, the camera
/// is reconnected
const RESTARTS_BEFORE_RECONNECT: u32 = 2;

/// What a hub sends its subscribers
#[derive(Clone)]
enum HubEvent {
    /// A frame from the camera
    Media(Arc<BcMedia>),
    /// The camera changed codec: a pipeline built for the old one cannot play
    /// what follows
    Reconfigured,
}

/// What a camera stream sends its hub
enum SourceEvent {
    /// The camera accepted the stream: video should follow. Until then, it
    /// may not even be connected.
    Started,
    Frame(BcMedia),
}

/// Runs the camera stream, sending its frames, until it ends or fails. It is
/// stopped by dropping the future.
type StreamSource =
    Box<dyn Fn(mpsc::Sender<SourceEvent>) -> BoxFuture<'static, AnyResult<()>> + Send + Sync>;

/// Reconnects to the camera
type Reconnect = Box<dyn Fn() + Send + Sync>;

#[derive(Clone, Copy, Default)]
struct Subscribers {
    /// How many there are
    count: usize,
    /// How many there ever were, which tells that someone new subscribed
    joined: u64,
}

/// The frames kept for new subscribers, and the channel to the others
struct Cache {
    events: broadcast::Sender<HubEvent>,
    /// The latest stream info
    info: Option<Arc<BcMedia>>,
    /// The frames since the last keyframe, starting with it. Empty while no
    /// whole GOP fits.
    gop: Vec<Arc<BcMedia>>,
    gop_bytes: usize,
    /// When the last frame came
    last_frame: Option<Instant>,
    /// The codec the subscribers' pipelines were built for. Kept when the
    /// stream stops, as the camera may change codec meanwhile.
    video_type: Option<VideoType>,
}

impl Cache {
    fn publish(&mut self, frame: BcMedia) {
        self.last_frame = Some(Instant::now());
        let size = media_size(&frame);
        let frame = Arc::new(frame);
        match &*frame {
            BcMedia::InfoV1(_) | BcMedia::InfoV2(_) => self.info = Some(frame.clone()),
            BcMedia::Iframe(iframe) => {
                if self
                    .video_type
                    .is_some_and(|video_type| video_type != iframe.video_type)
                {
                    let _ = self.events.send(HubEvent::Reconfigured);
                }
                self.video_type = Some(iframe.video_type);
                self.gop.clear();
                self.gop_bytes = 0;
                self.keep(frame.clone(), size);
            }
            // Only after a keyframe: without one, they cannot be decoded
            _ if !self.gop.is_empty() => self.keep(frame.clone(), size),
            _ => {}
        }
        let _ = self.events.send(HubEvent::Media(frame));
    }

    fn keep(&mut self, frame: Arc<BcMedia>, size: usize) {
        if self.gop_bytes + size > MAX_CACHED_BYTES {
            self.gop.clear();
            self.gop_bytes = 0;
            return;
        }
        self.gop_bytes += size;
        self.gop.push(frame);
    }

    /// Forgets the frames of a stream that stopped: they would show the
    /// camera as it was, not as it is
    fn clear(&mut self) {
        self.info = None;
        self.gop.clear();
        self.gop_bytes = 0;
    }
}

fn media_size(media: &BcMedia) -> usize {
    match media {
        BcMedia::Iframe(frame) => frame.data.len(),
        BcMedia::Pframe(frame) => frame.data.len(),
        BcMedia::Aac(aac) => aac.data.len(),
        BcMedia::Adpcm(adpcm) => adpcm.data.len(),
        BcMedia::InfoV1(_) | BcMedia::InfoV2(_) => 0,
    }
}

/// A subscription to a hub. Dropping it unsubscribes at once, without a
/// runtime.
pub(crate) struct HubSubscription {
    /// What the hub kept: the latest stream info, and the frames since the
    /// last keyframe
    cached: Vec<Arc<BcMedia>>,
    events: broadcast::Receiver<HubEvent>,
    subscribers: Arc<watch::Sender<Subscribers>>,
}

impl Drop for HubSubscription {
    fn drop(&mut self) {
        self.subscribers
            .send_modify(|subscribers| subscribers.count = subscribers.count.saturating_sub(1));
    }
}

impl HubSubscription {
    /// Sends the frames to `tx`, as the camera used to stream to each client:
    /// the ones kept, then the new ones. Ends as soon as `tx` closes, even
    /// while no frames come, which lets go of the hub.
    pub(crate) async fn forward(mut self, tx: mpsc::Sender<BcMedia>) {
        let mut cached = std::mem::take(&mut self.cached).into_iter();
        // Video starts at a keyframe: the frames before one cannot be decoded
        let mut need_keyframe = true;
        let mut sent_video = false;
        loop {
            let frame = match cached.next() {
                Some(frame) => frame,
                None => {
                    let event = tokio::select! {
                        _ = tx.closed() => return,
                        event = self.events.recv() => event,
                    };
                    match event {
                        Ok(HubEvent::Media(frame)) => frame,
                        // A pipeline built for the old codec cannot play the
                        // new one: its client has to start again
                        Ok(HubEvent::Reconfigured) if sent_video => return,
                        Ok(HubEvent::Reconfigured) => continue,
                        Err(broadcast::error::RecvError::Closed) => return,
                        Err(broadcast::error::RecvError::Lagged(missed)) => {
                            log::debug!("A stream client fell {missed} frames behind");
                            need_keyframe = true;
                            continue;
                        }
                    }
                }
            };
            match &*frame {
                BcMedia::Iframe(_) => need_keyframe = false,
                BcMedia::Pframe(_) if need_keyframe => continue,
                _ => {}
            }
            sent_video |= matches!(&*frame, BcMedia::Iframe(_) | BcMedia::Pframe(_));
            if tx.send(Arc::unwrap_or_clone(frame)).await.is_err() {
                return;
            }
        }
    }
}

/// One camera stream, shared by its subscribers
pub(crate) struct StreamHub {
    /// Owned by the hub's task, so that its subscriptions end with it
    cache: Weak<Mutex<Cache>>,
    subscribers: Arc<watch::Sender<Subscribers>>,
    task: JoinHandle<()>,
}

impl Drop for StreamHub {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl StreamHub {
    /// The hub for one of a camera's streams, paused as its config asks
    pub(crate) fn for_camera(
        instance: NeoInstance,
        config: watch::Receiver<CameraConfig>,
        stream: StreamKind,
    ) -> Self {
        let name = format!("{}::{stream}", config.borrow().name);
        let (gate_tx, gate) = watch::channel(!config.borrow().pause.on_motion);
        tokio::task::spawn(run_gate(
            name.clone(),
            instance.clone(),
            config.clone(),
            gate_tx,
        ));
        let reconnect_instance = instance.clone();
        let reconnect: Reconnect = Box::new(move || {
            let instance = reconnect_instance.clone();
            tokio::task::spawn(async move {
                if let Err(e) = instance.reconnect().await {
                    log::debug!("Could not reconnect the camera: {e:#}");
                }
            });
        });
        Self::new(
            name,
            camera_source(instance, config, stream),
            gate,
            reconnect,
        )
    }

    /// Runs `source` while anyone subscribes and `gate` is open, or a new
    /// subscriber still needs frames
    fn new(
        name: String,
        source: StreamSource,
        gate: watch::Receiver<bool>,
        reconnect: Reconnect,
    ) -> Self {
        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        let cache = Arc::new(Mutex::new(Cache {
            events,
            info: None,
            gop: vec![],
            gop_bytes: 0,
            last_frame: None,
            video_type: None,
        }));
        let (subscribers, subscribed) = watch::channel(Subscribers::default());
        Self {
            cache: Arc::downgrade(&cache),
            subscribers: Arc::new(subscribers),
            task: tokio::task::spawn(run(name, cache, source, subscribed, gate, reconnect)),
        }
    }

    /// A new subscription, or None once the hub has stopped for good
    pub(crate) fn subscribe(&self) -> Option<HubSubscription> {
        let cache = self.cache.upgrade()?;
        // Under the lock, so that no frame comes twice or never
        let cache = cache.lock().unwrap_or_else(PoisonError::into_inner);
        self.subscribers.send_modify(|subscribers| {
            subscribers.count += 1;
            subscribers.joined += 1;
        });
        let live = cache
            .last_frame
            .is_some_and(|last_frame| last_frame.elapsed() < STALE_AFTER);
        let cached = match live {
            true => cache.info.iter().chain(cache.gop.iter()).cloned().collect(),
            false => vec![],
        };
        Some(HubSubscription {
            cached,
            events: cache.events.subscribe(),
            subscribers: self.subscribers.clone(),
        })
    }
}

/// Why a camera stream stopped
enum Stopped {
    /// Nobody wants it, or it is paused
    Unwanted,
    /// It ended by itself
    Ended(AnyResult<()>),
}

/// Runs the camera stream while it is wanted. Returns once the gate is gone,
/// which drops the cache and so ends every subscription.
async fn run(
    name: String,
    cache: Arc<Mutex<Cache>>,
    source: StreamSource,
    mut subscribers: watch::Receiver<Subscribers>,
    mut gate: watch::Receiver<bool>,
    reconnect: Reconnect,
) {
    let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(60));
    // Restarts in a row that brought no video back
    let mut restarts = 0;
    let mut joined = 0;
    // Someone subscribed since the last keyframe. They need frames to learn
    // the stream type and show a picture, even while the stream is paused.
    let mut warming = false;
    loop {
        loop {
            let now = *subscribers.borrow_and_update();
            if now.joined != joined {
                joined = now.joined;
                warming = true;
            }
            if now.count > 0 && (warming || *gate.borrow_and_update()) {
                break;
            }
            let gone = tokio::select! {
                v = subscribers.changed() => v.is_err(),
                v = gate.changed() => v.is_err(),
            };
            if gone {
                return;
            }
        }

        log::debug!("{name}: Starting the camera stream");
        let started = Instant::now();
        let (frames_tx, mut frames) = mpsc::channel(100);
        let mut stream = source(frames_tx);
        let mut warm_until = None;
        let mut idle_since = None;
        // When video last came, or the camera accepted the stream
        let mut last_video = None;
        let stopped = loop {
            if !*gate.borrow_and_update() && !warming && warm_until.is_none() {
                break Stopped::Unwanted;
            }
            tokio::select! {
                biased;
                Some(event) = frames.recv() => {
                    let frame = match event {
                        SourceEvent::Started => {
                            last_video = Some(Instant::now());
                            continue;
                        }
                        SourceEvent::Frame(frame) => frame,
                    };
                    if warming && matches!(frame, BcMedia::Iframe(_)) {
                        warming = false;
                        warm_until = Some(Instant::now() + WARM_UP);
                    }
                    if matches!(frame, BcMedia::Iframe(_) | BcMedia::Pframe(_)) {
                        last_video = Some(Instant::now());
                        restarts = 0;
                    }
                    cache.lock().unwrap_or_else(PoisonError::into_inner).publish(frame);
                }
                result = &mut stream => break Stopped::Ended(result),
                v = subscribers.changed() => {
                    if v.is_err() {
                        return;
                    }
                    let now = *subscribers.borrow_and_update();
                    if now.joined != joined {
                        joined = now.joined;
                        warming = true;
                    }
                    idle_since = match now.count {
                        0 => idle_since.or_else(|| Some(Instant::now())),
                        _ => None,
                    };
                }
                v = gate.changed() => if v.is_err() {
                    return;
                },
                _ = until(warm_until) => warm_until = None,
                _ = until(idle_since.map(|since| since + LINGER)) => break Stopped::Unwanted,
                // Only for someone watching
                _ = until(last_video.map(|last| last + VIDEO_TIMEOUT)), if idle_since.is_none() => {
                    restarts += 1;
                    break Stopped::Ended(Err(anyhow!(
                        "No video for {}s",
                        VIDEO_TIMEOUT.as_secs()
                    )));
                }
            }
        };
        drop(stream);
        cache.lock().unwrap_or_else(PoisonError::into_inner).clear();

        match stopped {
            Stopped::Unwanted => {
                log::debug!("{name}: Stopped the camera stream");
                backoff.reset();
                restarts = 0;
            }
            Stopped::Ended(result) => {
                if restarts >= RESTARTS_BEFORE_RECONNECT {
                    log::warn!(
                        "{name}: Restarting the stream brought no video back, reconnecting to the camera"
                    );
                    reconnect();
                    restarts = 0;
                }
                if started.elapsed() >= HEALTHY_RUN {
                    backoff.reset();
                }
                let delay = backoff.next_delay();
                match result {
                    Ok(()) => {
                        log::debug!("{name}: The camera stream ended, restarting in {delay:.1?}")
                    }
                    Err(e) => log::warn!(
                        "{name}: The camera stream failed, restarting in {delay:.1?}: {e:#}"
                    ),
                }
                sleep(delay).await;
            }
        }
    }
}

/// Sleeps until `deadline`, or forever without one
async fn until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Streams from the camera, keeping it connected meanwhile
fn camera_source(
    instance: NeoInstance,
    config: watch::Receiver<CameraConfig>,
    stream: StreamKind,
) -> StreamSource {
    Box::new(move |events| {
        let instance = instance.clone();
        let config = config.clone();
        async move {
            instance
                .run_task(move |cam| {
                    let events = events.clone();
                    // Read on every run: a changed config reconnects the
                    // camera, which runs this again
                    let strict = config.borrow().strict;
                    Box::pin(async move {
                        let mut media = cam.start_video(stream, 0, strict).await?;
                        if events.send(SourceEvent::Started).await.is_err() {
                            return Ok(());
                        }
                        loop {
                            let frame = media.get_data().await??;
                            if events.send(SourceEvent::Frame(frame)).await.is_err() {
                                break AnyResult::Ok(());
                            }
                        }
                    })
                })
                .await
        }
        .boxed()
    })
}

/// Keeps `gate` open while the stream may run: always, or with
/// `pause.on_motion` while the camera sees motion and for a while after a
/// motion alert. Ends once the hub is gone.
async fn run_gate(
    name: String,
    instance: NeoInstance,
    mut config: watch::Receiver<CameraConfig>,
    gate: watch::Sender<bool>,
) {
    let motion = tokio::select! {
        _ = gate.closed() => return,
        motion = instance.motion() => motion,
    };
    let mut motion = match motion {
        Ok(motion) => Some(motion),
        Err(e) => {
            log::warn!("{name}: Cannot follow the camera's motion: {e:#}");
            None
        }
    };
    let mut alerts = MotionAlerts::new(&instance);
    let mut alerted_until = None;
    // Once motion ends, the stream keeps going until then
    let mut moved_until = None;
    let mut was_moving = false;
    let mut announced = None;
    loop {
        let (on_motion, motion_timeout) = {
            let config = config.borrow_and_update();
            (config.pause.on_motion, config.pause.motion_timeout)
        };
        let moving = motion
            .as_mut()
            .is_some_and(|motion| matches!(*motion.borrow_and_update(), MdState::Start(_)));
        if was_moving && !moving {
            // Even a config that skipped validation must not panic here
            let timeout = Duration::try_from_secs_f64(motion_timeout.clamp(0.0, 3600.0));
            moved_until = Some(Instant::now() + timeout.unwrap_or_default());
        }
        was_moving = moving;
        let open = !on_motion || moving || moved_until.is_some() || alerted_until.is_some();
        gate.send_if_modified(|gate| std::mem::replace(gate, open) != open);
        if on_motion && announced != Some(open) {
            if open {
                log::info!("{name}: Unpaused");
            } else {
                log::info!("{name}: Paused until motion");
            }
            announced = Some(open);
        }
        tokio::select! {
            _ = gate.closed() => return,
            v = config.changed() => if v.is_err() {
                return;
            },
            v = changed(&mut motion) => if v.is_err() {
                motion = None;
            },
            _ = alerts.next() => alerted_until = Some(Instant::now() + ALERT_WINDOW),
            _ = until(alerted_until) => alerted_until = None,
            _ = until(moved_until) => moved_until = None,
        }
    }
}

/// Waits for a change, or forever without a watch
async fn changed<T>(watch: &mut Option<watch::Receiver<T>>) -> Result<(), watch::error::RecvError> {
    match watch {
        Some(watch) => watch.changed().await,
        None => std::future::pending().await,
    }
}

/// The camera's motion alerts, which come as push notifications
struct MotionAlerts {
    #[cfg(feature = "pushnoti")]
    instance: NeoInstance,
    #[cfg(feature = "pushnoti")]
    notifications: Option<watch::Receiver<Option<PushNoti>>>,
    /// Set once they cannot come
    #[cfg(feature = "pushnoti")]
    unavailable: bool,
}

impl MotionAlerts {
    fn new(_instance: &NeoInstance) -> Self {
        Self {
            #[cfg(feature = "pushnoti")]
            instance: _instance.clone(),
            #[cfg(feature = "pushnoti")]
            notifications: None,
            #[cfg(feature = "pushnoti")]
            unavailable: false,
        }
    }

    /// Resolves on the next motion alert
    #[cfg(feature = "pushnoti")]
    async fn next(&mut self) {
        if self.unavailable {
            return std::future::pending().await;
        }
        let notifications = match &mut self.notifications {
            Some(notifications) => notifications,
            None => match self.instance.push_notifications().await {
                Ok(mut notifications) => {
                    // Only the ones from now on
                    notifications.borrow_and_update();
                    self.notifications.insert(notifications)
                }
                Err(e) => {
                    log::debug!("No push notifications to unpause on: {e:#}");
                    self.unavailable = true;
                    return std::future::pending().await;
                }
            },
        };
        loop {
            if notifications.changed().await.is_err() {
                self.unavailable = true;
                return std::future::pending().await;
            }
            let alert = notifications
                .borrow_and_update()
                .as_ref()
                .is_some_and(|noti| noti.message.contains("Motion Alert from"));
            if alert {
                return;
            }
        }
    }

    /// Resolves on the next motion alert: never, without push notifications
    #[cfg(not(feature = "pushnoti"))]
    async fn next(&mut self) {
        std::future::pending().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::NeoCamCommand;
    use neolink_core::bcmedia::model::{BcMediaAac, BcMediaIframe, BcMediaPframe};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio_util::sync::CancellationToken;

    fn iframe(n: u8) -> BcMedia {
        keyframe(VideoType::H264, n)
    }

    fn keyframe(video_type: VideoType, n: u8) -> BcMedia {
        BcMedia::Iframe(BcMediaIframe {
            video_type,
            microseconds: 0,
            time: None,
            data: vec![n; 16],
        })
    }

    fn pframe(n: u8) -> BcMedia {
        BcMedia::Pframe(BcMediaPframe {
            video_type: VideoType::H264,
            microseconds: 0,
            data: vec![n; 16],
        })
    }

    fn number(media: &BcMedia) -> u8 {
        match media {
            BcMedia::Iframe(frame) => frame.data[0],
            BcMedia::Pframe(frame) => frame.data[0],
            _ => panic!("not video"),
        }
    }

    /// A pretend camera: counts its streams, and streams what the test sends
    /// while they run
    struct Camera {
        starts: Arc<AtomicUsize>,
        frames: broadcast::Sender<BcMedia>,
    }

    impl Camera {
        fn streaming(&self) -> bool {
            self.frames.receiver_count() > 0
        }

        fn starts(&self) -> usize {
            self.starts.load(Ordering::SeqCst)
        }
    }

    fn camera() -> (Camera, StreamSource) {
        let starts = Arc::new(AtomicUsize::new(0));
        let (frames, _) = broadcast::channel(1024);
        let source_starts = starts.clone();
        let source_frames = frames.clone();
        let source: StreamSource = Box::new(move |out: mpsc::Sender<SourceEvent>| {
            source_starts.fetch_add(1, Ordering::SeqCst);
            let mut frames = source_frames.subscribe();
            async move {
                out.send(SourceEvent::Started).await?;
                while let Ok(frame) = frames.recv().await {
                    out.send(SourceEvent::Frame(frame)).await?;
                }
                Ok(())
            }
            .boxed()
        });
        (Camera { starts, frames }, source)
    }

    fn hub(source: StreamSource, open: bool) -> (StreamHub, watch::Sender<bool>) {
        let (hub, gate, _) = hub_reconnecting(source, open);
        (hub, gate)
    }

    /// A hub, and how many times it reconnected the camera
    fn hub_reconnecting(
        source: StreamSource,
        open: bool,
    ) -> (StreamHub, watch::Sender<bool>, Arc<AtomicUsize>) {
        let (gate, gated) = watch::channel(open);
        let reconnects = Arc::new(AtomicUsize::new(0));
        let counted = reconnects.clone();
        let reconnect: Reconnect = Box::new(move || {
            counted.fetch_add(1, Ordering::SeqCst);
        });
        let hub = StreamHub::new("test".to_string(), source, gated, reconnect);
        (hub, gate, reconnects)
    }

    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    /// Lets `duration` pass with the camera sending a frame a second while it
    /// streams
    async fn stream_for(camera: &Camera, duration: Duration) {
        let end = Instant::now() + duration;
        while Instant::now() < end {
            let _ = camera.frames.send(iframe(0));
            sleep(Duration::from_secs(1)).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_camera_streams_once_for_all_its_subscribers() {
        let (camera, source) = camera();
        let (hub, _gate) = hub(source, true);
        settle().await;
        assert_eq!(camera.starts(), 0);

        let first = hub.subscribe().unwrap();
        let second = hub.subscribe().unwrap();
        settle().await;
        assert_eq!(camera.starts(), 1);

        drop(first);
        drop(second);
        // It lingers for a client that comes straight back
        sleep(LINGER / 2).await;
        let back = hub.subscribe().unwrap();
        stream_for(&camera, LINGER * 2).await;
        assert_eq!(camera.starts(), 1);
        assert!(camera.streaming());

        drop(back);
        sleep(LINGER * 2).await;
        assert!(!camera.streaming());
    }

    #[tokio::test(start_paused = true)]
    async fn a_paused_stream_runs_until_a_new_subscriber_has_a_picture() {
        let (camera, source) = camera();
        let (hub, gate) = hub(source, false);
        let _subscription = hub.subscribe().unwrap();
        settle().await;
        assert!(camera.streaming(), "a new subscriber needs frames");
        camera.frames.send(iframe(1)).unwrap();
        settle().await;
        sleep(WARM_UP / 2).await;
        assert!(camera.streaming());
        sleep(WARM_UP).await;
        assert!(!camera.streaming(), "paused once it had its picture");

        gate.send_replace(true);
        settle().await;
        assert!(camera.streaming());
        gate.send_replace(false);
        settle().await;
        assert!(!camera.streaming(), "paused at once");
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_subscriber_gets_the_frames_since_the_last_keyframe() {
        let (camera, source) = camera();
        let (hub, _gate) = hub(source, true);
        let _first = hub.subscribe().unwrap();
        settle().await;
        for frame in [iframe(1), pframe(2), iframe(3), pframe(4)] {
            camera.frames.send(frame).unwrap();
        }
        settle().await;

        let late = hub.subscribe().unwrap();
        let cached: Vec<u8> = late.cached.iter().map(|frame| number(frame)).collect();
        assert_eq!(cached, [3, 4]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stopped_stream_shows_nothing_old_as_new() {
        let (camera, source) = camera();
        let (hub, _gate) = hub(source, true);
        let first = hub.subscribe().unwrap();
        settle().await;
        camera.frames.send(iframe(1)).unwrap();
        settle().await;
        drop(first);
        sleep(LINGER * 2).await;
        assert!(!camera.streaming());

        assert!(hub.subscribe().unwrap().cached.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_stream_shows_nothing_old_as_new() {
        let (camera, source) = camera();
        let (hub, _gate) = hub(source, true);
        let _first = hub.subscribe().unwrap();
        settle().await;
        camera.frames.send(iframe(1)).unwrap();
        settle().await;
        assert_eq!(hub.subscribe().unwrap().cached.len(), 1);

        // The camera stops sending frames, while its stream runs on
        sleep(STALE_AFTER).await;
        assert!(camera.streaming());
        assert!(hub.subscribe().unwrap().cached.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_subscriber_that_falls_behind_resumes_at_a_keyframe() {
        let (camera, source) = camera();
        let (hub, _gate) = hub(source, true);
        let subscription = hub.subscribe().unwrap();
        let (tx, mut rx) = mpsc::channel(1000);
        settle().await;

        // More than the hub holds for a subscriber that does not read
        camera.frames.send(iframe(0)).unwrap();
        for _ in 0..EVENT_CAPACITY * 2 {
            camera.frames.send(pframe(1)).unwrap();
            settle().await;
        }
        camera.frames.send(pframe(2)).unwrap();
        camera.frames.send(iframe(3)).unwrap();
        settle().await;

        let forwarding = tokio::spawn(subscription.forward(tx));
        settle().await;
        let mut got = vec![];
        while let Ok(frame) = rx.try_recv() {
            got.push(number(&frame));
        }
        // The P-frames that would decode against the missed ones are skipped
        assert_eq!(got, [3]);
        forwarding.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_forwarder_lets_go_once_its_client_is_gone() {
        let (camera, source) = camera();
        let (hub, _gate) = hub(source, true);
        let (tx, rx) = mpsc::channel(1);
        let forwarding = tokio::spawn(hub.subscribe().unwrap().forward(tx));
        settle().await;
        assert!(camera.streaming());

        // No frames come, and the client goes
        drop(rx);
        settle().await;
        assert!(forwarding.is_finished());
        sleep(LINGER * 2).await;
        assert!(!camera.streaming());
    }

    #[tokio::test(start_paused = true)]
    async fn a_codec_change_ends_the_streams_built_for_the_old_one() {
        let (camera, source) = camera();
        let (hub, _gate) = hub(source, true);
        let (tx, mut rx) = mpsc::channel(100);
        let forwarding = tokio::spawn(hub.subscribe().unwrap().forward(tx));
        settle().await;
        camera.frames.send(iframe(1)).unwrap();
        settle().await;
        camera.frames.send(keyframe(VideoType::H265, 2)).unwrap();
        settle().await;

        assert!(forwarding.is_finished());
        assert_eq!(rx.recv().await.as_ref().map(number), Some(1));
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_codec_change_while_nobody_watched_is_just_the_new_codec() {
        let (camera, source) = camera();
        let (hub, _gate) = hub(source, true);
        let first = hub.subscribe().unwrap();
        settle().await;
        camera.frames.send(iframe(1)).unwrap();
        settle().await;
        drop(first);
        sleep(LINGER * 2).await;

        let (tx, mut rx) = mpsc::channel(100);
        let forwarding = tokio::spawn(hub.subscribe().unwrap().forward(tx));
        settle().await;
        camera.frames.send(keyframe(VideoType::H265, 2)).unwrap();
        settle().await;

        assert!(!forwarding.is_finished());
        assert_eq!(rx.recv().await.as_ref().map(number), Some(2));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failing_stream_is_retried_with_backoff() {
        let starts = Arc::new(AtomicUsize::new(0));
        let counted = starts.clone();
        let source: StreamSource = Box::new(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Err(anyhow!("The camera is offline")) }.boxed()
        });
        let (hub, _gate) = hub(source, true);
        let _subscription = hub.subscribe().unwrap();
        sleep(Duration::from_secs(60)).await;

        let starts = starts.load(Ordering::SeqCst);
        assert!((2..50).contains(&starts), "{starts} starts in a minute");
    }

    #[tokio::test(start_paused = true)]
    async fn a_hub_that_stopped_ends_its_subscriptions() {
        let (_camera, source) = camera();
        let (hub, gate) = hub(source, true);
        let mut subscription = hub.subscribe().unwrap();
        drop(gate);
        settle().await;

        assert!(matches!(
            subscription.events.recv().await,
            Err(broadcast::error::RecvError::Closed)
        ));
        assert!(hub.subscribe().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_stream_that_stops_sending_video_is_restarted() {
        let (camera, source) = camera();
        let (hub, _gate, reconnects) = hub_reconnecting(source, true);
        let _subscription = hub.subscribe().unwrap();
        settle().await;
        camera.frames.send(iframe(1)).unwrap();
        settle().await;
        sleep(VIDEO_TIMEOUT / 2).await;
        assert_eq!(camera.starts(), 1);

        sleep(VIDEO_TIMEOUT).await;
        assert_eq!(camera.starts(), 2, "restarted once video stopped");
        camera.frames.send(iframe(2)).unwrap();
        settle().await;
        sleep(VIDEO_TIMEOUT / 2).await;
        camera.frames.send(iframe(3)).unwrap();
        settle().await;
        sleep(VIDEO_TIMEOUT / 2).await;
        assert_eq!(camera.starts(), 2, "video came back");
        assert_eq!(reconnects.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn restarts_that_bring_no_video_back_reconnect_the_camera() {
        // A camera that accepts the stream, and sends no video
        let starts = Arc::new(AtomicUsize::new(0));
        let counted = starts.clone();
        let source: StreamSource = Box::new(move |out: mpsc::Sender<SourceEvent>| {
            counted.fetch_add(1, Ordering::SeqCst);
            async move {
                out.send(SourceEvent::Started).await?;
                let audio = BcMedia::Aac(BcMediaAac { data: vec![] });
                out.send(SourceEvent::Frame(audio)).await?;
                std::future::pending::<AnyResult<()>>().await
            }
            .boxed()
        });
        let (hub, _gate, reconnects) = hub_reconnecting(source, true);
        let _subscription = hub.subscribe().unwrap();
        // Two restarts, and the third under way: the second reconnect is
        // due at 60s at the earliest
        sleep(VIDEO_TIMEOUT * 7 / 2).await;

        assert!(starts.load(Ordering::SeqCst) >= 3);
        assert_eq!(reconnects.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_camera_that_never_answers_is_not_reconnected_by_the_hub() {
        // It never accepts the stream: the connection itself is not up, which
        // is not the stream's to fix
        let source: StreamSource =
            Box::new(|_| async { std::future::pending::<AnyResult<()>>().await }.boxed());
        let (hub, _gate, reconnects) = hub_reconnecting(source, true);
        let _subscription = hub.subscribe().unwrap();
        sleep(VIDEO_TIMEOUT * 4).await;
        assert_eq!(reconnects.load(Ordering::SeqCst), 0);
    }

    fn camera_config(on_motion: bool) -> CameraConfig {
        let config: crate::config::Config = toml::from_str(&format!(
            "[[cameras]]
name = \"cam\"
username = \"admin\"
address = \"127.0.0.1:1\"
pause = {{ on_motion = {on_motion}, motion_timeout = 2.0 }}
"
        ))
        .unwrap();
        config.cameras[0].clone()
    }

    #[tokio::test(start_paused = true)]
    async fn the_gate_follows_the_motion_when_pausing_on_it() {
        // A camera that tells its motion, and nothing else
        let (commands_tx, mut commands) = mpsc::channel(10);
        let (motion_tx, motion) = watch::channel(MdState::Unknown);
        tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                if let NeoCamCommand::Motion(reply) = command {
                    let _ = reply.send(motion.clone());
                }
            }
        });
        let instance = NeoInstance::new(
            watch::channel(Weak::new()).1,
            commands_tx,
            CancellationToken::new(),
        )
        .unwrap();
        let (config_tx, config) = watch::channel(camera_config(true));
        let (gate_tx, gate) = watch::channel(true);
        tokio::spawn(run_gate("cam".to_string(), instance, config, gate_tx));
        settle().await;
        assert!(!*gate.borrow(), "paused while nothing moves");

        motion_tx.send_replace(MdState::Start(Instant::now()));
        settle().await;
        assert!(*gate.borrow());
        motion_tx.send_replace(MdState::Stop(Instant::now()));
        settle().await;
        assert!(*gate.borrow(), "streaming on for pause.motion_timeout");
        sleep(Duration::from_secs_f64(2.5)).await;
        assert!(!*gate.borrow());

        config_tx.send_replace(camera_config(false));
        settle().await;
        assert!(*gate.borrow(), "never paused without pause.on_motion");
    }
}
