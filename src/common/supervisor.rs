//! Keeps a task running for every enabled camera
use anyhow::anyhow;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
};
use tokio::{
    sync::watch::Receiver as WatchReceiver,
    task::{AbortHandle, JoinSet},
    time::{sleep, timeout, Duration, Instant},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

use super::Backoff;
use crate::{config::Config, AnyResult};

/// A task that ran at least this long was healthy, so its next restart is
/// quick again
const HEALTHY_RUN: Duration = Duration::from_secs(60);

/// Runs `start(name)` for every enabled camera. A camera's task starts when
/// the camera is enabled and stops when it is disabled or removed, after which
/// `stopped(name)` cleans up after it. A task that ends on its own, whether it
/// failed, returned or panicked, is restarted with backoff: one camera never
/// takes the others down, and never stays down itself.
///
/// Returns once `cancel` is cancelled and every task has stopped, or with an
/// error if the config can no longer be watched.
pub(crate) async fn supervise_cameras<S, F, C>(
    what: &'static str,
    mut config: WatchReceiver<Config>,
    cancel: CancellationToken,
    start: S,
    stopped: C,
) -> AnyResult<()>
where
    S: Fn(String) -> F + Send + Sync + 'static,
    F: Future<Output = AnyResult<()>> + Send + 'static,
    C: Fn(&str) + Send + Sync + 'static,
{
    let start = Arc::new(start);
    let stopped = Arc::new(stopped);
    let mut running: HashMap<String, (CancellationToken, AbortHandle)> = HashMap::new();
    let mut tasks = JoinSet::new();
    let mut wanted = enabled_cameras(&config.borrow_and_update());
    let result = loop {
        // A camera that is still stopping is started again once it has
        // stopped
        for name in &wanted {
            if !running.contains_key(name) {
                log::info!("{name}: {what} starting");
                let stop = cancel.child_token();
                let task = tasks.spawn(keep_running(
                    what,
                    name.clone(),
                    stop.clone(),
                    start.clone(),
                    stopped.clone(),
                ));
                running.insert(name.clone(), (stop, task));
            }
        }
        for (name, (stop, _)) in running.iter() {
            if !wanted.contains(name) && !stop.is_cancelled() {
                log::info!("{name}: {what} stopping");
                stop.cancel();
            }
        }

        tokio::select! {
            _ = cancel.cancelled() => break Ok(()),
            changed = config.changed() => {
                if changed.is_err() {
                    break Err(anyhow!("The config can no longer be watched"));
                }
                wanted = enabled_cameras(&config.borrow_and_update());
            }
            Some(joined) = tasks.join_next() => {
                if let Err(e) = joined {
                    log::error!("A {what} camera supervisor failed: {e}");
                }
                running.retain(|_, (_, task)| !task.is_finished());
            }
        }
    };

    for (stop, _) in running.values() {
        stop.cancel();
    }
    while tasks.join_next().await.is_some() {}
    result
}

fn enabled_cameras(config: &Config) -> HashSet<String> {
    config
        .cameras
        .iter()
        .filter(|camera| camera.enabled)
        .map(|camera| camera.name.clone())
        .collect()
}

/// Runs one camera's task until `stop` is cancelled, restarting it with
/// backoff whenever it ends on its own
async fn keep_running<S, F, C>(
    what: &'static str,
    name: String,
    stop: CancellationToken,
    start: Arc<S>,
    stopped: Arc<C>,
) where
    S: Fn(String) -> F + Send + Sync + 'static,
    F: Future<Output = AnyResult<()>> + Send + 'static,
    C: Fn(&str) + Send + Sync + 'static,
{
    let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(60));
    loop {
        let started = Instant::now();
        // A task of its own, so that a panic is caught like an error
        let mut task = AbortOnDropHandle::new(tokio::spawn(start(name.clone())));
        let ended = tokio::select! {
            _ = stop.cancelled() => None,
            ended = &mut task => Some(ended),
        };
        let Some(ended) = ended else {
            // Clean up only once the task is gone, or it could undo that
            task.abort();
            let _ = task.await;
            break;
        };
        // A camera being removed takes its task down with it, so give the
        // removal a moment to show before calling this a failure
        if timeout(Duration::from_millis(100), stop.cancelled())
            .await
            .is_ok()
        {
            break;
        }

        if started.elapsed() >= HEALTHY_RUN {
            backoff.reset();
        }
        let delay = backoff.next_delay();
        match ended {
            Ok(Ok(())) => log::warn!("{name}: {what} stopped, restarting in {delay:.1?}"),
            Ok(Err(e)) => log::warn!("{name}: {what} failed, restarting in {delay:.1?}: {e:#}"),
            Err(e) => log::error!("{name}: {what} crashed, restarting in {delay:.1?}: {e}"),
        }
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = sleep(delay) => {}
        }
    }
    stopped(&name);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::sync::watch;

    fn config(cameras: &[&str]) -> Config {
        let mut text = String::from("cameras = []\n");
        if !cameras.is_empty() {
            text.clear();
            for name in cameras {
                text += &format!(
                    "[[cameras]]\nname = \"{name}\"\nusername = \"admin\"\naddress = \"127.0.0.1:9000\"\n"
                );
            }
        }
        toml::from_str(&text).unwrap()
    }

    /// What the supervised tasks did, by camera
    #[derive(Default)]
    struct Log {
        started: Mutex<HashMap<String, usize>>,
        stopped: Mutex<Vec<String>>,
    }

    impl Log {
        fn starts(&self, name: &str) -> usize {
            self.started.lock().unwrap().get(name).copied().unwrap_or(0)
        }
    }

    /// Supervises cameras whose task fails at once if their name starts with
    /// "bad", panics if it starts with "panic", and runs forever otherwise
    fn supervise(
        config: WatchReceiver<Config>,
        cancel: CancellationToken,
        log: Arc<Log>,
    ) -> tokio::task::JoinHandle<AnyResult<()>> {
        let started = log.clone();
        tokio::spawn(supervise_cameras(
            "test",
            config,
            cancel,
            move |name: String| {
                *started
                    .started
                    .lock()
                    .unwrap()
                    .entry(name.clone())
                    .or_default() += 1;
                async move {
                    if name.starts_with("bad") {
                        Err(anyhow!("failed"))
                    } else if name.starts_with("panic") {
                        panic!("crashed")
                    } else {
                        futures::future::pending().await
                    }
                }
            },
            move |name: &str| log.stopped.lock().unwrap().push(name.to_string()),
        ))
    }

    #[tokio::test(start_paused = true)]
    async fn failing_cameras_are_restarted_and_the_others_left_alone() {
        let (_tx, rx) = watch::channel(config(&["good", "bad", "panic"]));
        let log = Arc::new(Log::default());
        let cancel = CancellationToken::new();
        let task = supervise(rx, cancel.clone(), log.clone());

        sleep(Duration::from_secs(600)).await;
        assert!(log.starts("bad") >= 5, "bad: {}", log.starts("bad"));
        assert!(log.starts("panic") >= 5, "panic: {}", log.starts("panic"));
        assert_eq!(log.starts("good"), 1);

        cancel.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn restarts_back_off() {
        let (_tx, rx) = watch::channel(config(&["bad"]));
        let log = Arc::new(Log::default());
        let task = supervise(rx, CancellationToken::new(), log.clone());

        // The waits average 30s once capped at 60s, so about 25 starts are
        // expected in 10 minutes; a restart loop would make millions
        sleep(Duration::from_secs(600)).await;
        assert!(log.starts("bad") <= 100, "bad: {}", log.starts("bad"));
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_removed_camera_is_stopped_cleaned_up_and_can_come_back() {
        let (tx, rx) = watch::channel(config(&["a", "b"]));
        let log = Arc::new(Log::default());
        let task = supervise(rx, CancellationToken::new(), log.clone());
        sleep(Duration::from_secs(1)).await;

        tx.send_replace(config(&["b"]));
        sleep(Duration::from_secs(1)).await;
        assert_eq!(*log.stopped.lock().unwrap(), ["a"]);

        tx.send_replace(config(&["a", "b"]));
        sleep(Duration::from_secs(1)).await;
        assert_eq!(log.starts("a"), 2);
        assert_eq!(log.starts("b"), 1);
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_stops_every_camera() {
        let (_tx, rx) = watch::channel(config(&["a", "b"]));
        let log = Arc::new(Log::default());
        let cancel = CancellationToken::new();
        let task = supervise(rx, cancel.clone(), log.clone());
        sleep(Duration::from_secs(1)).await;

        cancel.cancel();
        task.await.unwrap().unwrap();
        let mut stopped = log.stopped.lock().unwrap().clone();
        stopped.sort();
        assert_eq!(stopped, ["a", "b"]);
    }

    #[tokio::test(start_paused = true)]
    async fn losing_the_config_is_an_error() {
        let (tx, rx) = watch::channel(config(&[]));
        let task = supervise(rx, CancellationToken::new(), Arc::new(Log::default()));
        drop(tx);
        assert!(task.await.unwrap().is_err());
    }
}
