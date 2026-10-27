use std::sync::{Arc, Weak};
use tokio::{
    sync::watch::{Receiver as WatchReceiver, Sender as WatchSender},
    time::{interval, sleep, timeout, Duration, Instant},
};
use tokio_util::sync::CancellationToken;

use super::Backoff;
use crate::{config::CameraConfig, utils::connect_and_login, AnyResult};
use neolink_core::bc_protocol::BcCamera;

#[derive(Eq, PartialEq, Copy, Clone)]
pub(crate) enum NeoCamThreadState {
    Connected,
    Disconnected,
}

pub(crate) struct NeoCamThread {
    state: WatchReceiver<NeoCamThreadState>,
    config: WatchReceiver<CameraConfig>,
    /// Counts the requests to reconnect
    reconnect: WatchReceiver<u64>,
    cancel: CancellationToken,
    camera_watch: WatchSender<Weak<BcCamera>>,
}

impl NeoCamThread {
    pub(crate) async fn new(
        watch_state_rx: WatchReceiver<NeoCamThreadState>,
        watch_config_rx: WatchReceiver<CameraConfig>,
        reconnect_rx: WatchReceiver<u64>,
        camera_watch_tx: WatchSender<Weak<BcCamera>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            state: watch_state_rx,
            config: watch_config_rx,
            reconnect: reconnect_rx,
            cancel,
            camera_watch: camera_watch_tx,
        }
    }
    async fn run_camera(&mut self, config: &CameraConfig) -> AnyResult<()> {
        let name = config.name.clone();
        log::trace!("Attempting connection with config: {config:?}");
        let camera = Arc::new(connect_and_login(config).await?);
        log::trace!("  - Connected");

        let cancel_check = self.cancel.clone();
        // Stopping goes on to log out at once, rather than after the set up,
        // which neolink may not live to see
        tokio::select! {
            _ = cancel_check.cancelled() => {}
            _ = async {
                sleep(Duration::from_secs(2)).await; // Delay a little since some calls will error if camera is waking up
                if let Err(e) = update_camera_time(&camera, &name, config.update_time).await {
                    log::warn!("Could not set camera time, (perhaps missing on this camera of your login in not an admin): {e:?}");
                }
                sleep(Duration::from_secs(2)).await; // Delay a little since some calls will error if camera is waking up
            } => {}
        }

        self.camera_watch.send_replace(Arc::downgrade(&camera));

        // Now we wait for a disconnect
        tokio::select! {
            _ = cancel_check.cancelled() => {
                AnyResult::Ok(())
            }
            v = camera.join() => {
                v?;
                Ok(())
            },
            v = async {
                let mut interval = interval(Duration::from_secs(5));
                let mut missed_pings = 0;
                loop {
                    interval.tick().await;
                    log::trace!("Sending ping");
                    match timeout(Duration::from_secs(5), camera.get_linktype()).await {
                        Ok(Ok(_)) => {
                            log::trace!("Ping reply");
                            missed_pings = 0;
                            continue
                        },
                        // Any reply shows that the camera is there. Pinging
                        // used to stop for good on a reply it could not
                        // understand, and a refused ping dropped the
                        // connection, every 5s.
                        Ok(Err(e @ (neolink_core::Error::UnintelligibleReply { .. }
                        | neolink_core::Error::CameraServiceUnavailable { .. }))) => {
                            log::trace!("Ping answered without a link type: {e}");
                            missed_pings = 0;
                            continue
                        },
                        Ok(Err(e)) => {
                            break Err(e.into());
                        },
                        Err(_) => {
                            // Timeout
                            if missed_pings < 5 {
                                missed_pings += 1;
                                continue;
                            } else {
                                log::error!("Timed out waiting for camera ping reply");
                                break Err(anyhow::anyhow!("Timed out waiting for camera ping reply"));
                            }
                        }
                    }
                }
            } => v,
        }?;

        let _ = camera.logout().await;
        let _ = camera.shutdown().await;

        Ok(())
    }

    // Will run and attempt to maintain the connection
    //
    // A watch sender is used to send the new camera
    // whenever it changes
    pub(crate) async fn run(&mut self) -> AnyResult<()> {
        /// The first wait before connecting again. They then double, with
        /// jitter, up to the camera's `max_reconnect_backoff`: cameras that
        /// went away together used to be retried together, every 5s at most.
        const MIN_BACKOFF: Duration = Duration::from_secs(1);
        /// A connection that lasted this long worked
        const HEALTHY: Duration = Duration::from_secs(60);
        /// How often a refused login is tried again. A refused login used to
        /// stop the camera for good, until neolink restarted. More often could
        /// get the account locked.
        const LOGIN_RETRY: Duration = Duration::from_secs(5 * 60);

        let cancel = self.cancel.clone();
        let mut backoff = Backoff::new(MIN_BACKOFF, MIN_BACKOFF);
        let mut max_backoff = None;

        loop {
            let mut state = self.state.clone();
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                v = state.wait_for(|state| matches!(state, NeoCamThreadState::Connected)) => {
                    v?;
                }
            }
            let mut config_rec = self.config.clone();

            let config = config_rec.borrow_and_update().clone();
            let now = Instant::now();
            let name = config.name.clone();
            let max = Duration::from_secs(config.max_reconnect_backoff);
            if max_backoff != Some(max) {
                backoff = Backoff::new(MIN_BACKOFF, max);
                max_backoff = Some(max);
            }

            let mut state = self.state.clone();
            // Only the requests from now on
            let mut reconnect = self.reconnect.clone();
            let requests = *reconnect.borrow();

            let res = tokio::select! {
                Ok(_) = config_rec.changed() => {
                    None
                }
                Ok(_) = reconnect.wait_for(|now| *now != requests) => {
                    log::info!("{name}: Reconnecting");
                    None
                }
                Ok(_) = state.wait_for(|state| matches!(state, NeoCamThreadState::Disconnected)) => {
                    log::trace!("State changed to disconnect");
                    None
                }
                v = self.run_camera(&config) => {
                    Some(v)
                }
            };
            self.camera_watch.send_replace(Weak::new());

            let Some(result) = res else {
                // Go back and reload NOW
                //
                // This occurs if there was a config change
                log::trace!("Config change or Manual disconnect");
                continue;
            };

            if now.elapsed() >= HEALTHY {
                backoff.reset();
            }

            let delay = match result {
                Ok(()) => {
                    // Normal shutdown
                    log::trace!("Normal camera shutdown");
                    self.cancel.cancel();
                    return Ok(());
                }
                Err(e) => match e.downcast_ref::<neolink_core::Error>() {
                    Some(neolink_core::Error::CameraLoginFail { code }) => {
                        log::error!(
                            "{name}: The camera refused the login with code {code}: check the username and password. Trying again in {} minutes",
                            LOGIN_RETRY.as_secs() / 60
                        );
                        LOGIN_RETRY
                    }
                    _ => {
                        let delay = backoff.next_delay();
                        log::warn!("{name}: Connection Lost: {:?}", e);
                        log::info!("{name}: Attempt reconnect in {delay:.1?}");
                        delay
                    }
                },
            };
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = sleep(delay) => {}
                // A new config may well fix it
                Ok(_) = config_rec.changed() => {}
            }
        }
    }
}

impl Drop for NeoCamThread {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

async fn update_camera_time(camera: &BcCamera, name: &str, update_time: bool) -> AnyResult<()> {
    let cam_time = camera.get_time().await?;
    let mut update = false;
    if let Some(time) = cam_time {
        log::info!("{}: Camera time is already set: {}", name, time);
        if update_time {
            update = true;
        }
    } else {
        update = true;
        log::warn!("{}: Camera has no time set, Updating", name);
    }
    if update {
        // In the camera's time zone: setting the time used to reset it to UTC
        // (#361)
        let offset = cam_time.map_or(time::UtcOffset::UTC, |time| time.offset());
        let new_time = time::OffsetDateTime::now_utc().to_offset(offset);

        log::info!("{}: Setting time to {}", name, new_time);
        match camera.set_time(new_time).await {
            Ok(_) => {
                let cam_time = camera.get_time().await?;
                if let Some(time) = cam_time {
                    log::info!("{}: Camera time is now set: {}", name, time);
                }
            }
            Err(e) => {
                log::error!(
                    "{}: Camera did not accept new time (is user an admin?): Error: {:?}",
                    name,
                    e
                );
            }
        }
    }
    Ok(())
}
