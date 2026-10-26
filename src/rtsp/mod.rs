///
/// # Neolink RTSP
///
/// This module serves the rtsp streams for the
/// `neolink rtsp` subcommand
///
/// All camera specified in the config.toml will be served
/// over rtsp. By default it bind to all local ip addresses
/// on the port 8554.
///
/// You can view the streams with any rtsp compliement program
/// such as ffmpeg, vlc, blue-iris, home-assistant, zone-minder etc.
///
/// Each camera has it own endpoint based on its name. For example
/// a camera named `"Garage"` in the config can be found at.
///
/// `rtsp://my.ip.address:8554/Garage`
///
/// With the lower resolution stream at
///
/// `rtsp://my.ip.address:8554/Garage/subStream`
///
/// # Usage
///
/// To start the subcommand use the following in a shell.
///
/// ```bash
/// neolink rtsp --config=config.toml
/// ```
///
/// # Example Config
///
/// ```toml
// [[cameras]]
// name = "Cammy"
// username = "****"
// password = "****"
// address = "****:9000"
//   [cameras.pause]
//   on_motion = false
//   on_client = false
//   mode = "none"
//   timeout = 1.0
// ```
//
// - When `on_motion` is true the camera will pause streaming when motion is stopped and resume it when motion is started
// - When `on_client` is true the camera will pause while there is no client connected.
// - `timeout` handels how long to wait after motion stops before pausing the stream
// - `mode` has the following values:
//   - `"black"`: Switches to a black screen. Requires more cpu as the stream is fully reencoded
//   - `"still"`: Switches to a still image. Requires more cpu as the stream is fully reencoded
//   - `"test"`: Switches to the gstreamer test image. Requires more cpu as the stream is fully reencoded
//   - `"none"`: Resends the last iframe the camera. This does not reencode at all.  **Most use cases should use this one as it has the least effort on the cpu and gives what you would expect**
//
use anyhow::{anyhow, Context, Result};
use gstreamer_rtsp_server::prelude::*;
use log::*;
use neolink_core::bc_protocol::StreamKind;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::{
    sync::watch::{channel as watch, Sender as WatchSender},
    task::JoinSet,
    time::{sleep, timeout, Duration},
};
use tokio_util::sync::CancellationToken;

mod cmdline;
mod factory;
mod gst;
mod stream;

use crate::common::{supervise_cameras, Backoff, NeoInstance, NeoReactor};
use factory::*;
use stream::*;

use super::config::UserConfig;
pub(crate) use cmdline::Opt;
use gst::NeoRtspServer;

type AnyResult<T> = anyhow::Result<T, anyhow::Error>;

/// How often the streams a camera offers are checked again, besides whenever
/// it connects. They only change with its settings, and a battery camera
/// should be left to sleep.
const STREAM_INFO_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Entry point for the rtsp subcommand
///
/// Opt is the command line options. It serves until `stop` is cancelled, or
/// fails.
pub(crate) async fn main(_opt: Opt, reactor: NeoReactor, stop: CancellationToken) -> Result<()> {
    let rtsp = Arc::new(NeoRtspServer::new()?);

    let global_cancel = CancellationToken::new();

    let mut set = JoinSet::new();

    // Thread for the TLS from the config
    let mut thread_config = reactor.config().await?;
    let thread_cancel = global_cancel.clone();
    let thread_rtsp = rtsp.clone();
    thread_rtsp.set_up_tls(&thread_config.borrow_and_update().clone())?;
    set.spawn(async move {
        tokio::select! {
            _ = thread_cancel.cancelled() => AnyResult::Ok(()),
            v = async {
                loop {
                    thread_config.changed().await?;
                    if let Err(e) = thread_rtsp.set_up_tls(&thread_config.borrow().clone()) {
                        log::error!("Could not setup TLS: {e}");
                    }
                }
            } => v
        }
    });

    // Thread for the Users from the config
    let mut thread_config = reactor.config().await?;
    let thread_cancel = global_cancel.clone();
    let thread_rtsp = rtsp.clone();
    set.spawn(async move {
        tokio::select! {
            _ = thread_cancel.cancelled() => AnyResult::Ok(()),
            v = async {
                let mut curr_users = HashSet::new();
                loop {

                    curr_users = thread_config.wait_for(|new_config|
                        new_config.users.iter().cloned().collect::<HashSet<_>>() != curr_users
                    ).await?.users.iter().cloned().collect::<HashSet<_>>();

                    let config = thread_config.borrow().clone();
                    if let Err(e) = apply_users(&thread_rtsp, &curr_users).await {
                        log::error!("Could not setup TLS: {e}");
                    }

                    if config.certificate.is_none() && !curr_users.is_empty() {
                        warn!(
                            "Without a server certificate, usernames and passwords will be exchanged in plaintext!"
                        )
                    }
                }
            } => v
        }
    });

    // Start and stop each camera's streams as cameras are enabled and
    // disabled, and restart them if they fail
    let thread_reactor = reactor.clone();
    let thread_rtsp = rtsp.clone();
    let unmount_rtsp = rtsp.clone();
    set.spawn(supervise_cameras(
        "RTSP",
        reactor.config().await?,
        global_cancel.clone(),
        move |name| {
            let reactor = thread_reactor.clone();
            let rtsp = thread_rtsp.clone();
            async move { camera_main(reactor.get(&name).await?, &rtsp).await }
        },
        move |name: &str| unmount_camera(&unmount_rtsp, name),
    ));

    let rtsp_config = reactor.config().await?.borrow().clone();
    info!(
        "Starting RTSP Server at {}:{}",
        rtsp_config.bind_addr, rtsp_config.bind_port,
    );

    let bind_addr = rtsp_config.bind_addr.clone();
    let bind_port = rtsp_config.bind_port;
    let _running = rtsp.run(&bind_addr, bind_port).await?;
    let thread_rtsp = rtsp.clone();
    set.spawn(async move { thread_rtsp.join().await });

    // None of these tasks ends on its own, so whichever ends first stops the
    // others, and neolink exits with its error. Being asked to stop is no
    // error.
    let first = tokio::select! {
        _ = stop.cancelled() => None,
        ended = set.join_next() => Some(match ended {
            Some(Ok(ended)) => ended,
            Some(Err(e)) => Err(e.into()),
            None => Ok(()),
        }),
    };
    global_cancel.cancel();
    rtsp.quit();
    let stopping = async { while set.join_next().await.is_some() {} };
    if timeout(Duration::from_secs(5), stopping).await.is_err() {
        warn!("The RTSP server took over 5s to stop");
    }
    match first {
        None => Ok(()),
        Some(first) => Err(first
            .err()
            .unwrap_or_else(|| anyhow!("The RTSP server stopped"))),
    }
}

/// This keeps the users in rtsp and the config in sync
async fn apply_users(rtsp: &NeoRtspServer, curr_users: &HashSet<UserConfig>) -> AnyResult<()> {
    // Add those missing
    for user in curr_users.iter() {
        log::debug!("Adding user {} to rtsp server", user.name);
        rtsp.add_user(&user.name, &user.pass.clone().unwrap_or_default())
            .await?;
    }
    // Remove unused
    let rtsp_users = rtsp.get_users().await?;
    for user in rtsp_users {
        if !curr_users.iter().any(|a| a.name == user) {
            log::debug!("Removing user {} from rtsp server", user);
            rtsp.remove_user(&user).await?;
        }
    }
    Ok(())
}

/// Top level camera entry point
///
/// It checks which streams are supported and then starts them
async fn camera_main(camera: NeoInstance, rtsp: &NeoRtspServer) -> Result<()> {
    let name = camera.config().await?.borrow().name.clone();
    log::debug!("{name}: Camera Main");
    let (supported_streams_tx, supported_streams) = watch(HashSet::<StreamKind>::new());

    let mut set = JoinSet::new();
    set.spawn(poll_supported_streams(camera.clone(), supported_streams_tx));

    let mut camera_config = camera.config().await?.clone();
    loop {
        let prev_stream_config = camera_config.borrow_and_update().stream;
        let prev_stream_users = camera_config.borrow().permitted_users.clone();
        let active_streams = prev_stream_config
            .as_stream_kinds()
            .drain(..)
            .collect::<HashSet<_>>();
        let use_splash = camera_config.borrow().use_splash;
        let splash_pattern = camera_config.borrow().splash_pattern.to_string();

        // This select is for changes to camera_config.stream
        break tokio::select! {
            v = camera_config.wait_for(|config| config.stream != prev_stream_config || config.permitted_users != prev_stream_users || config.use_splash != use_splash) => {
                if let Err(e) = v {
                    AnyResult::Err(e.into())
                } else {
                    // config.stream changed restart
                    continue;
                }
            },
            v = async {
                // This select handles enabling the right stream
                // and setting up the users
                let all_users = rtsp.get_users().await?.iter().filter(|a| *a != "anyone" && *a != "anonymous").cloned().collect::<HashSet<_>>();
                let permitted_users: HashSet<String> = match &prev_stream_users {
                    // If in the camera config there is the user "anyone", or if none is specified but users
                    // are defined at all, then we add all users to the camera's allowed list.
                    Some(p) if p.iter().any(|u| u == "anyone") => all_users,
                    None if !all_users.is_empty() => all_users,

                    // The user specified permitted_users
                    Some(p) => p.iter().cloned().collect(),

                    // The user didn't specify permitted_users, and there are none defined anyway
                    None => ["anonymous".to_string()].iter().cloned().collect(),
                };

                // Create the dummy factory
                let dummy_factory = make_dummy_factory(use_splash, splash_pattern);
                dummy_factory.add_permitted_roles(&permitted_users);
                let mut supported_streams_1 = supported_streams.clone();
                let mut supported_streams_2 = supported_streams.clone();
                let mut supported_streams_3 = supported_streams.clone();
                tokio::select! {
                    v = async {
                        let mut paths = stream_paths(&name, StreamKind::Main);
                        paths.push(
                            format!("/{name}")
                        );
                        // Create a dummy factory so that the URL will not return 404 while waiting
                        // for configuration to compete
                        //
                        // This is for BI since it will give up forever on a 404 rather then retry
                        //
                        let mounts = rtsp
                            .mount_points()
                            .ok_or(anyhow!("RTSP server lacks mount point"))?;
                        for path in paths.iter() {
                            log::debug!("Path: {}", path);
                            mounts.add_factory(path, dummy_factory.clone());
                        }
                        log::debug!("{}: Preparing at {}", name, paths.join(", "));

                        supported_streams_1.wait_for(|ss| ss.contains(&StreamKind::Main)).await?;
                        stream_main(camera.clone(), StreamKind::Main, rtsp, &permitted_users, &paths).await
                    }, if active_streams.contains(&StreamKind::Main) => v,
                    v = async {
                        let mut paths = stream_paths(&name, StreamKind::Sub);
                        if ! active_streams.contains(&StreamKind::Main) {
                            paths.push(
                                format!("/{name}")
                            );
                        }

                        // Create a dummy factory so that the URL will not return 404 while waiting
                        // for configuration to compete
                        //
                        // This is for BI since it will give up forever on a 404 rather then retry
                        //
                        let mounts = rtsp
                            .mount_points()
                            .ok_or(anyhow!("RTSP server lacks mount point"))?;
                        // Create the dummy factory
                        for path in paths.iter() {
                            log::debug!("Path: {}", path);
                            mounts.add_factory(path, dummy_factory.clone());
                        }
                        log::debug!("{}: Preparing at {}", name, paths.join(", "));

                        supported_streams_2.wait_for(|ss| ss.contains(&StreamKind::Sub)).await?;

                        stream_main(camera.clone(), StreamKind::Sub, rtsp, &permitted_users, &paths).await
                    }, if active_streams.contains(&StreamKind::Sub) => v,
                    v = async {
                        let mut paths = stream_paths(&name, StreamKind::Extern);
                        if ! active_streams.contains(&StreamKind::Main) && ! active_streams.contains(&StreamKind::Sub) {
                            paths.push(
                                format!("/{name}")
                            );
                        }

                        // Create a dummy factory so that the URL will not return 404 while waiting
                        // for configuration to compete
                        //
                        // This is for BI since it will give up forever on a 404 rather then retry
                        //
                        let mounts = rtsp
                            .mount_points()
                            .ok_or(anyhow!("RTSP server lacks mount point"))?;
                        for path in paths.iter() {
                            log::debug!("Path: {}", path);
                            mounts.add_factory(path, dummy_factory.clone());
                        }
                        log::debug!("{}: Preparing at {}", name, paths.join(", "));

                        supported_streams_3.wait_for(|ss| ss.contains(&StreamKind::Extern)).await?;
                        stream_main(camera.clone(), StreamKind::Extern, rtsp, &permitted_users, &paths).await
                    }, if active_streams.contains(&StreamKind::Extern) => v,
                    else => {
                        // all disabled just wait here until config is changed
                        futures::future::pending().await
                    }
                }
            } => v,
        };
    }?;

    Ok(())
}

/// The paths a stream is served at. The camera's own path, without a stream,
/// also serves the first of its main, sub and extern streams that is enabled.
fn stream_paths(name: &str, stream: StreamKind) -> Vec<String> {
    let (lower, upper) = match stream {
        StreamKind::Main => ("main", "Main"),
        StreamKind::Sub => ("sub", "Sub"),
        StreamKind::Extern => ("extern", "Extern"),
    };
    vec![
        format!("/{name}/{lower}"),
        format!("/{name}/{upper}"),
        format!("/{name}/{lower}Stream"),
        format!("/{name}/{upper}Stream"),
        format!("/{name}/{upper}stream"),
        format!("/{name}/{lower}stream"),
    ]
}

/// Stops serving a camera that was disabled or removed
fn unmount_camera(rtsp: &NeoRtspServer, name: &str) {
    let Some(mounts) = rtsp.mount_points() else {
        return;
    };
    for stream in [StreamKind::Main, StreamKind::Sub, StreamKind::Extern] {
        for path in stream_paths(name, stream) {
            mounts.remove_factory(&path);
        }
    }
    mounts.remove_factory(&format!("/{name}"));
}

/// Keeps `supported` up to date with the streams the camera offers. They are
/// checked whenever the camera connects and every few minutes, and retried
/// with backoff after an error. Returns only if the camera is gone.
async fn poll_supported_streams(
    camera: NeoInstance,
    supported: WatchSender<HashSet<StreamKind>>,
) -> AnyResult<()> {
    let name = camera.config().await?.borrow().name.clone();
    let mut connection = camera.camera();
    let mut backoff = Backoff::new(Duration::from_secs(1), STREAM_INFO_INTERVAL);
    loop {
        let stream_info = camera
            .run_passive_task(|cam| Box::pin(async move { Ok(cam.get_stream_info().await?) }))
            .await;
        let wait = match stream_info {
            Ok(stream_info) => {
                let streams = stream_info
                    .stream_infos
                    .iter()
                    .flat_map(|stream_info| stream_info.encode_tables.clone())
                    .flat_map(|encode| match encode.name.as_str() {
                        "mainStream" => Some(StreamKind::Main),
                        "subStream" => Some(StreamKind::Sub),
                        "externStream" => Some(StreamKind::Extern),
                        new_stream_name => {
                            log::debug!("New stream name {}", new_stream_name);
                            None
                        }
                    })
                    .collect::<HashSet<_>>();
                supported.send_if_modified(|old| {
                    if *old != streams {
                        *old = streams;
                        true
                    } else {
                        false
                    }
                });
                backoff.reset();
                STREAM_INFO_INTERVAL
            }
            Err(e) => {
                let delay = backoff.next_delay();
                log::warn!(
                    "{name}: Could not learn the camera's streams, retrying in {delay:.1?}: {e:#}"
                );
                delay
            }
        };
        tokio::select! {
            _ = sleep(wait) => {}
            changed = connection.changed() => {
                changed.context("The camera is gone")?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_stream_keeps_its_six_paths() {
        assert_eq!(
            stream_paths("cam", StreamKind::Main),
            [
                "/cam/main",
                "/cam/Main",
                "/cam/mainStream",
                "/cam/MainStream",
                "/cam/Mainstream",
                "/cam/mainstream"
            ]
        );
        assert_eq!(stream_paths("cam", StreamKind::Sub)[5], "/cam/substream");
        assert_eq!(
            stream_paths("cam", StreamKind::Extern)[3],
            "/cam/ExternStream"
        );
    }
}
