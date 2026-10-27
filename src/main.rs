#![warn(unused_crate_dependencies)]
#![warn(missing_docs)]
#![warn(clippy::todo)]
//!
//! # Neolink
//!
//! Neolink is a small program that acts a general contol interface for Reolink IP cameras.
//!
//! It contains sub commands for running an rtsp proxy which can be used on Reolink cameras
//! that do not nativly support RTSP.
//!
//! This program is free software: you can redistribute it and/or modify it under the terms of the
//! GNU General Public License as published by the Free Software Foundation, either version 3 of
//! the License, or (at your option) any later version.
//!
//! This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
//! without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
//! the GNU General Public License for more details.
//!
//! You should have received a copy of the GNU General Public License along with this program. If
//! not, see <https://www.gnu.org/licenses/>.
//!
//! Neolink source code is available online at <https://github.com/QuantumEntangledAndy/neolink>
//!
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use env_logger::Env;
use log::*;
use std::{fs, future::Future, time::Duration};
use tokio_util::sync::CancellationToken;
use validator::Validate;

mod battery;
mod cmdline;
mod common;
mod config;
mod healthcheck;
#[cfg(feature = "gstreamer")]
mod image;
mod mqtt;
mod pir;
mod ptz;
mod reboot;
#[cfg(feature = "gstreamer")]
mod rtsp;
mod services;
mod statusled;
#[cfg(feature = "gstreamer")]
mod talk;
mod users;
mod utils;

use cmdline::{Command, Opt};
use common::NeoReactor;
use config::Config;

pub(crate) type AnyResult<T> = Result<T, anyhow::Error>;

fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Could not start the async runtime")?;
    let result = runtime.block_on(run());
    // A blocking task still running, like a DNS lookup that hangs, used to
    // keep neolink from exiting
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}

async fn run() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    // In the log, where they can be found with the rest, timestamped
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        log::error!("{info}");
        default_hook(info);
    }));

    let opt = Opt::parse();
    // Without a config, as it only reads heartbeats, and quietly, as it runs
    // every few seconds
    if let Some(Command::Healthcheck(opts)) = &opt.cmd {
        return healthcheck::main(opts);
    }

    info!(
        "Neolink {} {}",
        env!("NEOLINK_VERSION"),
        env!("NEOLINK_PROFILE")
    );

    let conf_path = opt.config.context("Must supply --config file")?;
    let config: Config = toml::from_str(
        &fs::read_to_string(&conf_path)
            .with_context(|| format!("Failed to read {:?}", conf_path))?,
    )
    .with_context(|| format!("Failed to parse the {:?} config file", conf_path))?;

    config
        .validate()
        .with_context(|| format!("Failed to validate the {:?} config file", conf_path))?;
    for ignored in config.ignored_options() {
        warn!("{ignored}");
    }
    mqtt::set_client_id(&conf_path, &config);

    let stop = stop_on_signal();
    let neo_reactor = NeoReactor::new(config.clone()).await;
    if serves(&opt.cmd) {
        healthcheck::clear();
        beat_while_alive(neo_reactor.clone());
    }

    let result = match opt.cmd {
        #[cfg(feature = "gstreamer")]
        None => {
            warn!(
                "Deprecated command line option. Please use: `neolink rtsp --config={:?}`",
                conf_path
            );
            rtsp::main(rtsp::Opt {}, neo_reactor.clone(), stop).await
        }
        #[cfg(not(feature = "gstreamer"))]
        None => {
            // When gstreamer is disabled the default command is MQTT
            warn!(
                "Deprecated command line option. Please use: `neolink mqtt --config={:?}`",
                conf_path
            );
            mqtt::main(mqtt::Opt {}, neo_reactor.clone(), stop).await
        }
        #[cfg(feature = "gstreamer")]
        Some(Command::Rtsp(opts)) => rtsp::main(opts, neo_reactor.clone(), stop).await,
        Some(Command::StatusLight(opts)) => {
            unless_stopped(&stop, statusled::main(opts, neo_reactor.clone())).await
        }
        Some(Command::Reboot(opts)) => {
            unless_stopped(&stop, reboot::main(opts, neo_reactor.clone())).await
        }
        Some(Command::Pir(opts)) => {
            unless_stopped(&stop, pir::main(opts, neo_reactor.clone())).await
        }
        Some(Command::Ptz(opts)) => {
            unless_stopped(&stop, ptz::main(opts, neo_reactor.clone())).await
        }
        #[cfg(feature = "gstreamer")]
        Some(Command::Talk(opts)) => {
            unless_stopped(&stop, talk::main(opts, neo_reactor.clone())).await
        }
        Some(Command::Mqtt(opts)) => mqtt::main(opts, neo_reactor.clone(), stop).await,
        #[cfg(feature = "gstreamer")]
        Some(Command::MqttRtsp(opts)) => {
            // Whichever ends first stops the other, and both are waited
            // for: the other used to be dropped wherever it was
            let both = stop.child_token();
            let (mqtt, rtsp) = tokio::join!(
                async {
                    let ended = mqtt::main(opts, neo_reactor.clone(), both.clone()).await;
                    both.cancel();
                    ended
                },
                async {
                    let ended = rtsp::main(rtsp::Opt {}, neo_reactor.clone(), both.clone()).await;
                    both.cancel();
                    ended
                },
            );
            mqtt.and(rtsp)
        }
        #[cfg(feature = "gstreamer")]
        Some(Command::Image(opts)) => {
            unless_stopped(&stop, image::main(opts, neo_reactor.clone())).await
        }
        Some(Command::Battery(opts)) => {
            unless_stopped(&stop, battery::main(opts, neo_reactor.clone())).await
        }
        Some(Command::Services(opts)) => {
            unless_stopped(&stop, services::main(opts, neo_reactor.clone())).await
        }
        Some(Command::Users(opts)) => {
            unless_stopped(&stop, users::main(opts, neo_reactor.clone())).await
        }
        Some(Command::Healthcheck(opts)) => healthcheck::main(&opts),
    };

    // Logged out of every camera, rather than leaving each to time its
    // session out
    neo_reactor.shutdown().await;
    result
}

/// Whether the command serves until it is stopped, rather than doing one
/// thing
fn serves(command: &Option<Command>) -> bool {
    match command {
        None | Some(Command::Mqtt(_)) => true,
        #[cfg(feature = "gstreamer")]
        Some(Command::Rtsp(_) | Command::MqttRtsp(_)) => true,
        _ => false,
    }
}

/// Beats for the healthcheck while the reactor answers: neither it nor the
/// async runtime froze
fn beat_while_alive(reactor: NeoReactor) {
    tokio::spawn(async move {
        let heartbeat = healthcheck::Heartbeat::new("neolink", "neolink");
        let mut ticks = tokio::time::interval(Duration::from_secs(1));
        loop {
            ticks.tick().await;
            if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(5), reactor.config()).await
            {
                heartbeat.beat();
            }
        }
    });
}

/// Runs a command that does its one thing and ends, unless neolink is asked
/// to stop first
async fn unless_stopped(
    stop: &CancellationToken,
    command: impl Future<Output = Result<()>>,
) -> Result<()> {
    tokio::select! {
        ended = command => ended,
        _ = stop.cancelled() => Err(anyhow!("Stopped before it was done")),
    }
}

/// Stops neolink cleanly on the first SIGINT or SIGTERM, and at once on the
/// second. SIGTERM used to kill neolink on the spot, before it logged out of
/// its cameras. And as the first process of a container, where signals
/// without a handler are ignored, neolink ignored SIGTERM until it was killed.
fn stop_on_signal() -> CancellationToken {
    let stop = CancellationToken::new();
    let signalled = stop.clone();
    tokio::spawn(async move {
        let mut signals = match Signals::new() {
            Ok(signals) => signals,
            Err(e) => {
                warn!("Could not listen for the signals to stop: {e}");
                return;
            }
        };
        signals.recv().await;
        info!("Stopping. Send the signal again to stop at once.");
        signalled.cancel();
        signals.recv().await;
        warn!("Stopping at once");
        std::process::exit(1);
    });
    stop
}

/// SIGINT and SIGTERM
#[cfg(unix)]
struct Signals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Signals {
    fn new() -> std::io::Result<Self> {
        use tokio::signal::unix::{signal, SignalKind};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    async fn recv(&mut self) {
        tokio::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
        }
    }
}

/// Ctrl+C
#[cfg(not(unix))]
struct Signals;

#[cfg(not(unix))]
impl Signals {
    fn new() -> std::io::Result<Self> {
        Ok(Self)
    }

    async fn recv(&mut self) {
        let _ = tokio::signal::ctrl_c().await;
    }
}
