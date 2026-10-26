//!
//! # Neolink Healthcheck
//!
//! `neolink healthcheck` tells whether a running neolink still works, for a
//! container's HEALTHCHECK or a liveness probe. It exits with 0 if it does,
//! and 1 if part of it stopped answering, which restarting neolink fixes.
//!
//! While neolink serves, each part of it refreshes a heartbeat file every
//! second, as long as it answers: neolink itself, the RTSP server, and each
//! camera. This only reads them. A heartbeat older than the timeout is a part
//! that froze.
//!
//! A camera that is away, asleep or paused still answers, so it is healthy:
//! restarting neolink would bring none of its video back. Its stream, should
//! it stop, is restarted by neolink itself.
//!
//! # Usage
//!
//! ```bash
//! neolink healthcheck [--timeout=10] [CameraName]
//! ```
//!
//! It needs no config. The heartbeats are in `neolink-health` in the system's
//! temporary directory, or in `NEOLINK_HEALTH_DIR`.
//!
use anyhow::{anyhow, Result};
use std::time::Duration;

mod cmdline;
mod heartbeat;

pub(crate) use cmdline::Opt;
pub(crate) use heartbeat::{camera_id, clear, Heartbeat};

/// Entry point for the healthcheck subcommand
///
/// Opt is the command line options
pub(crate) fn main(opt: &Opt) -> Result<()> {
    // No heartbeats: nothing that serves runs, so nothing froze
    let beats = heartbeat::read(&heartbeat::dir());
    let camera = opt.camera.as_deref().map(camera_id);
    let (frozen, seen_camera) =
        frozen_parts(&beats, Duration::from_secs(opt.timeout), camera.as_deref());
    if let (Some(name), false) = (&opt.camera, seen_camera) {
        println!("No heartbeat from camera {name}: it does not run");
    }
    if frozen.is_empty() {
        println!("Healthy");
        Ok(())
    } else {
        for part in &frozen {
            println!("{part}");
        }
        Err(anyhow!("Unhealthy"))
    }
}

/// The parts that froze among `beats`: neolink's own, and those of `camera`
/// or of every camera. With whether `camera` beats at all.
fn frozen_parts(
    beats: &[(String, String, Duration)],
    timeout: Duration,
    camera: Option<&str>,
) -> (Vec<String>, bool) {
    let mut frozen = vec![];
    let mut seen_camera = false;
    for (id, what, age) in beats {
        if id.starts_with(heartbeat::CAMERA) {
            match camera {
                Some(camera) if camera != id => continue,
                Some(_) => seen_camera = true,
                None => {}
            }
        }
        if *age > timeout {
            frozen.push(format!("{what}: no sign of life for {}s", age.as_secs()));
        }
    }
    (frozen, seen_camera)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn beat(id: &str, what: &str, age: u64) -> (String, String, Duration) {
        (id.to_string(), what.to_string(), Duration::from_secs(age))
    }

    #[test]
    fn a_part_that_did_not_beat_within_the_timeout_froze() {
        let beats = [
            beat("neolink", "neolink", 1),
            beat("rtsp", "the RTSP server", 30),
            beat("camera-a", "camera a", 2),
            beat("camera-b", "camera b", 30),
        ];
        let timeout = Duration::from_secs(10);
        let (frozen, _) = frozen_parts(&beats, timeout, None);
        assert_eq!(
            frozen,
            [
                "the RTSP server: no sign of life for 30s",
                "camera b: no sign of life for 30s"
            ]
        );

        let (frozen, seen) = frozen_parts(&beats, timeout, Some("camera-a"));
        assert_eq!(frozen, ["the RTSP server: no sign of life for 30s"]);
        assert!(seen);

        let (frozen, seen) = frozen_parts(&beats, timeout, Some("camera-c"));
        assert_eq!(frozen, ["the RTSP server: no sign of life for 30s"]);
        assert!(!seen, "a camera that does not run");
    }
}
