//! Heartbeat files, one for each part of neolink that should keep answering
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

/// How the id of a camera's heartbeat starts
pub(super) const CAMERA: &str = "camera-";

/// Where the heartbeats are: `NEOLINK_HEALTH_DIR`, or `neolink-health` in the
/// system's temporary directory
pub(super) fn dir() -> PathBuf {
    if cfg!(test) {
        // Away from those of a neolink that runs
        return std::env::temp_dir().join(format!("neolink-health-test-{}", std::process::id()));
    }
    std::env::var_os("NEOLINK_HEALTH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("neolink-health"))
}

/// `id` as a file name: what one cannot hold, such as `/`, is escaped
fn file_name(id: &str) -> String {
    let mut name = String::new();
    for byte in id.bytes() {
        if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' {
            name.push(byte as char);
        } else {
            name.push_str(&format!("%{byte:02X}"));
        }
    }
    name
}

/// The file name of a camera's heartbeat
pub(crate) fn camera_id(camera: &str) -> String {
    file_name(&format!("{CAMERA}{camera}"))
}

/// Removes the heartbeats an earlier run left, which would look frozen
pub(crate) fn clear() {
    if let Ok(entries) = fs::read_dir(dir()) {
        for entry in entries.flatten() {
            let _ = fs::remove_file(entry.path());
        }
    }
}

/// Every heartbeat in `dir`: its file name, what beats, and how long ago it
/// last did
pub(super) fn read(dir: &Path) -> Vec<(String, String, Duration)> {
    let Ok(entries) = fs::read_dir(dir) else {
        return vec![];
    };
    let now = SystemTime::now();
    entries
        .flatten()
        .filter_map(|entry| {
            // Gone meanwhile, as its part stopped
            let what = fs::read_to_string(entry.path()).ok()?;
            let beat = entry.metadata().ok()?.modified().ok()?;
            Some((
                entry.file_name().to_string_lossy().into_owned(),
                what,
                now.duration_since(beat).unwrap_or_default(),
            ))
        })
        .collect()
}

/// The heartbeat file of a part of neolink, which `beat` refreshes. It is
/// removed with the part.
pub(crate) struct Heartbeat {
    path: PathBuf,
    what: String,
}

impl Heartbeat {
    /// The heartbeat of `what`, as the healthcheck names it, in the file
    /// named after `id`
    pub(crate) fn new(id: &str, what: &str) -> Self {
        Self::in_dir(&dir(), id, what)
    }

    fn in_dir(dir: &Path, id: &str, what: &str) -> Self {
        Self {
            path: dir.join(file_name(id)),
            what: what.to_string(),
        }
    }

    /// Shows that the part is alive
    pub(crate) fn beat(&self) {
        if let Some(dir) = self.path.parent() {
            let _ = fs::create_dir_all(dir);
        }
        if let Err(e) = fs::write(&self.path, &self.what) {
            log::debug!("Could not write the heartbeat {:?}: {e}", self.path);
        }
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_heartbeat_tells_how_long_ago_it_beat_until_its_part_is_gone() {
        let dir = dir().join("beats");
        let heartbeat = Heartbeat::in_dir(&dir, "neolink", "neolink");
        assert!(read(&dir).is_empty(), "before the first beat");

        heartbeat.beat();
        let beats = read(&dir);
        assert_eq!(beats.len(), 1);
        let (id, what, age) = &beats[0];
        assert_eq!((id.as_str(), what.as_str()), ("neolink", "neolink"));
        assert!(*age < Duration::from_secs(5), "{age:?}");

        drop(heartbeat);
        assert!(read(&dir).is_empty(), "after its part is gone");
    }

    #[test]
    fn any_camera_name_makes_a_file_name() {
        assert_eq!(camera_id("front_door-2"), "camera-front_door-2");
        assert_eq!(camera_id("front door/2"), "camera-front%20door%2F2");
        assert_eq!(camera_id("..").len(), "camera-%2E%2E".len());
    }
}
