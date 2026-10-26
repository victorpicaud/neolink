//! Used to track number of users of a service
use std::sync::Arc;
use tokio::sync::watch::{channel as watch, Sender as WatchSender};

use crate::Result;

/// Counts the active users of the stream
///
/// The count changes synchronously, so a [`Permit`] is released wherever it
/// is dropped, even where no tokio runtime is running. A release that went
/// missing would keep the camera from ever going idle.
pub(crate) struct UseCounter {
    value: Arc<WatchSender<u32>>,
}

impl UseCounter {
    pub(crate) async fn new() -> Self {
        let (value, _) = watch(0);
        Self {
            value: Arc::new(value),
        }
    }

    pub(crate) async fn create_activated(&self) -> Result<Permit> {
        let mut res = Permit::new(self);
        res.activate().await?;
        Ok(res)
    }

    #[cfg(feature = "gstreamer")]
    #[allow(dead_code)]
    pub(crate) async fn create_deactivated(&self) -> Result<Permit> {
        Ok(Permit::new(self))
    }
}

pub(crate) struct Permit {
    is_active: bool,
    value: Arc<WatchSender<u32>>,
}

impl Permit {
    #[cfg(feature = "gstreamer")]
    #[allow(dead_code)]
    pub(crate) fn subscribe(&self) -> Self {
        Self {
            is_active: false,
            value: self.value.clone(),
        }
    }

    fn new(source: &UseCounter) -> Self {
        Self {
            is_active: false,
            value: source.value.clone(),
        }
    }

    pub(crate) async fn activate(&mut self) -> Result<()> {
        self.set_active(true);
        Ok(())
    }

    pub(crate) async fn deactivate(&mut self) -> Result<()> {
        self.set_active(false);
        Ok(())
    }

    fn set_active(&mut self, active: bool) {
        if self.is_active == active {
            return;
        }
        self.is_active = active;
        self.value.send_modify(|value| {
            let old = *value;
            *value = if active {
                old.saturating_add(1)
            } else {
                old.saturating_sub(1)
            };
            log::trace!("Usecounter: {}->{}", old, *value);
        });
    }

    pub(crate) async fn aquired_users(&self) -> Result<()> {
        self.value
            .subscribe()
            .wait_for(|curr| {
                log::trace!("aquired_users: {}", *curr);
                *curr > 0
            })
            .await?;
        Ok(())
    }

    pub(crate) async fn dropped_users(&self) -> Result<()> {
        self.value
            .subscribe()
            .wait_for(|curr| {
                log::trace!("dropped_users: {}", *curr);
                *curr == 0
            })
            .await?;
        Ok(())
    }

    #[cfg(feature = "gstreamer")]
    #[allow(dead_code)]
    pub(crate) fn get_counter(&self) -> tokio::sync::watch::Receiver<u32> {
        self.value.subscribe()
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.set_active(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_count_follows_the_active_permits() -> Result<()> {
        let counter = UseCounter::new().await;
        let count = counter.value.subscribe();
        let mut first = counter.create_activated().await?;
        let second = counter.create_activated().await?;
        assert_eq!(*count.borrow(), 2);

        first.activate().await?;
        assert_eq!(*count.borrow(), 2, "activating twice counts once");
        first.deactivate().await?;
        drop(first);
        assert_eq!(*count.borrow(), 1, "an inactive permit releases nothing");
        drop(second);
        assert_eq!(*count.borrow(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn a_permit_dropped_without_a_runtime_is_still_released() -> Result<()> {
        let counter = UseCounter::new().await;
        let count = counter.value.subscribe();
        let permit = counter.create_activated().await?;

        // A plain thread has no tokio runtime: this used to panic there and
        // leave the count at 1 for good
        std::thread::spawn(move || drop(permit)).join().unwrap();
        assert_eq!(*count.borrow(), 0);
        Ok(())
    }
}
