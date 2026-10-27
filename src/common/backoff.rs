//! Exponential backoff with full jitter
use rand::Rng;
use std::time::Duration;

/// The waits between retries. Their cap starts at `base` and doubles up to
/// `max`, and each wait is random between zero and the cap ("full jitter") so
/// that tasks which failed together do not all retry at the same moment.
pub(crate) struct Backoff {
    base: Duration,
    max: Duration,
    attempt: u32,
}

impl Backoff {
    pub(crate) fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            attempt: 0,
        }
    }

    /// How long to wait before the next retry
    pub(crate) fn next_delay(&mut self) -> Duration {
        let cap = self.cap();
        self.attempt = self.attempt.saturating_add(1);
        cap.mul_f64(rand::thread_rng().gen())
    }

    /// Starts again from `base`, after a success
    pub(crate) fn reset(&mut self) {
        self.attempt = 0;
    }

    fn cap(&self) -> Duration {
        let factor = 1u32.checked_shl(self.attempt).unwrap_or(u32::MAX);
        self.base.saturating_mul(factor).min(self.max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backoff() -> Backoff {
        Backoff::new(Duration::from_secs(1), Duration::from_secs(60))
    }

    #[test]
    fn delays_stay_under_a_cap_that_doubles_up_to_the_maximum() {
        let mut backoff = backoff();
        for cap in [1, 2, 4, 8, 16, 32, 60, 60] {
            assert_eq!(backoff.cap(), Duration::from_secs(cap));
            assert!(backoff.next_delay() <= Duration::from_secs(cap));
        }
    }

    #[test]
    fn endless_retries_stay_at_the_maximum() {
        let mut backoff = backoff();
        for _ in 0..100 {
            assert!(backoff.next_delay() <= Duration::from_secs(60));
        }
        assert_eq!(backoff.cap(), Duration::from_secs(60));
    }

    #[test]
    fn a_reset_starts_again_from_the_base() {
        let mut backoff = backoff();
        for _ in 0..10 {
            backoff.next_delay();
        }
        backoff.reset();
        assert_eq!(backoff.cap(), Duration::from_secs(1));
    }
}
