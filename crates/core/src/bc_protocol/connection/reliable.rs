//! The reliable delivery over a UDP connection to a camera, without any I/O
//!
//! Each side numbers its data packets. The receiver hands them over in order,
//! and acks the last one it got in order along with a map of the ones it got
//! after that. The sender keeps each packet until it is acked, and sends it
//! again until then.
use crate::bcudp::model::*;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

/// How far past the next packet one is kept. Further ones are dropped, and the
/// camera sends them again once they fit.
pub(super) const WINDOW: u32 = 4096;
/// How many packets the map in an ack covers: an ack must fit in one datagram
const MAX_SACK: usize = 1000;
/// How long the next packet may stay missing, while later ones wait behind
/// it, before it is given up on
pub(super) const GAP_TIMEOUT: Duration = Duration::from_secs(3);
/// How many sent packets may wait for their ack: no more is sent until some
/// are acked
pub(super) const MAX_UNACKED: usize = 4096;

pub(super) struct ReliableState {
    /// The number of the next packet to hand over
    want: u32,
    /// Set once a packet was handed over. Until then acks say that none has
    /// arrived, and a `want` that wrapped back to 0 does not mean that.
    started: bool,
    /// Packets from `want` on that arrived early, by their distance to `want`
    early: VecDeque<Option<Vec<u8>>>,
    /// Since when packets have been waiting behind a missing one
    gap_since: Option<Instant>,
    /// The number the next packet sent gets
    next_sent: u32,
    /// The packets sent and not acked yet, by number
    sent: BTreeMap<u32, UdpData>,
}

impl ReliableState {
    pub(super) fn new() -> Self {
        Self {
            want: 0,
            started: false,
            early: VecDeque::new(),
            gap_since: None,
            next_sent: 0,
            sent: BTreeMap::new(),
        }
    }

    /// Takes in a data packet from the camera
    pub(super) fn on_data(&mut self, packet_id: u32, payload: Vec<u8>, now: Instant) {
        let distance = packet_id.wrapping_sub(self.want);
        if distance >= WINDOW {
            // Handed over already, or too far ahead to keep
            return;
        }
        let distance = distance as usize;
        if self.early.len() <= distance {
            self.early.resize(distance + 1, None);
        }
        self.early[distance] = Some(payload);
        if distance > 0 && self.gap_since.is_none() {
            self.gap_since = Some(now);
        }
    }

    /// The packets that can now be handed over, in order
    pub(super) fn pop_ready(&mut self, now: Instant) -> Vec<Vec<u8>> {
        let mut ready = vec![];
        while let Some(Some(_)) = self.early.front() {
            ready.extend(self.early.pop_front().flatten());
            self.want = self.want.wrapping_add(1);
        }
        if !ready.is_empty() {
            self.started = true;
            // Whatever still waits, waits behind a new gap
            self.gap_since = self.waiting().then_some(now);
        }
        ready
    }

    /// Whether packets wait behind a missing one
    fn waiting(&self) -> bool {
        self.early.iter().any(Option::is_some)
    }

    /// Gives up on the packets that never came, once later ones have waited
    /// long enough behind them or the window is full. The camera may have
    /// stopped sending them, and waiting would stall the connection for good.
    /// Returns how many were given up on; `pop_ready` then hands over what
    /// waited.
    pub(super) fn skip_gap(&mut self, now: Instant) -> u32 {
        let stuck = self
            .gap_since
            .is_some_and(|since| now.duration_since(since) >= GAP_TIMEOUT);
        let full = self.early.len() >= WINDOW as usize;
        if !(self.waiting() && (stuck || full)) {
            return 0;
        }
        let mut skipped = 0;
        while let Some(None) = self.early.front() {
            self.early.pop_front();
            self.want = self.want.wrapping_add(1);
            skipped += 1;
        }
        skipped
    }

    /// The ack to send the camera: the last packet received in order, and
    /// which of the ones after it arrived
    pub(super) fn ack(&self, camera_id: i32, maybe_latency: u32) -> UdpAck {
        if !self.started {
            return UdpAck::empty(camera_id);
        }
        let span = self
            .early
            .iter()
            .rposition(Option::is_some)
            .map_or(0, |last| last + 1)
            .min(MAX_SACK);
        UdpAck {
            connection_id: camera_id,
            packet_id: self.want.wrapping_sub(1),
            group_id: 0,
            maybe_latency,
            payload: self
                .early
                .iter()
                .take(span)
                .map(|packet| packet.is_some() as u8)
                .collect(),
        }
    }

    /// Whether another packet can be sent, or too many wait for their ack
    pub(super) fn can_send(&self) -> bool {
        self.sent.len() < MAX_UNACKED
    }

    /// Numbers a packet to send, and keeps it until it is acked
    pub(super) fn send(&mut self, camera_id: i32, payload: Vec<u8>) -> UdpData {
        let data = UdpData {
            connection_id: camera_id,
            packet_id: self.next_sent,
            payload,
        };
        self.next_sent = self.next_sent.wrapping_add(1);
        self.sent.insert(data.packet_id, data.clone());
        data
    }

    /// The packets to send again
    pub(super) fn unacked(&self) -> impl Iterator<Item = &UdpData> {
        self.sent.values()
    }

    /// Takes in an ack from the camera, forgetting the packets it got
    pub(super) fn on_ack(&mut self, ack: &UdpAck) {
        if ack.packet_id == 0xffffffff {
            // It has not got anything yet
            return;
        }
        // Everything up to the acked packet, counting across a wrap
        self.sent.retain(|&id, _| {
            let ahead = id.wrapping_sub(ack.packet_id);
            ahead != 0 && ahead < 1 << 31
        });
        for (offset, &got) in ack.payload.iter().enumerate() {
            if got > 0 {
                let id = ack.packet_id.wrapping_add(1).wrapping_add(offset as u32);
                self.sent.remove(&id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(n: u8) -> Vec<u8> {
        vec![n]
    }

    fn numbers(packets: Vec<Vec<u8>>) -> Vec<u8> {
        packets.into_iter().map(|p| p[0]).collect()
    }

    #[test]
    fn packets_are_handed_over_in_order() {
        let now = Instant::now();
        let mut state = ReliableState::new();
        state.on_data(1, packet(1), now);
        assert!(state.pop_ready(now).is_empty());
        state.on_data(0, packet(0), now);
        state.on_data(2, packet(2), now);
        assert_eq!(numbers(state.pop_ready(now)), [0, 1, 2]);
        // Handed over already
        state.on_data(1, packet(1), now);
        assert!(state.pop_ready(now).is_empty());
    }

    #[test]
    fn nothing_beyond_the_window_is_kept() {
        let now = Instant::now();
        let mut state = ReliableState::new();
        state.on_data(WINDOW, packet(9), now);
        assert!(state.early.is_empty());
        state.on_data(WINDOW - 1, packet(8), now);
        assert_eq!(state.early.len(), WINDOW as usize);
    }

    #[test]
    fn numbers_wrap_around() {
        let now = Instant::now();
        let mut state = ReliableState::new();
        state.want = u32::MAX - 1;
        state.on_data(0, packet(2), now);
        state.on_data(u32::MAX, packet(1), now);
        state.on_data(u32::MAX - 1, packet(0), now);
        assert_eq!(numbers(state.pop_ready(now)), [0, 1, 2]);
        assert_eq!(state.want, 1);
        // Still acks for real once the count passed 0
        assert_eq!(state.ack(7, 0).packet_id, 0);
    }

    #[test]
    fn a_packet_that_never_comes_is_given_up_on() {
        let start = Instant::now();
        let mut state = ReliableState::new();
        state.on_data(0, packet(0), start);
        assert_eq!(numbers(state.pop_ready(start)), [0]);
        // 1 is lost, 2 and 3 wait behind it
        state.on_data(2, packet(2), start);
        state.on_data(3, packet(3), start);

        assert_eq!(state.skip_gap(start + GAP_TIMEOUT / 2), 0);
        let later = start + GAP_TIMEOUT;
        assert_eq!(state.skip_gap(later), 1);
        assert_eq!(numbers(state.pop_ready(later)), [2, 3]);
    }

    #[test]
    fn a_full_window_is_skipped_at_once() {
        let now = Instant::now();
        let mut state = ReliableState::new();
        state.on_data(WINDOW - 1, packet(1), now);
        assert_eq!(state.skip_gap(now), WINDOW - 1);
        assert_eq!(numbers(state.pop_ready(now)), [1]);
    }

    #[test]
    fn an_ack_fits_in_a_datagram() {
        let now = Instant::now();
        let mut state = ReliableState::new();
        state.on_data(0, packet(0), now);
        state.pop_ready(now);
        state.on_data(WINDOW - 1, packet(1), now);
        let ack = state.ack(7, 0);
        assert_eq!(ack.packet_id, 0);
        assert_eq!(ack.payload.len(), MAX_SACK);
    }

    #[test]
    fn acks_mark_what_arrived() {
        let now = Instant::now();
        let mut state = ReliableState::new();
        assert_eq!(state.ack(7, 0).packet_id, 0xffffffff);
        state.on_data(0, packet(0), now);
        state.pop_ready(now);
        state.on_data(2, packet(2), now);
        let ack = state.ack(7, 0);
        assert_eq!(ack.packet_id, 0);
        assert_eq!(ack.payload, [0, 1]);
    }

    #[test]
    fn acked_packets_are_forgotten_across_a_wrap() {
        let mut state = ReliableState::new();
        state.next_sent = u32::MAX - 1;
        for n in 0..5 {
            state.send(7, packet(n));
        }
        // Got everything up to 0, past the wrap, and 2 but not 1. (u32::MAX
        // itself cannot be acked: it means that nothing arrived.)
        state.on_ack(&UdpAck {
            connection_id: 0,
            packet_id: 0,
            group_id: 0,
            maybe_latency: 0,
            payload: vec![0, 1],
        });
        let left: Vec<u32> = state.unacked().map(|data| data.packet_id).collect();
        assert_eq!(left, [1]);
    }

    #[test]
    fn sending_stops_while_too_much_waits_for_an_ack() {
        let mut state = ReliableState::new();
        for _ in 0..MAX_UNACKED {
            assert!(state.can_send());
            state.send(7, packet(0));
        }
        assert!(!state.can_send());
    }
}
