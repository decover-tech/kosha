use std::collections::{HashMap, VecDeque};
use std::sync::{mpsc, Mutex};
use std::time::{Duration, Instant};

use kosha_core::SegmentId;

const MIN_PEERS: usize = 3;
const RECENT_PEERS: usize = 32;

pub(crate) struct Watchdog {
    multiplier: u32,
    floor: Duration,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    active: HashMap<SegmentId, Instant>,
    recent: VecDeque<Duration>,
}

#[derive(Debug)]
pub(crate) struct Timeout {
    pub segment: SegmentId,
    pub elapsed: Duration,
    pub limit: Duration,
}

pub(crate) struct WatchedSegment<'a> {
    watchdog: &'a Watchdog,
    segment: SegmentId,
    elapsed: Option<Duration>,
}

impl Watchdog {
    pub(crate) fn new(multiplier: u32, floor: Duration) -> Self {
        Self {
            multiplier,
            floor,
            state: Mutex::default(),
        }
    }

    pub(crate) fn start(&self, segment: SegmentId, started: Instant) -> WatchedSegment<'_> {
        self.state
            .lock()
            .unwrap()
            .active
            .insert(segment.clone(), started);
        WatchedSegment {
            watchdog: self,
            segment,
            elapsed: None,
        }
    }

    pub(crate) fn monitor(&self, stop: mpsc::Receiver<()>) -> Option<Timeout> {
        while let Err(mpsc::RecvTimeoutError::Timeout) = stop.recv_timeout(Duration::from_secs(1)) {
            if let Some(timeout) = self.overdue(Instant::now()) {
                return Some(timeout);
            }
        }
        None
    }

    fn overdue(&self, now: Instant) -> Option<Timeout> {
        let state = self.state.lock().unwrap();
        let (segment, &started) = state.active.iter().min_by_key(|(_, started)| *started)?;
        let elapsed = now.saturating_duration_since(started);
        let baseline = if state.recent.len() >= MIN_PEERS {
            let mut recent: Vec<_> = state.recent.iter().copied().collect();
            let middle = recent.len() / 2;
            *recent.select_nth_unstable(middle).1
        } else {
            self.floor
        };
        let limit = self.floor.max(baseline.saturating_mul(self.multiplier));
        (elapsed >= limit).then(|| Timeout {
            segment: segment.clone(),
            elapsed,
            limit,
        })
    }
}

impl WatchedSegment<'_> {
    pub(crate) fn sealed(mut self, elapsed: Duration) {
        self.elapsed = Some(elapsed);
    }
}

impl Drop for WatchedSegment<'_> {
    fn drop(&mut self) {
        let mut state = self.watchdog.state.lock().unwrap();
        state.active.remove(&self.segment);
        if let Some(elapsed) = self.elapsed {
            if state.recent.len() == RECENT_PEERS {
                state.recent.pop_front();
            }
            state.recent.push_back(elapsed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    #[test]
    fn small_build_gets_a_longer_but_finite_timeout() {
        let watchdog = Watchdog::new(10, secs(300));
        let start = Instant::now();
        let id = SegmentId("example-offline-000000".into());
        let _stuck = watchdog.start(id.clone(), start);
        watchdog
            .start(SegmentId("fast".into()), start)
            .sealed(secs(1));
        assert!(watchdog.overdue(start + secs(2999)).is_none());
        let timeout = watchdog.overdue(start + secs(3000)).unwrap();
        assert_eq!(timeout.segment, id);
        assert_eq!(timeout.elapsed, secs(3000));
        assert_eq!(timeout.limit, secs(3000));
    }

    #[test]
    fn flags_only_the_stuck_segment_using_the_peer_median_and_floor() {
        for (peer_secs, expected_limit) in [(16, 300), (45, 450)] {
            let watchdog = Watchdog::new(10, secs(300));
            let start = Instant::now();
            let stuck_id = SegmentId("example-offline-000100".into());
            let stuck = watchdog.start(stuck_id.clone(), start);
            for (i, elapsed) in [peer_secs - 1, peer_secs, 200].into_iter().enumerate() {
                watchdog
                    .start(SegmentId(format!("peer-{i}")), start)
                    .sealed(secs(elapsed));
            }
            let _recent = watchdog.start(
                SegmentId("recent".into()),
                start + secs(expected_limit - 10),
            );
            assert!(watchdog.overdue(start + secs(expected_limit - 1)).is_none());
            let timeout = watchdog.overdue(start + secs(expected_limit)).unwrap();
            assert_eq!(timeout.segment, stuck_id);
            assert_eq!(timeout.elapsed, secs(expected_limit));
            assert_eq!(timeout.limit, secs(expected_limit));
            drop(stuck);
            assert!(watchdog.overdue(start + secs(expected_limit)).is_none());
        }
    }

    #[test]
    fn monitor_reports_a_timeout_without_a_worker_finishing() {
        let watchdog = Watchdog::new(1, Duration::from_millis(1));
        let id = SegmentId("stalled".into());
        let _stuck = watchdog.start(id.clone(), Instant::now());
        let (stop, stopped) = mpsc::channel();
        let (report, reported) = mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| report.send(watchdog.monitor(stopped)).unwrap());
            let result = reported.recv_timeout(secs(10));
            drop(stop);
            assert_eq!(result.unwrap().unwrap().segment, id);
        });
    }
}
