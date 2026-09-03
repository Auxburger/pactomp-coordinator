use crate::protocol::Request;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct ClientInfo {
    pub max_threads: u16,
    pub hint_threads: u16,
    pub last_seen: Instant,
    /// Start index into RmState::cpu_pool for this client's CPU slice.
    /// Only meaningful when cpu_slot_count > 0.
    pub cpu_slot_start: usize,
    /// Number of consecutive CPUs assigned from cpu_pool starting at cpu_slot_start.
    /// 0 = not yet assigned.
    pub cpu_slot_count: usize,
}

#[derive(Clone, Debug)]
pub struct RmState {
    pub clients: HashMap<u32, ClientInfo>,
    /// The ordered CPU pool for this domain, parsed from POMP_CPU_LIST.
    /// Empty when no CPU-list env var is set (disables CPU pinning).
    pub cpu_pool: Vec<u16>,
}

impl RmState {
    pub fn new(cpu_pool: Vec<u16>) -> Self {
        Self {
            clients: HashMap::new(),
            cpu_pool,
        }
    }

    pub fn upsert_client(&mut self, req: &Request) {
        let entry = self.clients.entry(req.pid).or_insert_with(|| ClientInfo {
            max_threads: req.max_threads.max(1),
            hint_threads: req.hint_threads.max(1),
            last_seen: Instant::now(),
            cpu_slot_start: 0,
            cpu_slot_count: 0,
        });
        entry.max_threads = req.max_threads.max(1);
        entry.hint_threads = req.hint_threads.max(1);
        entry.last_seen = Instant::now();
    }

    pub fn cleanup_stale(&mut self, max_age: Duration) {
        let now = Instant::now();
        self.clients
            .retain(|_, c| now.duration_since(c.last_seen) < max_age);
    }

    pub fn active_clients(&self, active_window: Duration) -> Vec<(u32, ClientInfo)> {
        let now = Instant::now();
        self.clients
            .iter()
            .filter(|(_, c)| now.duration_since(c.last_seen) < active_window)
            .map(|(pid, info)| (*pid, info.clone()))
            .collect()
    }

    /// Assign this client's slice of `cpu_pool`, sized `count` and placed by the
    /// client's rank among the currently active clients.
    ///
    /// The slice follows from the same active set the scheduler used for the
    /// grant, so it can be computed without any other client acting first, and
    /// it is stable by construction while that set and the grant are unchanged.
    ///
    /// The earlier first-fit search could not do that. It only ever moved the
    /// requesting client's own slice and treated the ranges other clients held
    /// as immovable, so a client that had requested while it was alone kept a
    /// slice covering the whole pool. Every later client then found no free
    /// block and ran with no interval at all until the first one happened to
    /// request again — which a short-lived process never does.
    ///
    /// Returns `(base_cpu, num_cpus)` where `base_cpu` is the actual CPU number
    /// of the first assigned core and `num_cpus == count`.  The runtime is
    /// expected to treat CPUs as the contiguous range `[base_cpu, base_cpu + num_cpus)`.
    ///
    /// Returns `(0, 0)` when no cpu_pool is configured or the slice does not fit.
    pub fn assign_cpus(&mut self, pid: u32, count: u16, active_window: Duration) -> (u16, u16) {
        if self.cpu_pool.is_empty() || count == 0 {
            return (0, 0);
        }

        // Rank by pid: an arbitrary but stable order, so a client keeps its
        // slice across its own repeated requests.
        let mut active: Vec<u32> = self
            .active_clients(active_window)
            .into_iter()
            .map(|(p, _)| p)
            .collect();
        if !active.contains(&pid) {
            active.push(pid);
        }
        active.sort_unstable();
        let rank = active.iter().position(|&p| p == pid).unwrap_or(0);

        let count_usize = count as usize;
        let start = rank * count_usize;
        if start + count_usize > self.cpu_pool.len() {
            return (0, 0);
        }

        if let Some(info) = self.clients.get_mut(&pid) {
            info.cpu_slot_start = start;
            info.cpu_slot_count = count_usize;
        }
        (self.cpu_pool[start], count)
    }
}

/// Shared, cloneable state handle
#[derive(Clone)]
pub struct SharedState {
    inner: Arc<Mutex<RmState>>,
}

impl SharedState {
    pub fn new(state: RmState) -> Self {
        Self {
            inner: Arc::new(Mutex::new(state)),
        }
    }

    pub fn with_lock<T>(&self, f: impl FnOnce(&mut RmState) -> T) -> T {
        let mut guard = self.inner.lock().unwrap();
        f(&mut guard)
    }
}
