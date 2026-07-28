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
    /// The ordered CPU pool for this domain, parsed from DRM_CPU_LIST.
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

    /// Assign a contiguous block of `count` CPUs from `cpu_pool` to `pid`.
    ///
    /// If the client already has a slot of exactly the requested count, it is
    /// returned unchanged (stable assignment within a benchmark run).  If the
    /// count differs the old slot is freed and a new one is searched.
    ///
    /// Returns `(base_cpu, num_cpus)` where `base_cpu` is the actual CPU number
    /// of the first assigned core and `num_cpus == count`.  The runtime is
    /// expected to treat CPUs as the contiguous range `[base_cpu, base_cpu + num_cpus)`.
    ///
    /// Returns `(0, 0)` when no cpu_pool is configured or no free slot is found.
    pub fn get_or_assign_cpus(&mut self, pid: u32, count: u16) -> (u16, u16) {
        if self.cpu_pool.is_empty() || count == 0 {
            return (0, 0);
        }

        let count_usize = count as usize;
        let pool_len = self.cpu_pool.len();

        // Return existing assignment if count matches.
        if let Some(info) = self.clients.get(&pid) {
            if info.cpu_slot_count == count_usize
                && info.cpu_slot_start + info.cpu_slot_count <= pool_len
            {
                return (self.cpu_pool[info.cpu_slot_start], count);
            }
        }

        // Clear stale / wrong-sized assignment for this pid.
        if let Some(info) = self.clients.get_mut(&pid) {
            info.cpu_slot_start = 0;
            info.cpu_slot_count = 0;
        }

        // Collect index ranges occupied by other active clients.
        let used_ranges: Vec<(usize, usize)> = self
            .clients
            .iter()
            .filter(|&(p, c)| *p != pid && c.cpu_slot_count > 0)
            .map(|(_, c)| (c.cpu_slot_start, c.cpu_slot_start + c.cpu_slot_count))
            .collect();

        // First-fit: find the earliest free block of size count_usize.
        'outer: for start in 0..pool_len {
            if start + count_usize > pool_len {
                break;
            }
            let end = start + count_usize;
            for &(r_start, r_end) in &used_ranges {
                if start < r_end && end > r_start {
                    continue 'outer; // overlaps
                }
            }
            // Free slot found.
            if let Some(info) = self.clients.get_mut(&pid) {
                info.cpu_slot_start = start;
                info.cpu_slot_count = count_usize;
            }
            return (self.cpu_pool[start], count);
        }

        (0, 0) // no free slot
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
