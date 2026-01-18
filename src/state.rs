use crate::protocol::Request;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub struct ClientInfo {
    pub max_threads: u16,
    pub hint_threads: u16,
    pub last_seen: Instant,
}

#[derive(Clone, Debug)]
pub struct RmState {
    pub clients: HashMap<u32, ClientInfo>,
}

impl RmState {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }

    pub fn upsert_client(&mut self, req: &Request) {
        self.clients.insert(
            req.pid,
            ClientInfo {
                max_threads: req.max_threads.max(1),
                hint_threads: req.hint_threads.max(1),
                last_seen: Instant::now(),
            },
        );
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
            .map(|(pid, info)| (*pid, *info))
            .collect()
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
