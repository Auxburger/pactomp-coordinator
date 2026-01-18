use crate::protocol::Request;
use crate::state::RmState;
use crate::scheduler::SchedulingStrategy;
use std::time::Duration;

pub struct FairShareStrategy {
    active_window: Duration,
}

impl FairShareStrategy {
    pub fn new(active_window: Duration) -> Self {
        Self { active_window }
    }
}

impl SchedulingStrategy for FairShareStrategy {
    fn name(&self) -> &'static str {
        "fair_share"
    }

    fn decide(&self, capacity: u16, state: &RmState, req: &Request) -> u16 {
        let active = state.active_clients(self.active_window);
        let n_active = active.len().max(1) as u16;

        if capacity <= n_active {
            return 1;
        }

        let remaining = capacity - n_active;
        let share = remaining / n_active;
        let base = 1u16;

        let mut granted = base + share;
        granted = granted.max(1);

        // respect per-request max/hint
        granted = granted.min(req.max_threads.max(1));
        granted = granted.max(req.hint_threads.min(req.max_threads.max(1)));

        granted
    }
}
