use crate::protocol::Request;
use crate::scheduler::fair_share::FairShareStrategy;
use crate::state::RmState;
use std::sync::Arc;
use std::time::Duration;

pub trait SchedulingStrategy: Send + Sync {
    fn name(&self) -> &'static str;
    fn decide(&self, capacity: u16, state: &RmState, req: &Request) -> u16;
}

/// Context in GoF terms
pub struct Scheduler {
    capacity: u16,
    active_window: Duration,
    strategy: Arc<dyn SchedulingStrategy>,
}

impl Scheduler {
    pub fn new(capacity: u16, active_window: Duration) -> Self {
        let strategy = Arc::new(FairShareStrategy::new(active_window));
        Self { capacity, active_window, strategy }
    }

    pub fn set_strategy(&mut self, strategy: Arc<dyn SchedulingStrategy>) {
        self.strategy = strategy;
    }

    pub fn strategy_name(&self) -> &'static str {
        self.strategy.name()
    }

    pub fn decide(&self, state: &RmState, req: &Request) -> u16 {
        self.strategy.decide(self.capacity, state, req)
    }

    pub fn capacity(&self) -> u16 {
        self.capacity
    }

    pub fn active_window(&self) -> Duration {
        self.active_window
    }
}

pub mod fair_share;
