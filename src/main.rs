mod protocol;
mod server;
mod state;
mod scheduler;

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::{fs, io};
use std::time::Duration;

fn main() -> io::Result<()> {
    let sock_path = "/tmp/omp-rm.sock";

    if Path::new(sock_path).exists() {
        fs::remove_file(sock_path)?;
    }

    let capacity: u16 = std::thread::available_parallelism()
        .map(|n| n.get() as u16)
        .unwrap_or(1);

    let state = state::SharedState::new(state::RmState::new());

    // Context
    let scheduler = Arc::new(Mutex::new(scheduler::Scheduler::new(capacity, Duration::from_secs(5))));

    server::run_unix_socket_server(sock_path, state, scheduler)
}
