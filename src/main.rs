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

    let capacity: u16 = std::env::var("DRM_CAPACITY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get() as u16)
                .unwrap_or(1)
        });

    // Optional: ordered list of CPUs the DRM may assign to clients.
    // Format: comma-separated integers or ranges, e.g. "0,1,2-7,10-15".
    // When unset, CPU pinning is disabled (base_cpu=0, num_cpus=0 in reply).
    let cpu_pool = std::env::var("DRM_CPU_LIST")
        .ok()
        .map(|s| protocol::parse_cpu_list(&s))
        .unwrap_or_default();

    if cpu_pool.is_empty() {
        println!("DRM_CPU_LIST not set — CPU pinning disabled");
    } else {
        println!(
            "CPU pool ({} cores): {}–{}",
            cpu_pool.len(),
            cpu_pool[0],
            cpu_pool[cpu_pool.len() - 1]
        );
    }

    let state = state::SharedState::new(state::RmState::new(cpu_pool));

    // Context
    let scheduler = Arc::new(Mutex::new(scheduler::Scheduler::new(capacity, Duration::from_secs(5))));

    server::run_unix_socket_server(sock_path, state, scheduler)
}
