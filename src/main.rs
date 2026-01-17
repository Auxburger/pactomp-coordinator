use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{fs, thread};

#[derive(Clone, Copy)]
struct ClientInfo {
    max_threads: u16,
    hint_threads: u16,
    last_seen: Instant,
}

fn allocate(
    capacity: u16,
    clients: &HashMap<u32, ClientInfo>,
    pid: u32,
    max: u16,
    hint: u16,
) -> u16 {
    // Active clients: seen within last second
    let now = Instant::now();
    let active: Vec<(u32, ClientInfo)> = clients
        .iter()
        .filter(|(_, c)| now.duration_since(c.last_seen) < Duration::from_secs(1))
        .map(|(p, c)| (*p, *c))
        .collect();

    if active.is_empty() {
        return hint.min(max).max(1);
    }

    // Baseline: 1 thread each
    let n_active = active.len() as u16;
    if capacity <= n_active {
        return 1;
    }
    let remaining = capacity - n_active;

    // Simple fair share: remaining equally distributed, capped by each max
    let share = (remaining / n_active).max(1);

    // granted = 1 + min(share, max-1) (cap)
    let base = 1u16;
    let extra_cap = max.saturating_sub(base);
    let granted = base + share.min(extra_cap);
    println!("Granted preliminary {granted} threads to pid {pid}. ");

    // Optionally bias towards hint by taking min(max, max(granted, hint)) etc.
    let _ = pid; // keep pid in signature in case you want per-pid policy later
    granted.min(max).max(1)
}

fn handle(
    mut stream: UnixStream,
    clients: Arc<Mutex<HashMap<u32, ClientInfo>>>,
    capacity: u16,
) -> std::io::Result<()> {
    println!("Handling client");

    loop {
        let mut req = [0u8; 16];

        if let Err(e) = stream.read_exact(&mut req) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                println!("Client closed connection");
                return Ok(());
            }
            eprintln!("Read error: {e}");
            return Ok(());
        }

        let pid = u32::from_ne_bytes(req[0..4].try_into().unwrap());
        let max_threads = u16::from_ne_bytes(req[4..6].try_into().unwrap());
        let hint_threads = u16::from_ne_bytes(req[6..8].try_into().unwrap());

        println!("Request from pid {pid}: max_threads={max_threads}, hint_threads={hint_threads}");

        // Update shared state and compute grant under lock
        let granted = {
            let mut map = clients.lock().unwrap();

            map.insert(
                pid,
                ClientInfo {
                    max_threads,
                    hint_threads,
                    last_seen: Instant::now(),
                },
            );

            // Cleanup stale clients (keeps fairness sane over time)
            let now = Instant::now();
            map.retain(|_, c| now.duration_since(c.last_seen) < Duration::from_secs(5));

            allocate(capacity, &map, pid, max_threads.max(1), hint_threads.max(1))
        };

        let ttl_ms: u16 = 10;
        let epoch: u32 = 1;

        let mut rep = [0u8; 8];
        rep[0..2].copy_from_slice(&granted.to_ne_bytes());
        rep[2..4].copy_from_slice(&ttl_ms.to_ne_bytes());
        rep[4..8].copy_from_slice(&epoch.to_ne_bytes());

        stream.write_all(&rep)?;
    }
}

fn main() -> std::io::Result<()> {
    let sock_path = "/tmp/omp-rm.sock";
    if Path::new(sock_path).exists() {
        fs::remove_file(sock_path)?;
    }
    let listener = UnixListener::bind(sock_path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(sock_path, fs::Permissions::from_mode(0o666))?;
    }

    // Total capacity to distribute (start simple: number of cores)
    let capacity: u16 = std::thread::available_parallelism()
        .map(|n| n.get() as u16)
        .unwrap_or(1);

    println!("Resource manager listening on {sock_path} with capacity {capacity}");

    let clients: Arc<Mutex<HashMap<u32, ClientInfo>>> = Arc::new(Mutex::new(HashMap::new()));

    // Accept loop: spawn a handler thread per connection
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                println!("Accepted connection");
                let clients_clone = Arc::clone(&clients);

                thread::spawn(move || {
                    let _ = handle(stream, clients_clone, capacity);
                });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }

    Ok(())
}
