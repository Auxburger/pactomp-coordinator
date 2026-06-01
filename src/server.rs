use crate::protocol::{build_reply, parse_request};
use crate::scheduler::Scheduler;
use crate::state::SharedState;

use std::io::{self, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

pub fn run_unix_socket_server(
    sock_path: &str,
    state: SharedState,
    scheduler: Arc<Mutex<Scheduler>>,
) -> io::Result<()> {
    let listener = UnixListener::bind(sock_path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(sock_path, std::fs::Permissions::from_mode(0o666))?;
    }

    let strategy_name = scheduler.lock().unwrap().strategy_name();
    let capacity = scheduler.lock().unwrap().capacity();
    println!("Listening on {sock_path} (capacity={capacity}, strategy={strategy_name})");

    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                println!("Accepted connection");
                let st = state.clone();
                let sched = Arc::clone(&scheduler);

                thread::spawn(move || {
                    let _ = handle_client(stream, st, sched);
                });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }

    Ok(())
}

fn handle_client(
    mut stream: UnixStream,
    state: SharedState,
    scheduler: Arc<Mutex<Scheduler>>,
) -> io::Result<()> {
    println!("Handling client");

    loop {
        let mut buf = [0u8; 16];

        if let Err(e) = stream.read_exact(&mut buf) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                println!("Client closed connection");
                return Ok(());
            }
            eprintln!("Read error: {e}");
            return Ok(());
        }

        let req = parse_request(buf);
        println!(
            "Request from pid {}: max_threads={}, hint_threads={}",
            req.pid, req.max_threads, req.hint_threads
        );

        let (granted, base_cpu, num_cpus) = state.with_lock(|st| {
            st.upsert_client(&req);
            st.cleanup_stale(Duration::from_secs(5));
            let granted = scheduler.lock().unwrap().decide(st, &req);
            let (base_cpu, num_cpus) = st.get_or_assign_cpus(req.pid, granted);
            (granted, base_cpu, num_cpus)
        });

        let ttl_ms: u16 = 10;
        let epoch: u32 = 1;
        let rep = build_reply(granted, ttl_ms, epoch, base_cpu, num_cpus);
        stream.write_all(&rep)?;

        println!(
            "Granted {granted} threads to pid {} (CPUs {}+{})",
            req.pid, base_cpu, num_cpus
        );
    }
}
