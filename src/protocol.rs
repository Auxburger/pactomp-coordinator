#[derive(Clone, Copy, Debug)]
pub struct Request {
    pub pid: u32,
    pub max_threads: u16,
    pub hint_threads: u16,
}

pub fn parse_request(buf: [u8; 16]) -> Request {
    let pid = u32::from_ne_bytes(buf[0..4].try_into().unwrap());
    let max_threads = u16::from_ne_bytes(buf[4..6].try_into().unwrap());
    let hint_threads = u16::from_ne_bytes(buf[6..8].try_into().unwrap());
    Request {
        pid,
        max_threads,
        hint_threads,
    }
}

/// Reply layout (12 bytes):
///   0-1:  granted  (u16) — thread count granted by DRM
///   2-3:  ttl_ms   (u16) — how long the grant is valid
///   4-7:  epoch    (u32) — monotonic counter (informational)
///   8-9:  base_cpu (u16) — first CPU of the assigned range (0 if no pinning)
///  10-11: num_cpus (u16) — number of consecutive CPUs assigned (0 if no pinning)
///
/// The runtime interprets CPUs as [base_cpu, base_cpu + num_cpus) and
/// calls sched_setaffinity(0, …) on the master thread before forking the team.
pub fn build_reply(granted: u16, ttl_ms: u16, epoch: u32, base_cpu: u16, num_cpus: u16) -> [u8; 12] {
    let mut rep = [0u8; 12];
    rep[0..2].copy_from_slice(&granted.to_ne_bytes());
    rep[2..4].copy_from_slice(&ttl_ms.to_ne_bytes());
    rep[4..8].copy_from_slice(&epoch.to_ne_bytes());
    rep[8..10].copy_from_slice(&base_cpu.to_ne_bytes());
    rep[10..12].copy_from_slice(&num_cpus.to_ne_bytes());
    rep
}

/// Parse a CPU list string like "0,1,2-7,10" into a sorted Vec<u16>.
pub fn parse_cpu_list(s: &str) -> Vec<u16> {
    let mut cpus = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some(dash) = part.find('-') {
            let a: u16 = part[..dash].trim().parse().unwrap_or(0);
            let b: u16 = part[dash + 1..].trim().parse().unwrap_or(0);
            for c in a..=b {
                cpus.push(c);
            }
        } else if let Ok(c) = part.parse::<u16>() {
            cpus.push(c);
        }
    }
    cpus.sort_unstable();
    cpus.dedup();
    cpus
}
