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

pub fn build_reply(granted: u16, ttl_ms: u16, epoch: u32) -> [u8; 8] {
    let mut rep = [0u8; 8];
    rep[0..2].copy_from_slice(&granted.to_ne_bytes());
    rep[2..4].copy_from_slice(&ttl_ms.to_ne_bytes());
    rep[4..8].copy_from_slice(&epoch.to_ne_bytes());
    rep
}
