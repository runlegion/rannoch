//! graph toy round 5d: a stand-in for a game holding the machine. Keeps `cores` threads busy and
//! writes `mbps` MB/s to a file in `dir`, flushing each 5 MB write, until killed.

use std::io::{Seek, SeekFrom, Write};
use std::time::{Duration, Instant};

fn main() {
    let cores: usize = std::env::args().nth(1).and_then(|v| v.parse().ok()).unwrap_or(9);
    let mbps: u64 = std::env::args().nth(2).and_then(|v| v.parse().ok()).unwrap_or(50);
    let dir = std::env::args().nth(3).unwrap_or_else(|| ".".into());
    for _ in 0..cores {
        std::thread::spawn(|| {
            let mut x: u64 = 1;
            loop {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                std::hint::black_box(x);
            }
        });
    }
    let path = format!("{dir}/hog.bin");
    let mut f = std::fs::File::create(&path).expect("hog file");
    let chunk = vec![7u8; 5 * 1024 * 1024];
    let every = Duration::from_millis(5 * 1000 / mbps.max(1));
    let mut n = 0u64;
    loop {
        let t = Instant::now();
        if n % 20 == 0 {
            f.seek(SeekFrom::Start(0)).expect("seek");
        }
        f.write_all(&chunk).expect("write");
        f.sync_all().expect("flush");
        n += 1;
        if let Some(rest) = every.checked_sub(t.elapsed()) {
            std::thread::sleep(rest);
        }
    }
}
