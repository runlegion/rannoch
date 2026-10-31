//! Times one fill on a copy of a graph file.
fn main() {
    let path = std::env::args().nth(1).expect("path");
    let key: u8 = std::env::args().nth(2).map_or(7, |k| k.parse().expect("key"));
    let mut g = rannoch_store::open(&path, &[key; 32]).expect("open");
    let t = std::time::Instant::now();
    let to = g.fill().expect("fill");
    println!("fill to {to:?} in {:.1} ms, stopped {}", t.elapsed().as_secs_f64() * 1e3, g.stopped().expect("s").len());
}
