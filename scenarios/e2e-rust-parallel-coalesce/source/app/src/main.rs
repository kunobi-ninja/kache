fn main() {
    let x: u64 = 1;
    println!(
        "parallel-coalesce ok: {}",
        heavy1::sum(x) ^ heavy2::sum(x) ^ heavy3::sum(x)
    );
}
