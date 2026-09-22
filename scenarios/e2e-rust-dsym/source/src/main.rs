fn main() {
    // Use the dependency so it is actually compiled and linked in.
    let mut buf = itoa::Buffer::new();
    println!("rust-dsym: {}", buf.format(42u32));
}

#[cfg(test)]
mod tests {
    // The unit-test harness never calls `main`, so it needs its own use of
    // the dependency for its dSYM to carry an itoa compile unit.
    #[test]
    fn formats() {
        assert_eq!(itoa::Buffer::new().format(42u32), "42");
    }
}
