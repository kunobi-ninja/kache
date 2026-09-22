#[test]
fn formats() {
    assert_eq!(itoa::Buffer::new().format(1u8), "1");
}
