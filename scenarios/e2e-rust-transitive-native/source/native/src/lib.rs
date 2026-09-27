unsafe extern "C" {
    fn native_value() -> i32;
}

pub fn value() -> i32 {
    unsafe { native_value() }
}
