fn main() {
    if std::env::var("CARGO_CFG_TARGET_POINTER_WIDTH").unwrap() != "64" {
        panic!("This crate requires a 64-bit target.");
    }
}
