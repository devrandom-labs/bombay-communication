fn main() {
    // `loom` is an opt-in cfg (RUSTFLAGS="--cfg loom") used by the
    // model-check test lane; declare it so check-cfg stays quiet.
    println!("cargo::rustc-check-cfg=cfg(loom)");
}
