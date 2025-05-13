// build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let v = option_env!("CARGO_APP_VERSION").unwrap_or("dev");
    println!("cargo:rustc-env=CARGO_APP_VERSION={v}");
    Ok(())
}
