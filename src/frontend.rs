mod version;

#[cfg(target_os = "macos")]
#[used]
#[unsafe(link_section = "__TEXT,__info_plist")]
static MACOS_INFO_PLIST: [u8; include_bytes!("../assets/macos/Info.plist").len()] =
    *include_bytes!("../assets/macos/Info.plist");

fn main() {
    let exit = match kache_client::run_with_binary_name(version::VERSION, env!("CARGO_BIN_NAME")) {
        Ok(exit) => exit,
        Err(error) => {
            eprintln!("error: {error:#}");
            1
        }
    };
    std::process::exit(exit);
}
