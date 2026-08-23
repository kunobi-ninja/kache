#![cfg(target_os = "macos")]

use std::process::Command;

#[test]
fn kache_binary_embeds_local_network_privacy_metadata() {
    let output = Command::new("otool")
        .args(["-s", "__TEXT", "__info_plist", env!("CARGO_BIN_EXE_kache")])
        .output()
        .expect("run otool against the built kache binary");
    assert!(
        output.status.success(),
        "otool could not read __TEXT,__info_plist: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("otool output is UTF-8");
    let mut bytes = Vec::new();
    for line in stdout.lines().skip(2) {
        for word in line.split_whitespace().skip(1) {
            let pairs: Vec<_> = word.as_bytes().as_chunks::<2>().0.iter().collect();
            // `otool -s` prints each little-endian 32-bit word as a host-order
            // hex integer, so restore its byte order before parsing the plist.
            for pair in pairs.into_iter().rev() {
                let pair = std::str::from_utf8(pair).expect("otool hex is ASCII");
                bytes.push(u8::from_str_radix(pair, 16).expect("otool emitted valid hex"));
            }
        }
    }
    let plist = String::from_utf8(bytes).expect("embedded Info.plist is UTF-8");

    assert!(plist.contains("<string>ninja.kunobi.kache</string>"));
    assert!(plist.contains("NSLocalNetworkUsageDescription"));
    assert!(plist.contains("build-cache servers configured on your local network"));
}
