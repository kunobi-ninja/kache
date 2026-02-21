use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    name: String,
    version: u32,
}

fn main() {
    let config = Config {
        name: "kache-test".to_string(),
        version: 1,
    };
    let json = serde_json::to_string_pretty(&config).unwrap();
    println!("{json}");
}
