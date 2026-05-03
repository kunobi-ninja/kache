use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Greeting {
    name: String,
    count: u32,
}

fn main() -> anyhow::Result<()> {
    let g = Greeting {
        name: "kache".into(),
        count: 1,
    };
    println!("{}", serde_json::to_string(&g)?);
    Ok(())
}
