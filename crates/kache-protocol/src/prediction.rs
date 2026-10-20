#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PortablePrediction {
    pub schema: u32,
    pub sources: Vec<Portable>,
    pub env_deps: Vec<(String, Option<Portable>)>,
    pub tree: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Portable {
    Literal(String),
    OutDir(String),
    Workspace(String),
    /// The bytes after `<CARGO_HOME>/registry/src`. Written only for a row
    /// that travels through the remote (`prediction_share`), so it
    /// carries no local path.
    Registry(String),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedPrediction {
    Portable(PortablePrediction),
    Plain(PlainRow),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PlainRow {
    pub schema: u32,
    pub sources: Vec<Portable>,
    pub env_deps: Vec<(String, Option<Portable>)>,
    pub tree: Option<String>,
}
