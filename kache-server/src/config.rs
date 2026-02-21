#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub listen: String,
    pub data_dir: String,
    pub endpoint_url: String,
    pub bucket: String,
}
