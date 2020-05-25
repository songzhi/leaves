use std::fs::File;
use std::io::Read;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub mongodb: Option<String>,
    pub mysql: Option<String>,
    pub postgresql: Option<String>,
    pub sqlite: Option<String>,
    pub redis: Option<String>,
}

pub fn get_config(path: &str) -> Option<Config> {
    let mut file = File::open(path).ok()?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).ok()?;
    toml::from_str(buf.as_str()).ok()
}
