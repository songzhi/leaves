use std::fs::File;
use std::io::Read;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    mongodb: Option<String>,
    mysql: Option<String>,
    postgresql: Option<String>,
    sqlite: Option<String>,
}

pub fn get_config(path: &str) -> Option<Config> {
    let mut file = File::open(path).ok()?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).ok()?;
    toml::from_str(buf.as_str()).ok()
}
