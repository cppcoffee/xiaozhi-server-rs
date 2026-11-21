use std::net::SocketAddr;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Failed to read or write config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to parse TOML config: {0}")]
    Toml(#[from] toml::de::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("Failed to bind the OTA HTTP listener to {addr}: {source}")]
    BindHttp {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to bind the WebSocket listener to {addr}: {source}")]
    BindWs {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("Server error: {0}")]
    Serve(#[from] std::io::Error),
}
