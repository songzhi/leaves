pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("tag not exist")]
    TagNotExist,
    #[error("both segment not ready")]
    BothSegmentsNotReady,
    #[error("service not ready")]
    ServiceNotReady,
    #[error("serialization error")]
    SerializationError,
    #[cfg(any(feature = "mysql", feature = "postgres", feature = "sqlite"))]
    #[error("sqlx error")]
    SqlX(#[from] sqlx::error::Error),
    #[cfg(feature = "redis")]
    #[error("redis error")]
    Redis(#[from] darkredis::Error),
    #[cfg(feature = "mongo")]
    #[error("mongodb error")]
    MongoDB(#[from] mongodb::error::Error),
}
