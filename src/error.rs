#[derive(Debug)]
pub enum Error {
    TagNotExist,
    BothSegmentsNotReady,
    ServiceNotReady,
    SerializationError,
    #[cfg(any(feature = "mysql", feature = "postgres"))]
    SqlXError(sqlx::error::Error),
    #[cfg(feature = "redis")]
    RedisError(darkredis::Error),
}

#[cfg(any(feature = "mysql", feature = "postgres"))]
impl From<sqlx::error::Error> for Error {
    fn from(err: sqlx::error::Error) -> Self {
        Self::SqlXError(err)
    }
}

#[cfg(feature = "redis")]
impl From<darkredis::Error> for Error {
    fn from(err: darkredis::Error) -> Self {
        Self::RedisError(err)
    }
}
