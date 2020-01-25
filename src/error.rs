#[derive(Debug)]
pub enum Error {
    TagNotExist,
    BothSegmentsNotReady,
    ServiceNotReady,
    #[cfg(any(feature = "mysql", feature = "postgres"))]
    SqlXError(sqlx::error::Error),
    #[cfg(feature = "redis")]
    RedisError(redis_async::error::Error),
}

#[cfg(any(feature = "mysql", feature = "postgres"))]
impl From<sqlx::error::Error> for Error {
    fn from(err: sqlx::error::Error) -> Self {
        Self::SqlXError(err)
    }
}

#[cfg(feature = "redis")]
impl From<redis_async::error::Error> for Error {
    fn from(err: redis_async::error::Error) -> Self {
        Self::RedisError(err)
    }
}
