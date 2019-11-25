#[derive(Debug)]
pub enum Error {
    TagNotExist,
    BothSegmentsNotReady,
    ServiceNotReady,
    #[cfg(feature = "mysql")]
    MySqlError(mysql_async::error::Error),
    #[cfg(feature = "redis")]
    RedisError(redis_async::error::Error),
}

#[cfg(feature = "mysql")]
impl From<mysql_async::error::Error> for Error {
    fn from(err: mysql_async::error::Error) -> Self {
        Self::MySqlError(err)
    }
}

#[cfg(feature = "redis")]
impl From<redis_async::error::Error> for Error {
    fn from(err: redis_async::error::Error) -> Self { Self::RedisError(err) }
}