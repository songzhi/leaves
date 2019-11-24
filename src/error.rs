#[derive(Debug)]
pub enum Error {
    TagNotExist,
    BothSegmentsNotReady,
    ServiceNotReady,
    MySqlDataBaseError(mysql_async::error::Error),
}

impl From<mysql_async::error::Error> for Error {
    fn from(err: mysql_async::error::Error) -> Self {
        Self::MySqlDataBaseError(err)
    }
}