use std::future::Future;
use std::time::Duration;

pub(crate) fn spawn<T>(task: T)
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    cfg_if::cfg_if! {
        if #[cfg(feature = "runtime-async-std")] {
            async_std::task::spawn(task);
        } else if #[cfg(feature = "runtime-tokio")] {
            tokio::task::spawn(task);
        }
    }
}

pub(crate) async fn sleep(duration: Duration) {
    cfg_if::cfg_if! {
        if #[cfg(feature = "runtime-async-std")] {
            async_std::task::sleep(duration).await;
        } else if #[cfg(feature = "runtime-tokio")] {
            tokio::time::delay_for(duration).await;
        }
    }
}
