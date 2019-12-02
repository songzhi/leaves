use async_std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use leaves::dao::mysql::MySqlLeafDao;
use leaves::LeafSegment;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let dao = Arc::new(MySqlLeafDao::new("mysql://root:12138@localhost:3306/course").unwrap());
    //    dao.create_table().await.unwrap();
    //    dao.insert_row(1).await.unwrap();

    let mut service = LeafSegment::new(dao);
    service.init().await.unwrap();
    let service = Arc::new(service);
    let mut listener = TcpListener::bind("127.0.0.1:8080").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;
        let service = service.clone();
        tokio::spawn(async move {
            let mut buf = [0; 1024];

            // In a loop, read data from the socket and write the data back.
            loop {
                let n = match socket.read(&mut buf).await {
                    // socket closed
                    Ok(n) if n == 0 => return,
                    Ok(n) => n,
                    Err(e) => {
                        println!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };

                let id = service.get(1).await.unwrap();

                // Write the data back
                if let Err(e) = socket.write_all(&buf[0..n]).await {
                    println!("failed to write to socket; err = {:?}", e);
                    return;
                }
            }
        });
    }
}
