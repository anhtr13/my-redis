mod data_type;
mod handler;

use tokio::net::TcpListener;

use crate::handler::handle;

#[tokio::main()]
async fn main() -> anyhow::Result<()> {
    println!("Logs from program:");

    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("failed to listening");

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, sockaddr)) => {
                tokio::spawn(async move {
                    if let Err(e) = handle(stream, sockaddr).await {
                        eprintln!("connection error: {e}");
                    }
                });
            }
            Err(e) => {
                break Err(e.into());
            }
        }
    }
}
