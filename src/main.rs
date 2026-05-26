mod protocol;
mod server;

use std::sync::Arc;

use tokio::net::TcpListener;

use crate::server::{handler::handle_connection, storage::Storage};

#[tokio::main()]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("failed to listening");

    let storage = Arc::new(Storage::new());

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, sockaddr)) => {
                let storage = storage.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(storage, stream, sockaddr).await {
                        eprintln!("error when handle connection: {e}");
                    }
                });
            }
            Err(e) => {
                break Err(e.into());
            }
        }
    }
}
