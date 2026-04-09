mod data_type;
mod handler;
mod server;

use std::sync::Arc;

use tokio::net::TcpListener;

use crate::{handler::handle, server::Server};

#[tokio::main()]
async fn main() -> anyhow::Result<()> {
    println!("Logs from program:");

    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("failed to listening");

    let server = Arc::new(Server::new());

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, sockaddr)) => {
                let s = server.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle(s, stream, sockaddr).await {
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
