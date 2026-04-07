use std::net::SocketAddr;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

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
                handler(stream, sockaddr).await?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handler(stream: TcpStream, _sockaddr: SocketAddr) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut buffer = String::new();

    loop {
        let result = reader.read_line(&mut buffer).await;
        match result {
            Err(e) => return Err(e.into()),
            Ok(0) => return Ok(()),
            Ok(_) => {
                if buffer.contains("PING") {
                    writer.write_all("+PONG\r\n".as_bytes()).await?;
                }
                buffer.clear();
            }
        }
    }
}
