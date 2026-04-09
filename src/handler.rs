use std::{net::SocketAddr, sync::Arc};

use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
};

use crate::{data_type::DataType, server::Server};

pub async fn handle(
    server: Arc<Server>,
    stream: TcpStream,
    _sockaddr: SocketAddr,
) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    loop {
        let data = DataType::deserialize(&mut reader).await?;
        match data {
            DataType::Array { value } => {
                let cmd = &value[0];
                let mut args = value[1..].to_vec();
                let res = match cmd {
                    DataType::BulkString { value } => match value.to_uppercase().as_str() {
                        "PING" => "+PONG\r\n".as_bytes(),
                        "ECHO" => {
                            if args.is_empty() {
                                "-ERROR\r\n".as_bytes()
                            } else {
                                &args[0].serialize()
                            }
                        }
                        "SET" => {
                            if let Err(e) = server.set(&mut args).await {
                                &DataType::BulkError {
                                    value: e.to_string(),
                                }
                                .serialize()
                            } else {
                                "+OK\r\n".as_bytes()
                            }
                        }
                        "GET" => {
                            if args.is_empty() {
                                "-ERROR\r\n".as_bytes()
                            } else if let Some(data) = server.get(&args[0]).await {
                                &data.serialize()
                            } else {
                                "$-1\r\n".as_bytes()
                            }
                        }
                        _ => "-UNKNOW\r\n".as_bytes(),
                    },
                    _ => "-UNKNOW\r\n".as_bytes(),
                };
                writer.write_all(res).await?
            }
            _ => writer.write_all(b"+OK\r\n").await?,
        }
    }
}
