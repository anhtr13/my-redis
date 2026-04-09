use std::net::SocketAddr;

use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
};

use crate::data_type::DataType;

pub async fn handle(stream: TcpStream, _sockaddr: SocketAddr) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    loop {
        let data = DataType::deserialize(&mut reader).await?;
        match data {
            DataType::Array { value: arr } => {
                let res = match &arr[0] {
                    DataType::BulkString { value } => {
                        if value.to_uppercase() == "PING" {
                            "+PONG\r\n".as_bytes()
                        } else if value.to_uppercase() == "ECHO" && arr.len() > 1 {
                            &arr[1].serialize()
                        } else {
                            &vec![]
                        }
                    }
                    _ => &[],
                };
                writer.write_all(res).await?
            }
            _ => {}
        }
    }
}
