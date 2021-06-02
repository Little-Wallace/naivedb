use crate::conn::MysqlConnection;
use msql_srv::*;
use std::io;
use std::sync::Arc;
use tokio;
use tokio::stream::{Stream, StreamExt};

pub struct MysqlServerCore {}

pub struct Server {
    core: Arc<MysqlServerCore>,
}

impl Server {
    pub fn create() -> Server {
        let core = Arc::new(MysqlServerCore {});
        Server { core }
    }

    pub async fn start(&self) -> io::Result<()> {
        let core = self.core.clone();
        let _ = tokio::spawn(async move {
            let mut listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            println!("listening on port: {}", port);
            let mut incoming = listener.incoming();
            while let Some(stream) = incoming.next().await {
                match stream {
                    Ok(s) => {
                        let conn = MysqlConnection::create(core.clone());
                        if let Err(e) = MysqlIntermediary::run_on_tcp(conn, s).await {
                            println!("connection error, {:?}", e);
                        }
                    }
                    Err(_) => {
                        println!("connection error");
                    }
                }
            }
        })
        .await;
        Ok(())
    }
}
