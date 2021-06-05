use crate::conn::MysqlServerCore;
use msql_srv::*;
use std::io;
use std::sync::Arc;
use tokio;
use tokio::stream::{StreamExt};

pub struct Server {
    core: Arc<MysqlServerCore>,
}

impl Server {
    pub fn create() -> Server {
        let core = Arc::new(MysqlServerCore::new());
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
                        let conn = core.create_connection();
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
