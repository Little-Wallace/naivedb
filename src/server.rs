// Copyright 2021 Little-Wallace, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::config::Config;
use crate::conn::MysqlServerCore;
use futures::prelude::*;
use msql_srv::*;
use std::io;
use std::sync::Arc;
use tokio;

pub struct Server {
    core: Arc<MysqlServerCore>,
    address: String,
}

impl Server {
    pub async fn new(address: String, config: Config) -> Server {
        let core = Arc::new(MysqlServerCore::new(config).await);
        Server { core, address }
    }

    pub async fn start(&self) -> io::Result<()> {
        let core = self.core.clone();
        let address = self.address.clone();

        let r = tokio::spawn(async move {
            let mut listener = tokio::net::TcpListener::bind(&address).await?;
            let port = listener.local_addr()?.port();
            println!("listening on port: {}", port);

            let mut incoming = listener.incoming();
            while let Some(stream) = incoming.next().await {
                match stream {
                    Ok(s) => {
                        let conn = core.create_connection();
                        tokio::spawn(MysqlIntermediary::run_on_tcp(conn, s).map_err(|err| {
                            eprintln!("MySQL error: {}", err);
                        }));
                    }
                    Err(err) => {
                        eprintln!("Connection error: {}", err);
                    }
                }
            }

            Ok::<(), io::Error>(())
        })
        .await;

        match r {
            Ok(res) => res?,
            Err(err) => {
                panic!("Runtime error: {}", err);
            }
        }

        Ok(())
    }
}
