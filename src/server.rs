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
use crate::mysql_driver::MysqlServerCore;
use msql_srv::*;
use std::io;
use std::sync::Arc;
use tokio;
use tokio::runtime::{Builder, Runtime};

pub struct Server {
    core: Arc<MysqlServerCore>,
    address: String,
    pool: Runtime,
}

impl Server {
    pub async fn new(address: String, config: Config) -> Server {
        println!("{:?}", config);
        let core = Arc::new(MysqlServerCore::new(config.clone()).await);
        let pool = Builder::new_multi_thread()
            .worker_threads(config.connection_pool_size)
            .thread_name("sql")
            .enable_time()
            .build()
            .unwrap_or_else(|e| panic!("create pool failed, {}", e));
        Server {
            core,
            address,
            pool,
        }
    }

    pub async fn start(&self) -> io::Result<()> {
        let core = self.core.clone();
        let address = self.address.clone();
        let listener = tokio::net::TcpListener::bind(address.as_str()).await?;
        let port = listener.local_addr().unwrap().port();
        println!("listening on port: {}", port);
        while let Ok((stream, _)) = listener.accept().await {
            let conn = core.create_connection();
            self.pool
                .spawn(async move { MysqlIntermediary::run_on_tcp(conn, stream).await });
        }
        Ok(())
    }
}
