use clap::{crate_authors, App, Arg};
use naive_sql::server::Server;
use naive_sql::Config;
use naive_sql::StorageType;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new("TiKV")
        .about("A distributed transactional key-value database powered by Rust and Raft")
        .author(crate_authors!())
        .arg(
            Arg::with_name("addr")
                .short("A")
                .long("addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .default_value("127.0.0.1:0")
                .help("Set the listening address"),
        )
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Set the configuration file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("storage")
                .long("storage")
                .takes_value(true)
                .help("Set the storage type"),
        )
        .get_matches();

    let address = matches.value_of("addr").unwrap_or("");

    let mut config = matches
        .value_of_os("config")
        .map_or_else(Config::default, |path| Config::from_file(Path::new(path)));
    if let Some(v) = matches.value_of("storage") {
        if v == "mem" {
            config.storage = StorageType::Mem;
        } else if v == "tikv" {
            config.storage = StorageType::TiKV;
        } else {
            panic!("unkown storage type");
        }
    }
    let s = Server::new(address.to_string(), config).await;

    if let Err(err) = s.start().await {
        panic!("Server error: {}", err);
    }

    Ok(())
}
