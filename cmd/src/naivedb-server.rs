use clap::{crate_authors, App, Arg};
use naive_sql::server::Server;
use naive_sql::Config;
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
        .get_matches();

    let address = matches.value_of("addr").unwrap_or("");

    let config = matches
        .value_of_os("config")
        .map_or_else(Config::default, |path| Config::from_file(Path::new(path)));
    let s = Server::new(address.to_string(), config).await;
    let _ = s.start().await;
    Ok(())
}
