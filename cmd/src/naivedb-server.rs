use naive_sql::server::Server;
use clap::{crate_authors, App, Arg};

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
       .get_matches();

    let address = matches.value_of("addr").unwrap_or("");

    let s = Server::new(address.to_string());
    let _ = s.start().await;
    Ok(())
}
