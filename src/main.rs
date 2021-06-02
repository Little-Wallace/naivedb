use simple_sql::server::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let s = Server::create();
    s.start().await;
    Ok(())
}
