//! Integration tests for the Extractor

use std::{
    net::TcpStream,
    process::{Child, Command},
    thread::sleep,
    time::Duration,
};

use alloy::primitives::address;
use extractor::Extractor;
use primitives::headers::L1Header;

use eyre::Result;
use tokio_stream::StreamExt;
use url::Url;

/// Spawn Anvil as a child process (auto-mining every second),
/// and kill it when dropped.
struct Anvil(Child);

impl Anvil {
    fn new() -> Result<Self> {
        let child = Command::new("anvil").args(["--port", "8545", "--block-time", "1"]).spawn()?;
        Ok(Self(child))
    }
}

impl Drop for Anvil {
    fn drop(&mut self) {
        self.0.kill().unwrap();
    }
}

fn wait_for_anvil() -> Result<()> {
    for _ in 0..50 {
        if TcpStream::connect("127.0.0.1:8545").is_ok() {
            return Ok(());
        }
        sleep(Duration::from_millis(100));
    }

    Err(eyre::eyre!("Anvil did not start listening on port 8545 in time"))
}

#[tokio::test]
async fn test_get_block_stream() -> Result<()> {
    // Spawn Anvil
    let _anvil = match Anvil::new() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Skipping test_get_block_stream: {e}");
            return Ok(());
        }
    };
    wait_for_anvil()?;

    let ws: Url = Url::parse("ws://127.0.0.1:8545").unwrap();

    // Create Extractor
    let ext = Extractor::new(
        ws.clone(),
        ws,
        None, // no L2 auth RPC for local anvil test
        None, // no JWT secret
        address!("0xa7B208DE7F35E924D59C2b5f7dE3bb346E8A138C"),
        address!("0x3ea351Db28A9d4833Bf6c519F52766788DE14eC1"),
        // address!("0x962C95233f04Ef08E7FaA84DBd1c5171f06f5616"),
        address!("0x0000000000000000000000000000000000000000"),
    )
    .await?;
    let mut stream = ext.get_l1_header_stream().await?;

    // Wait for the first block
    let header: L1Header = stream.next().await.expect("stream ended unexpectedly");
    assert!(header.number > 0);
    assert!(header.timestamp > 0);
    Ok(())
}
