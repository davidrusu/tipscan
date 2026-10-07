//! tipscan: joins a Logos blockchain network as a passive peer, polls every
//! reachable peer's tip with chain sync `GetTip`, downloads any branch it has
//! not seen, and serves the resulting network-wide block tree.

mod p2p;
mod scan;
mod store;
mod web;
mod wire;

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context as _, Result};
use clap::Parser;
use libp2p::{Multiaddr, identity};
use tracing::info;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Directory for the SQLite database and the node key.
    #[arg(long, env = "TIPSCAN_DATA", default_value = "data")]
    data_dir: PathBuf,
    /// Chain ID (names the identify and Kademlia protocols).
    #[arg(long, default_value = "0.3.0")]
    chain_id: String,
    /// Chain sync stream protocol (carries the fork digest).
    #[arg(long, default_value = "/logos-blockchain/846e7212264a8118b498749419d0b6e66bffa5bd630f0e4cd56922f51e6ac1cc/chainsync")]
    chainsync: String,
    /// Genesis block id (root of the tree; the first sync starts here).
    #[arg(long, default_value = "7a0f476e996fe8e75990a3b58f28cb4fc388881feb1c934eb3dd165a01cd815e")]
    genesis_id: String,
    /// Genesis time, unix ms (slot 0).
    #[arg(long, default_value_t = 1_790_758_800_000)]
    genesis_ms: i64,
    /// Bootstrap peers (multiaddrs with /p2p/<id>). Defaults to the testnet bootstrap nodes.
    #[arg(long = "bootstrap", value_delimiter = ',', default_values_t = [
        "/ip4/65.109.51.37/udp/3000/quic-v1/p2p/12D3KooWFrouXfmrR4nsLMtE7wu15DoMJ6VtoUtHinREZCvbWHar".to_string(),
        "/ip4/65.109.51.37/udp/3001/quic-v1/p2p/12D3KooWJRGau8M1rjT7R5e4YYsgdFhsMX35nRDtMwCDjxQkXAHz".to_string(),
        "/ip4/65.109.51.37/udp/3002/quic-v1/p2p/12D3KooWQXJavMDTRscjauFSgVAB1VLB6Rzpy2uY5SU9Tk7927tb".to_string(),
        "/ip4/65.109.51.37/udp/50001/quic-v1/p2p/12D3KooWSQc7CcGtvWDPF1yCbBthFnQjprfCVHmfmNDUrSmqQsU1".to_string(),
    ])]
    bootstrap: Vec<String>,
    /// Optional QUIC listen address, e.g. /ip4/0.0.0.0/udp/3020/quic-v1.
    #[arg(long)]
    listen: Option<Multiaddr>,
    /// Seconds between scans.
    #[arg(long, default_value_t = 60)]
    interval: u64,
    /// Concurrent GetTip requests.
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
    /// Per-request timeout, seconds.
    #[arg(long, default_value_t = 15)]
    timeout: u64,
    /// Seconds after a block's slot its per-peer arrivals are followed before
    /// being stored permanently.
    #[arg(long, default_value_t = 600)]
    arrival_window: u64,
    /// HTTP address for the API and viewer.
    #[arg(long, default_value = "127.0.0.1:8097")]
    http: String,
    /// Serve web/index.html from this directory instead of the built-in copy.
    #[arg(long)]
    web_dir: Option<PathBuf>,
}

fn load_or_create_key(path: &std::path::Path) -> Result<identity::Keypair> {
    if let Ok(bytes) = std::fs::read(path) {
        return identity::Keypair::from_protobuf_encoding(&bytes).context("decode node key");
    }
    let kp = identity::Keypair::generate_ed25519();
    std::fs::write(path, kp.to_protobuf_encoding()?)?;
    Ok(kp)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "tipscan=info".into()),
        )
        .init();
    let args = Args::parse();
    std::fs::create_dir_all(&args.data_dir)?;
    let keypair = load_or_create_key(&args.data_dir.join("node.key"))?;
    let store = Arc::new(store::Store::open(&args.data_dir.join("tipscan.sqlite"))?);
    store.ensure_genesis(&args.genesis_id)?;
    info!("{} blocks in store", store.block_count());

    let bootstrap = args.bootstrap.iter().map(|s| s.parse()).collect::<Result<Vec<Multiaddr>, _>>()?;
    let net = p2p::start(p2p::Config {
        keypair,
        chain_id: args.chain_id.clone(),
        chainsync: args.chainsync.clone(),
        bootstrap,
        listen: args.listen.clone(),
        idle_timeout: Duration::from_secs(args.interval * 5),
    })?;
    let local_peer = net.local_peer_id.to_string();
    info!("local peer id {local_peer}");

    let app = web::router(web::AppState {
        store: store.clone(),
        genesis_ms: args.genesis_ms,
        web_dir: args.web_dir.clone(),
        local_peer,
        interval_s: args.interval,
    });
    let listener = tokio::net::TcpListener::bind(&args.http).await.context("bind http")?;
    info!("viewer on http://{}/", args.http);
    tokio::spawn(async move { axum::serve(listener, app).await });

    scan::Scanner {
        net,
        store,
        genesis: args.genesis_id,
        genesis_ms: args.genesis_ms,
        arrival_window: Duration::from_secs(args.arrival_window),
        interval: Duration::from_secs(args.interval),
        concurrency: args.concurrency,
        request_timeout: Duration::from_secs(args.timeout),
    }
    .run()
    .await;
    Ok(())
}
