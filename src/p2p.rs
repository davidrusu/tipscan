//! The libp2p side: a minimal peer that speaks identify, Kademlia (client
//! mode) and chain sync, discovers the network, and keeps a peer book.
//!
//! It never gossips, never serves blocks and never claims to be online: an
//! inbound chain sync request gets `NodeNotOnline` / `Unknown`, like a node
//! that is still bootstrapping.

use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context as _, Result};
use futures::StreamExt as _;
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, SwarmBuilder, identify, identity, kad,
    multiaddr::Protocol,
    swarm::{NetworkBehaviour, SwarmEvent},
};
use lb_cryptarchia_sync::{BlocksUnavailableReason, GetTipResponse, GetTipResponseReason};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::{
    store::now_ms,
    wire::{DownloadBlocksResponse, RequestMessage, read_msg, write_msg},
};

#[derive(NetworkBehaviour)]
struct Behaviour {
    identify: identify::Behaviour,
    kad: kad::Behaviour<kad::store::MemoryStore>,
    stream: libp2p_stream::Behaviour,
}

#[derive(Debug, Clone, Default)]
pub struct PeerEntry {
    pub addrs: Vec<Multiaddr>,
    pub agent: Option<String>,
    /// Whether identify listed our chain sync protocol (None until identified).
    pub chainsync: Option<bool>,
    pub connected: bool,
    pub first_seen: i64,
    pub last_seen: i64,
}

pub type PeerBook = Arc<Mutex<HashMap<PeerId, PeerEntry>>>;

pub enum Command {
    Dial(PeerId, Vec<Multiaddr>),
    RandomWalk,
}

pub struct Network {
    pub local_peer_id: PeerId,
    pub control: libp2p_stream::Control,
    pub book: PeerBook,
    pub commands: mpsc::UnboundedSender<Command>,
    pub chainsync: StreamProtocol,
}

pub struct Config {
    pub keypair: identity::Keypair,
    pub chain_id: String,
    pub chainsync: String,
    pub bootstrap: Vec<Multiaddr>,
    pub listen: Option<Multiaddr>,
    pub idle_timeout: Duration,
}

/// Only addresses a host on the public internet can dial.
pub fn dialable(addr: &Multiaddr) -> bool {
    let mut public_ip = false;
    let mut quic = false;
    for p in addr.iter() {
        match p {
            Protocol::Ip4(ip) => public_ip = is_public(IpAddr::V4(ip)),
            Protocol::Ip6(ip) => public_ip = is_public(IpAddr::V6(ip)),
            Protocol::Dns(_) | Protocol::Dns4(_) | Protocol::Dns6(_) => public_ip = true,
            Protocol::QuicV1 => quic = true,
            _ => {}
        }
    }
    public_ip && quic
}

fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => {
            let o = v.octets();
            !(v.is_private() || v.is_loopback() || v.is_link_local() || v.is_unspecified()
                || v.is_broadcast() || v.is_documentation()
                || (o[0] == 100 && (o[1] & 0xc0) == 64)) // CGNAT 100.64/10 (incl. tailscale)
        }
        IpAddr::V6(v) => {
            let s = v.segments();
            !(v.is_loopback() || v.is_unspecified() || (s[0] & 0xfe00) == 0xfc00 || (s[0] & 0xffc0) == 0xfe80)
        }
    }
}

fn strip_p2p(addr: &Multiaddr) -> Multiaddr {
    addr.iter().filter(|p| !matches!(p, Protocol::P2p(_))).collect()
}

pub fn start(cfg: Config) -> Result<Network> {
    let local_peer_id = cfg.keypair.public().to_peer_id();
    let identify_version = format!("/logos-blockchain/{}/identify", cfg.chain_id);
    let kad_protocol = StreamProtocol::try_from_owned(format!("/logos-blockchain/{}/kad", cfg.chain_id))?;
    let chainsync = StreamProtocol::try_from_owned(cfg.chainsync.clone())?;

    let mut swarm = SwarmBuilder::with_existing_identity(cfg.keypair.clone())
        .with_tokio()
        .with_quic()
        .with_dns()?
        .with_behaviour(|key| {
            let mut kcfg = kad::Config::new(kad_protocol.clone());
            kcfg.set_query_timeout(Duration::from_secs(60));
            let mut kad = kad::Behaviour::with_config(local_peer_id, kad::store::MemoryStore::new(local_peer_id), kcfg);
            // Client mode: we query the DHT but are not advertised in it.
            kad.set_mode(Some(kad::Mode::Client));
            Behaviour {
                identify: identify::Behaviour::new(
                    identify::Config::new(identify_version.clone(), key.public())
                        .with_agent_version(format!("tipscan/{}", env!("CARGO_PKG_VERSION"))),
                ),
                kad,
                stream: libp2p_stream::Behaviour::new(),
            }
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(cfg.idle_timeout))
        .build();

    if let Some(addr) = &cfg.listen {
        swarm.listen_on(addr.clone()).context("listen")?;
    }

    let book: PeerBook = Arc::default();
    let control = swarm.behaviour().stream.new_control();

    // Answer inbound chain sync like a node that is not online yet.
    let mut incoming = swarm
        .behaviour()
        .stream
        .new_control()
        .accept(chainsync.clone())
        .context("register chainsync handler")?;
    tokio::spawn(async move {
        while let Some((peer, mut stream)) = incoming.next().await {
            tokio::spawn(async move {
                match read_msg::<_, RequestMessage>(&mut stream).await {
                    Ok(RequestMessage::GetTip) => {
                        let _ = write_msg(&mut stream, &GetTipResponse::Failure(GetTipResponseReason::NodeNotOnline)).await;
                    }
                    Ok(RequestMessage::DownloadBlocksRequest(_)) => {
                        let _ = write_msg(&mut stream, &DownloadBlocksResponse::Failure(BlocksUnavailableReason::Unknown)).await;
                    }
                    Err(e) => debug!("inbound chainsync from {peer}: {e:#}"),
                }
            });
        }
    });

    for addr in &cfg.bootstrap {
        let Some(Protocol::P2p(peer)) = addr.iter().last() else {
            warn!("bootstrap address {addr} has no /p2p/ peer id; skipped");
            continue;
        };
        let bare = strip_p2p(addr);
        swarm.behaviour_mut().kad.add_address(&peer, bare.clone());
        let mut b = book.lock().unwrap();
        let e = b.entry(peer).or_insert_with(|| PeerEntry { first_seen: now_ms(), ..Default::default() });
        if !e.addrs.contains(&bare) {
            e.addrs.push(bare);
        }
        drop(b);
        if let Err(e) = swarm.dial(addr.clone()) {
            warn!("dial bootstrap {addr}: {e}");
        }
    }

    let (tx, mut rx) = mpsc::unbounded_channel::<Command>();
    let book2 = book.clone();
    let chainsync2 = chainsync.clone();
    tokio::spawn(async move {
        let book = book2;
        let mut bootstrapped = false;
        loop {
            tokio::select! {
                cmd = rx.recv() => match cmd {
                    Some(Command::Dial(peer, addrs)) => {
                        for a in &addrs {
                            swarm.add_peer_address(peer, a.clone());
                        }
                        let opts = libp2p::swarm::dial_opts::DialOpts::peer_id(peer).addresses(addrs).build();
                        if let Err(e) = swarm.dial(opts) {
                            debug!("dial {peer}: {e}");
                        }
                    }
                    Some(Command::RandomWalk) => {
                        if !bootstrapped {
                            bootstrapped = swarm.behaviour_mut().kad.bootstrap().is_ok();
                        }
                        swarm.behaviour_mut().kad.get_closest_peers(PeerId::random());
                    }
                    None => break,
                },
                ev = swarm.select_next_some() => handle_event(ev, &mut swarm, &book, &chainsync2),
            }
        }
    });

    Ok(Network { local_peer_id, control, book, commands: tx, chainsync })
}

fn learn(book: &PeerBook, peer: PeerId, addrs: impl IntoIterator<Item = Multiaddr>) {
    let mut b = book.lock().unwrap();
    let e = b.entry(peer).or_insert_with(|| PeerEntry { first_seen: now_ms(), ..Default::default() });
    e.last_seen = now_ms();
    for a in addrs {
        let a = strip_p2p(&a);
        if !e.addrs.contains(&a) {
            e.addrs.push(a);
        }
    }
}

fn handle_event(
    ev: SwarmEvent<BehaviourEvent>,
    swarm: &mut libp2p::Swarm<Behaviour>,
    book: &PeerBook,
    chainsync: &StreamProtocol,
) {
    match ev {
        SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
            learn(book, peer_id, []);
            if let Some(e) = book.lock().unwrap().get_mut(&peer_id) {
                e.connected = true;
            }
            debug!("connected {peer_id} via {}", endpoint.get_remote_address());
        }
        SwarmEvent::ConnectionClosed { peer_id, num_established: 0, .. } => {
            if let Some(e) = book.lock().unwrap().get_mut(&peer_id) {
                e.connected = false;
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received { peer_id, info, .. })) => {
            let public: Vec<Multiaddr> = info.listen_addrs.iter().filter(|a| dialable(a)).cloned().collect();
            for a in &public {
                swarm.behaviour_mut().kad.add_address(&peer_id, a.clone());
            }
            learn(book, peer_id, public);
            if let Some(e) = book.lock().unwrap().get_mut(&peer_id) {
                e.agent = Some(info.agent_version.clone());
                e.chainsync = Some(info.protocols.iter().any(|p| p == chainsync));
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Kad(kad::Event::RoutingUpdated { peer, addresses, .. })) => {
            learn(book, peer, addresses.iter().filter(|a| dialable(a)).cloned().collect::<Vec<_>>());
        }
        SwarmEvent::Behaviour(BehaviourEvent::Kad(kad::Event::OutboundQueryProgressed {
            result: kad::QueryResult::GetClosestPeers(Ok(ok)),
            ..
        })) => {
            let n = ok.peers.len();
            for p in ok.peers {
                let addrs: Vec<Multiaddr> = p.addrs.into_iter().filter(dialable).collect();
                for a in &addrs {
                    swarm.behaviour_mut().kad.add_address(&p.peer_id, a.clone());
                }
                learn(book, p.peer_id, addrs);
            }
            debug!("random walk returned {n} peers; book has {}", book.lock().unwrap().len());
        }
        SwarmEvent::NewListenAddr { address, .. } => info!("listening on {address}"),
        _ => {}
    }
}

/// Peers that are worth querying: connected, or with a dialable address, and
/// not known to lack our chain sync protocol.
pub fn candidates(book: &PeerBook, local: &PeerId) -> Vec<(PeerId, Vec<Multiaddr>, bool)> {
    book.lock()
        .unwrap()
        .iter()
        .filter(|(p, e)| *p != local && e.chainsync != Some(false))
        .filter(|(_, e)| e.connected || e.addrs.iter().any(dialable))
        .map(|(p, e)| (*p, e.addrs.iter().filter(|a| dialable(a)).cloned().collect(), e.connected))
        .collect()
}

pub fn distinct_chainsync_mismatch(book: &PeerBook) -> usize {
    book.lock().unwrap().values().filter(|e| e.chainsync == Some(false)).count()
}

pub fn known_peers(book: &PeerBook) -> HashSet<PeerId> {
    book.lock().unwrap().keys().copied().collect()
}
