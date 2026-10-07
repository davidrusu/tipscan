//! The scan loop: ask every reachable peer for its tip, then download any
//! branch we have not seen, back to where it joins our tree.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context as _, Result, anyhow, bail};
use futures::{StreamExt as _, stream};
use lb_cryptarchia_sync::{DownloadBlocksRequest, GetTipResponse};
use libp2p::PeerId;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::{
    p2p::{self, Command, Network},
    store::{Observation, PeerRow, Store, now_ms},
    wire::{DownloadBlocksResponse, RequestMessage, decode_block, hex_id, hid, read_msg, tip_of, write_msg},
};

pub struct Scanner {
    pub net: Network,
    pub store: Arc<Store>,
    pub genesis: String,
    pub genesis_ms: i64,
    /// How long after a block's slot its arrivals are followed before they
    /// are finalized into the `arrivals` table.
    pub arrival_window: Duration,
    pub interval: Duration,
    pub concurrency: usize,
    pub request_timeout: Duration,
}

impl Scanner {
    pub async fn run(self) {
        // Give bootstrap connections and the first DHT walks a head start.
        for _ in 0..3 {
            let _ = self.net.commands.send(Command::RandomWalk);
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        // Finalizing arrivals reads every ping in a block's window, so it runs
        // on its own timer instead of delaying scans.
        let store = self.store.clone();
        let (genesis_ms, window) = (self.genesis_ms, self.arrival_window);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(15));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if let Err(e) = finalize_arrivals(store.clone(), genesis_ms, window, 50).await {
                    warn!("finalizing arrivals failed: {e:#}");
                }
            }
        });
        let mut walk = tokio::time::interval(Duration::from_secs(20));
        let mut scan = tokio::time::interval(self.interval);
        // A scan longer than the interval delays the next one rather than
        // triggering a burst of catch-up scans.
        scan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = walk.tick() => { let _ = self.net.commands.send(Command::RandomWalk); }
                _ = scan.tick() => {
                    if let Err(e) = self.scan_once().await {
                        warn!("scan failed: {e:#}");
                    }
                }
            }
        }
    }

    async fn scan_once(&self) -> Result<()> {
        let started = std::time::Instant::now();
        let scan = self.store.start_scan()?;
        let peers = p2p::candidates(&self.net.book, &self.net.local_peer_id);
        for (peer, addrs, connected) in &peers {
            if !connected && !addrs.is_empty() {
                let _ = self.net.commands.send(Command::Dial(*peer, addrs.clone()));
            }
        }
        self.persist_peers()?;

        let obs: Vec<Observation> = stream::iter(peers.into_iter().map(|(peer, _, _)| peer))
            .map(|peer| {
                let mut control = self.net.control.clone();
                let proto = self.net.chainsync.clone();
                let t = self.request_timeout;
                async move {
                    let res = timeout(t, get_tip(&mut control, peer, proto)).await;
                    let mut o = Observation { scan, peer: peer.to_string(), t: now_ms(), tip: None, slot: None, height: None, err: None };
                    match res {
                        Ok(Ok(Ok((tip, slot, height)))) => {
                            o.tip = Some(hex_id(&tip));
                            o.slot = Some(slot);
                            o.height = Some(height);
                        }
                        Ok(Ok(Err(reason))) => o.err = Some(reason),
                        Ok(Err(e)) => o.err = Some(short_err(&e)),
                        Err(_) => o.err = Some("timeout".into()),
                    }
                    o
                }
            })
            .buffer_unordered(self.concurrency)
            .collect()
            .await;

        // Fill in every reported tip we do not have, trying each peer that
        // reported it until one serves the branch.
        let mut by_tip: HashMap<String, Vec<&Observation>> = HashMap::new();
        for o in &obs {
            if let Some(t) = &o.tip {
                by_tip.entry(t.clone()).or_default().push(o);
            }
        }
        let mut unknown: Vec<_> = by_tip.iter().filter(|(t, _)| !self.store.has_block(t)).collect();
        unknown.sort_by_key(|(_, os)| std::cmp::Reverse(os.len()));
        let mut filled = 0;
        for (tip, reporters) in unknown {
            for o in reporters.iter().take(3) {
                let peer: PeerId = o.peer.parse()?;
                match self.backfill(peer, tip).await {
                    Ok(n) => {
                        filled += n;
                        break;
                    }
                    Err(e) => debug!("backfill {} from {}: {e:#}", &tip[..10], short_peer(&o.peer)),
                }
            }
        }

        self.store.finish_scan(scan, &obs)?;
        let ok = obs.iter().filter(|o| o.tip.is_some()).count();
        let mut errs: HashMap<&str, usize> = HashMap::new();
        for o in &obs {
            if let Some(e) = &o.err {
                *errs.entry(e.as_str()).or_default() += 1;
            }
        }
        // Separate statements: a `lock()` temporary lives to the end of its
        // statement, so two in one `info!` would deadlock the peer book.
        let book_len = self.net.book.lock().unwrap().len();
        let other_fork = p2p::distinct_chainsync_mismatch(&self.net.book);
        info!(
            "scan {scan}: {ok}/{} peers answered, {} distinct tips, {filled} new blocks, {} blocks stored, book {book_len} peers ({other_fork} other-fork); errors {errs:?}; {:.1}s",
            obs.len(),
            by_tip.len(),
            self.store.block_count(),
            started.elapsed().as_secs_f32(),
        );
        Ok(())
    }

    fn persist_peers(&self) -> Result<()> {
        let snapshot: Vec<_> = self.net.book.lock().unwrap().iter().map(|(p, e)| (*p, e.clone())).collect();
        for (p, e) in snapshot {
            self.store.upsert_peer(&PeerRow {
                peer: p.to_string(),
                addrs: e.addrs.iter().map(ToString::to_string).collect(),
                agent: e.agent,
                chainsync: e.chainsync.unwrap_or(false),
                first_seen: e.first_seen,
                last_seen: e.last_seen.max(e.first_seen),
            })?;
        }
        Ok(())
    }

    /// Downloads the branch ending at `tip` from `peer` until it connects to
    /// the stored tree. Returns the number of new blocks.
    ///
    /// The peer streams from the latest of our known blocks it has, at most
    /// 1000 blocks per request. Each round the last block it sent becomes our
    /// anchor, so a walk that starts deep (or at genesis) keeps advancing
    /// through blocks we already have until it reaches the new branch.
    async fn backfill(&self, peer: PeerId, tip: &str) -> Result<usize> {
        let target = hid(tip)?;
        let mut total = 0;
        // A peer on a branch that forked below its own LIB only searches its
        // immutable storage for a start block, so the anchor must predate the
        // fork: start 1500 blocks down and deepen on StartBlockNotFound.
        let anchors = [1_500usize, 5_000, 15_000, usize::MAX];
        let mut anchor_idx = 0;
        let mut cursor: Option<String> = None;
        for _round in 0..200 {
            if self.store.has_block(tip) {
                return Ok(total);
            }
            let mut known = self.known_blocks(anchors[anchor_idx]);
            if let Some(c) = &cursor {
                known.retain(|k| k != c);
                known.insert(1.min(known.len()), c.clone());
            }
            let mut iter = known.iter();
            let local_tip = hid(iter.next().unwrap())?;
            let immutable = hid(iter.next().unwrap_or(&self.genesis))?;
            let additional = iter.take(5).map(|s| hid(s)).collect::<Result<HashSet<_>>>()?;
            let req = DownloadBlocksRequest::new(target, local_tip, immutable, additional);

            let mut control = self.net.control.clone();
            let mut s = timeout(self.request_timeout, control.open_stream(peer, self.net.chainsync.clone()))
                .await
                .map_err(|_| anyhow!("open timeout"))?
                .map_err(|e| anyhow!("open: {e}"))?;
            write_msg(&mut s, &RequestMessage::DownloadBlocksRequest(req)).await?;

            let mut batch = Vec::new();
            let mut deepen = false;
            loop {
                let msg = timeout(self.request_timeout, read_msg::<_, DownloadBlocksResponse>(&mut s))
                    .await
                    .map_err(|_| anyhow!("read timeout after {} blocks", batch.len()))??;
                match msg {
                    DownloadBlocksResponse::Block(bytes) => {
                        let b = decode_block(&bytes)?;
                        batch.push((hex_id(&b.id), hex_id(&b.parent), b.slot, b.leader, b.txs));
                    }
                    DownloadBlocksResponse::NoMoreBlocks => break,
                    DownloadBlocksResponse::Failure(lb_cryptarchia_sync::BlocksUnavailableReason::StartBlockNotFound)
                        if cursor.is_none() && anchor_idx + 1 < anchors.len() =>
                    {
                        deepen = true;
                        break;
                    }
                    DownloadBlocksResponse::Failure(r) => bail!("peer refused: {r}"),
                }
            }
            if deepen {
                anchor_idx += 1;
                debug!("{}: start block not found, anchoring {} deep", short_peer(&peer.to_string()), anchors[anchor_idx]);
                continue;
            }
            let Some(last) = batch.last().map(|b| b.0.clone()) else {
                bail!("peer returned no blocks");
            };
            if cursor.as_deref() == Some(last.as_str()) {
                bail!("no progress");
            }
            let n = self.store.insert_blocks(&batch, &peer.to_string()).context("insert")?;
            total += n;
            cursor = Some(last);
            if batch.len() >= 1000 || n > 100 {
                info!("backfill from {}: +{n} new of {} streamed (now {} stored)", short_peer(&peer.to_string()), batch.len(), self.store.block_count());
            }
        }
        bail!("gave up after 200 rounds")
    }

    /// Our "known blocks" for a download request: local tip, an immutable
    /// anchor `anchor_depth` blocks down our best chain (genesis if the chain
    /// is shorter), then up to five more blocks so a peer on another branch
    /// can start from the latest common ancestor it has.
    fn known_blocks(&self, anchor_depth: usize) -> Vec<String> {
        let Some(best) = self.store.best() else { return vec![self.genesis.clone()] };
        let mut out = vec![best.id.clone()];
        let anchor = if anchor_depth == usize::MAX {
            self.genesis.clone()
        } else {
            self.store.ancestor_at_depth(&best.id, anchor_depth).unwrap_or_else(|| self.genesis.clone())
        };
        out.push(anchor);
        for d in [20, 150, 500] {
            if let Some(a) = self.store.ancestor_at_depth(&best.id, d) {
                out.push(a);
            }
        }
        // Recently seen heads of other branches.
        let mut heads: Vec<_> = {
            let blocks = self.store.blocks.lock().unwrap();
            let parents: HashSet<&str> = blocks.values().map(|b| b.parent.as_str()).collect();
            blocks
                .values()
                .filter(|b| !parents.contains(b.id.as_str()) && b.id != best.id)
                .map(|b| (b.first_seen, b.id.clone()))
                .collect()
        };
        heads.sort();
        out.extend(heads.into_iter().rev().take(2).map(|(_, id)| id));
        let mut seen = HashSet::new();
        out.retain(|id| seen.insert(id.clone()));
        out.truncate(7);
        out
    }
}

async fn get_tip(
    control: &mut libp2p_stream::Control,
    peer: PeerId,
    proto: libp2p::StreamProtocol,
) -> Result<std::result::Result<(lb_core::header::HeaderId, u64, u64), String>> {
    let mut s = control.open_stream(peer, proto).await.map_err(|e| anyhow!("open: {e}"))?;
    write_msg(&mut s, &RequestMessage::GetTip).await?;
    let resp: GetTipResponse = read_msg(&mut s).await?;
    Ok(tip_of(resp))
}

fn short_err(e: &anyhow::Error) -> String {
    let s = format!("{e}");
    if s.contains("UnsupportedProtocol") || s.contains("unsupported") {
        "unsupported protocol".into()
    } else if s.starts_with("open") {
        "unreachable".into()
    } else {
        s.chars().take(60).collect()
    }
}

pub fn short_peer(p: &str) -> &str {
    &p[p.len().saturating_sub(8)..]
}

/// Stores the per-peer arrival intervals of up to `limit` blocks whose
/// observation window has closed, so they outlive the raw pings.
async fn finalize_arrivals(store: Arc<Store>, genesis_ms: i64, window: Duration, limit: usize) -> Result<()> {
    let window_ms = window.as_millis() as i64;
    let n = tokio::task::spawn_blocking(move || -> Result<usize> {
        let Some(first_scan) = store.first_scan_ms()? else { return Ok(0) };
        let pending = store.arrivals_pending(genesis_ms, window_ms, first_scan, now_ms(), limit)?;
        for b in &pending {
            let rows = store.compute_arrivals(b, genesis_ms + b.slot as i64 * 1000, window_ms)?;
            store.save_arrivals(&b.id, window_ms, &rows)?;
        }
        Ok(pending.len())
    })
    .await??;
    if n > 0 {
        debug!("finalized arrivals of {n} blocks");
    }
    Ok(())
}
