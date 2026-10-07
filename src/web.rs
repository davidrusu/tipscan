//! HTTP API and the viewer page.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};

use crate::store::{Arrival, Block, Observation, Store, now_ms};

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub genesis_ms: i64,
    pub web_dir: Option<PathBuf>,
    pub local_peer: String,
    pub interval_s: u64,
}

const INDEX: &str = include_str!("../web/index.html");

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/state", get(api_state))
        .route("/api/scans", get(api_scans))
        .route("/api/peers", get(api_peers))
        .route("/api/peer/:id", get(api_peer))
        .route("/api/block/:id/spread", get(api_spread))
        .route("/api/arrivals", get(api_arrivals))
        .with_state(state)
}

async fn index(State(s): State<AppState>) -> Response {
    // Serve from disk when a web dir is given, so the page can be edited live.
    if let Some(dir) = &s.web_dir {
        if let Ok(html) = std::fs::read_to_string(dir.join("index.html")) {
            return ([(header::CACHE_CONTROL, "no-store")], Html(html)).into_response();
        }
    }
    Html(INDEX).into_response()
}

fn err500(e: anyhow::Error) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")).into_response()
}

#[derive(Deserialize)]
struct StateQ {
    hours: Option<f64>,
    scan: Option<i64>,
}

#[derive(Serialize)]
struct TipGroup {
    tip: String,
    height: Option<u64>,
    slot: Option<u64>,
    known: bool,
    peers: Vec<String>,
}

#[derive(Serialize)]
struct StateResp {
    genesis_ms: i64,
    now_slot: i64,
    scan: Option<crate::store::Scan>,
    scan_slot: i64,
    interval_s: u64,
    local_peer: String,
    tips: Vec<TipGroup>,
    errors: BTreeMap<String, Vec<String>>,
    lca: Option<String>,
    blocks: Vec<Block>,
    stored_blocks: usize,
}

async fn api_state(State(s): State<AppState>, Query(q): Query<StateQ>) -> Response {
    let hours = q.hours.unwrap_or(3.0).clamp(0.05, 72.0);
    let run = || -> anyhow::Result<StateResp> {
        let scan = s.store.scan(q.scan)?;
        let obs: Vec<Observation> = match &scan {
            Some(sc) => s.store.observations(sc.id)?,
            None => vec![],
        };
        let scan_ms = scan.as_ref().map_or_else(now_ms, |sc| sc.started);
        let scan_slot = (scan_ms - s.genesis_ms) / 1000;

        let mut groups: HashMap<String, TipGroup> = HashMap::new();
        let mut errors: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for o in &obs {
            match &o.tip {
                Some(t) => {
                    let g = groups.entry(t.clone()).or_insert_with(|| TipGroup {
                        tip: t.clone(),
                        height: o.height,
                        slot: o.slot,
                        known: s.store.has_block(t),
                        peers: vec![],
                    });
                    g.peers.push(o.peer.clone());
                }
                None => errors.entry(o.err.clone().unwrap_or_default()).or_default().push(o.peer.clone()),
            }
        }
        let mut tips: Vec<TipGroup> = groups.into_values().collect();
        tips.sort_by(|a, b| b.peers.len().cmp(&a.peers.len()).then(b.height.cmp(&a.height)));

        // Window by slot, plus every reported tip's path down to the tips' LCA
        // so deep divergences are drawn even when older than the window.
        let cutoff = (scan_slot - (hours * 3600.0) as i64).max(0) as u64;
        let known_tips: Vec<String> = tips.iter().filter(|t| t.known).map(|t| t.tip.clone()).collect();
        let lca = s.store.lca(&known_tips);
        let mut keep: HashSet<String> = HashSet::new();
        {
            let blocks = s.store.blocks.lock().unwrap();
            for b in blocks.values() {
                if b.slot >= cutoff && b.slot <= scan_slot.max(0) as u64 + 60 {
                    keep.insert(b.id.clone());
                }
            }
        }
        if let Some(l) = &lca {
            for t in &known_tips {
                for a in s.store.ancestors(t, 3000) {
                    let stop = &a == l;
                    keep.insert(a);
                    if stop {
                        break;
                    }
                }
            }
        }
        let blocks = {
            let all = s.store.blocks.lock().unwrap();
            keep.iter().filter_map(|id| all.get(id).cloned()).collect()
        };
        Ok(StateResp {
            genesis_ms: s.genesis_ms,
            now_slot: (now_ms() - s.genesis_ms) / 1000,
            scan,
            scan_slot,
            interval_s: s.interval_s,
            local_peer: s.local_peer.clone(),
            tips,
            errors,
            lca,
            blocks,
            stored_blocks: s.store.block_count(),
        })
    };
    match run() {
        Ok(r) => Json(r).into_response(),
        Err(e) => err500(e),
    }
}

#[derive(Deserialize)]
struct HoursQ {
    hours: Option<f64>,
}

async fn api_scans(State(s): State<AppState>, Query(q): Query<HoursQ>) -> Response {
    let since = now_ms() - (q.hours.unwrap_or(24.0) * 3_600_000.0) as i64;
    match s.store.scans_since(since) {
        Ok(v) => Json(v).into_response(),
        Err(e) => err500(e),
    }
}

#[derive(Serialize)]
struct PeerOut {
    #[serde(flatten)]
    row: crate::store::PeerRow,
    last: Option<Observation>,
}

async fn api_peers(State(s): State<AppState>) -> Response {
    let run = || -> anyhow::Result<Vec<PeerOut>> {
        let latest: HashMap<String, Observation> =
            s.store.latest_per_peer()?.into_iter().map(|o| (o.peer.clone(), o)).collect();
        Ok(s.store
            .peers()?
            .into_iter()
            .map(|row| {
                let last = latest.get(&row.peer).cloned();
                PeerOut { row, last }
            })
            .collect())
    };
    match run() {
        Ok(v) => Json(v).into_response(),
        Err(e) => err500(e),
    }
}

#[derive(Deserialize)]
struct SpreadQ {
    /// Seconds after the block's slot to follow it (default 300).
    window: Option<i64>,
}

#[derive(Serialize)]
struct PeerSpread {
    #[serde(flatten)]
    arrival: Arrival,
    ip: Option<String>,
    agent: Option<String>,
}

#[derive(Serialize)]
struct SpreadResp {
    block: Block,
    slot_ms: i64,
    window_s: i64,
    /// True when read from the finalized `arrivals` table.
    stored: bool,
    scan_times: Vec<i64>,
    peers: Vec<PeerSpread>,
}

/// How one block spread: for every peer observed in the window after the
/// block's slot, the interval in which it first had the block on its chain.
/// Finalized blocks come from the `arrivals` table; younger ones are
/// computed from the raw pings.
async fn api_spread(State(s): State<AppState>, Path(id): Path<String>, Query(q): Query<SpreadQ>) -> Response {
    let run = || -> anyhow::Result<Option<SpreadResp>> {
        let Some(block) = s.store.block(&id) else { return Ok(None) };
        let slot_ms = s.genesis_ms + block.slot as i64 * 1000;
        let (window_ms, rows, stored) = match s.store.arrivals(&id)? {
            Some((w, rows)) => (w, rows, true),
            None => {
                let w = q.window.unwrap_or(300).clamp(10, 3600) * 1000;
                (w, s.store.compute_arrivals(&block, slot_ms, w)?, false)
            }
        };
        let scan_times = s.store.scans_between(slot_ms - 60_000, slot_ms + window_ms)?.iter().map(|sc| sc.started).collect();
        let peer_rows: HashMap<String, crate::store::PeerRow> =
            s.store.peers()?.into_iter().map(|p| (p.peer.clone(), p)).collect();
        let mut peers: Vec<PeerSpread> = rows
            .into_iter()
            .map(|a| {
                let row = peer_rows.get(&a.peer);
                PeerSpread {
                    ip: row.and_then(|r| r.addrs.iter().find_map(|x| ip_of(x))),
                    agent: row.and_then(|r| r.agent.clone()),
                    arrival: a,
                }
            })
            .collect();
        peers.sort_by_key(|p| p.arrival.first_with.unwrap_or(i64::MAX));
        Ok(Some(SpreadResp { block, slot_ms, window_s: window_ms / 1000, stored, scan_times, peers }))
    };
    match run() {
        Ok(Some(r)) => Json(r).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "unknown block").into_response(),
        Err(e) => err500(e),
    }
}

#[derive(Serialize)]
struct ArrivalRow {
    block: String,
    height: u64,
    slot: u64,
    #[serde(flatten)]
    arrival: Arrival,
}

/// Every finalized block x peer arrival with slot in the last `hours`.
async fn api_arrivals(State(s): State<AppState>, Query(q): Query<HoursQ>) -> Response {
    let hours = q.hours.unwrap_or(24.0).clamp(0.1, 24.0 * 30.0);
    let since_slot = ((now_ms() - s.genesis_ms) / 1000 - (hours * 3600.0) as i64).max(0) as u64;
    match s.store.arrivals_since(since_slot) {
        Ok(v) => Json(
            v.into_iter()
                .map(|(block, height, slot, arrival)| ArrivalRow { block, height, slot, arrival })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(e) => err500(e),
    }
}

fn ip_of(addr: &str) -> Option<String> {
    let mut parts = addr.split('/');
    while let Some(p) = parts.next() {
        if p == "ip4" || p == "ip6" {
            return parts.next().map(str::to_owned);
        }
    }
    None
}

async fn api_peer(State(s): State<AppState>, Path(id): Path<String>) -> Response {
    match s.store.peer_history(&id, 500) {
        Ok(v) => Json(v).into_response(),
        Err(e) => err500(e),
    }
}
