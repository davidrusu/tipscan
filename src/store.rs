//! Persistent state: the block tree, the peers seen, and every GetTip
//! observation grouped into scans. SQLite on disk, with the block tree also
//! held in memory for tree walks.

use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension as _, params};
use serde::Serialize;

pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

#[derive(Debug, Clone, Serialize)]
pub struct Block {
    pub id: String,
    pub parent: String,
    pub slot: u64,
    pub height: u64,
    pub leader: String,
    pub txs: u32,
    /// When this explorer first learned of the block (unix ms).
    pub first_seen: i64,
    /// The peer it was first downloaded from.
    pub first_peer: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Observation {
    pub scan: i64,
    pub peer: String,
    pub t: i64,
    pub tip: Option<String>,
    pub slot: Option<u64>,
    pub height: Option<u64>,
    pub err: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Scan {
    pub id: i64,
    pub started: i64,
    pub finished: Option<i64>,
    pub queried: i64,
    pub ok: i64,
    pub tips: i64,
}

/// When one peer first had one block on its chain.
#[derive(Debug, Clone, Serialize)]
pub struct Arrival {
    pub peer: String,
    /// Latest observation (unix ms) at or after the slot without the block.
    pub last_without: Option<i64>,
    /// First observation (unix ms) with the block on the peer's chain.
    pub first_with: Option<i64>,
    /// Observations of this peer in the window.
    pub seen: u32,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PeerRow {
    pub peer: String,
    pub addrs: Vec<String>,
    pub agent: Option<String>,
    pub chainsync: bool,
    pub first_seen: i64,
    pub last_seen: i64,
}

pub struct Store {
    db: Mutex<Connection>,
    pub blocks: Mutex<HashMap<String, Block>>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Connection::open(path)?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS blocks(
               id TEXT PRIMARY KEY, parent TEXT NOT NULL, slot INTEGER NOT NULL,
               height INTEGER NOT NULL, leader TEXT NOT NULL, txs INTEGER NOT NULL,
               first_seen INTEGER NOT NULL, first_peer TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS peers(
               peer TEXT PRIMARY KEY, addrs TEXT NOT NULL, agent TEXT, chainsync INTEGER NOT NULL,
               first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS scans(
               id INTEGER PRIMARY KEY AUTOINCREMENT, started INTEGER NOT NULL, finished INTEGER,
               queried INTEGER NOT NULL DEFAULT 0, ok INTEGER NOT NULL DEFAULT 0, tips INTEGER NOT NULL DEFAULT 0);
             CREATE TABLE IF NOT EXISTS obs(
               scan INTEGER NOT NULL, peer TEXT NOT NULL, t INTEGER NOT NULL,
               tip TEXT, slot INTEGER, height INTEGER, err TEXT);
             CREATE INDEX IF NOT EXISTS obs_scan ON obs(scan);
             CREATE INDEX IF NOT EXISTS obs_peer ON obs(peer, t);
             -- Per block x peer arrival interval, finalized once the block's
             -- observation window has closed (see Store::compute_arrivals).
             CREATE TABLE IF NOT EXISTS arrivals(
               block TEXT NOT NULL, peer TEXT NOT NULL,
               last_without INTEGER, first_with INTEGER, seen INTEGER NOT NULL,
               PRIMARY KEY(block, peer));
             CREATE INDEX IF NOT EXISTS arrivals_peer ON arrivals(peer);
             CREATE TABLE IF NOT EXISTS arrivals_done(
               block TEXT PRIMARY KEY, computed INTEGER NOT NULL, window_ms INTEGER NOT NULL, peers INTEGER NOT NULL);",
        )?;
        let mut blocks = HashMap::new();
        {
            let mut st = db.prepare("SELECT id,parent,slot,height,leader,txs,first_seen,first_peer FROM blocks")?;
            let rows = st.query_map([], |r| {
                Ok(Block {
                    id: r.get(0)?,
                    parent: r.get(1)?,
                    slot: r.get::<_, i64>(2)? as u64,
                    height: r.get::<_, i64>(3)? as u64,
                    leader: r.get(4)?,
                    txs: r.get(5)?,
                    first_seen: r.get(6)?,
                    first_peer: r.get(7)?,
                })
            })?;
            for b in rows {
                let b = b?;
                blocks.insert(b.id.clone(), b);
            }
        }
        Ok(Self { db: Mutex::new(db), blocks: Mutex::new(blocks) })
    }

    pub fn has_block(&self, id: &str) -> bool {
        self.blocks.lock().unwrap().contains_key(id)
    }

    pub fn block(&self, id: &str) -> Option<Block> {
        self.blocks.lock().unwrap().get(id).cloned()
    }

    pub fn block_count(&self) -> usize {
        self.blocks.lock().unwrap().len()
    }

    /// Inserts blocks in parent-to-child order. Each block's parent must
    /// already be stored (or be earlier in `batch`); height is derived from it.
    /// Returns how many were new.
    pub fn insert_blocks(&self, batch: &[(String, String, u64, String, u32)], peer: &str) -> Result<usize> {
        let t = now_ms();
        let mut blocks = self.blocks.lock().unwrap();
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let mut added = 0;
        for (id, parent, slot, leader, txs) in batch {
            if blocks.contains_key(id) {
                continue;
            }
            let Some(height) = blocks.get(parent).map(|p| p.height + 1) else {
                anyhow::bail!("parent {} of {} is unknown", &parent[..10], &id[..10]);
            };
            let b = Block {
                id: id.clone(),
                parent: parent.clone(),
                slot: *slot,
                height,
                leader: leader.clone(),
                txs: *txs,
                first_seen: t,
                first_peer: peer.to_owned(),
            };
            tx.execute(
                "INSERT OR IGNORE INTO blocks VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                params![b.id, b.parent, b.slot as i64, b.height as i64, b.leader, b.txs, b.first_seen, b.first_peer],
            )?;
            blocks.insert(id.clone(), b);
            added += 1;
        }
        tx.commit()?;
        Ok(added)
    }

    /// Stores the genesis block as the root of the tree.
    pub fn ensure_genesis(&self, id: &str) -> Result<()> {
        if self.has_block(id) {
            return Ok(());
        }
        let b = Block {
            id: id.to_owned(),
            parent: String::new(),
            slot: 0,
            height: 0,
            leader: String::new(),
            txs: 0,
            first_seen: now_ms(),
            first_peer: String::new(),
        };
        self.db.lock().unwrap().execute(
            "INSERT OR IGNORE INTO blocks VALUES (?1,?2,0,0,'',0,?3,'')",
            params![b.id, b.parent, b.first_seen],
        )?;
        self.blocks.lock().unwrap().insert(id.to_owned(), b);
        Ok(())
    }

    /// The highest block (ties: lowest slot), used as our local tip.
    pub fn best(&self) -> Option<Block> {
        self.blocks
            .lock()
            .unwrap()
            .values()
            .max_by(|a, b| a.height.cmp(&b.height).then(b.slot.cmp(&a.slot)))
            .cloned()
    }

    /// Ancestors of `id`, newest first, including `id`, at most `limit`.
    pub fn ancestors(&self, id: &str, limit: usize) -> Vec<String> {
        let blocks = self.blocks.lock().unwrap();
        let mut out = Vec::new();
        let mut cur = id.to_owned();
        while out.len() < limit {
            let Some(b) = blocks.get(&cur) else { break };
            out.push(cur.clone());
            if b.parent.is_empty() {
                break;
            }
            cur = b.parent.clone();
        }
        out
    }

    /// The block `depth` blocks below `id` on its chain (or the oldest known).
    pub fn ancestor_at_depth(&self, id: &str, depth: usize) -> Option<String> {
        self.ancestors(id, depth + 1).last().cloned()
    }

    /// Lowest common ancestor of a set of blocks.
    pub fn lca(&self, ids: &[String]) -> Option<String> {
        let mut iter = ids.iter().filter(|i| self.has_block(i));
        let first = iter.next()?;
        let mut common: Vec<String> = self.ancestors(first, usize::MAX);
        for id in iter {
            let set: HashSet<String> = self.ancestors(id, usize::MAX).into_iter().collect();
            common.retain(|a| set.contains(a));
        }
        common.into_iter().next()
    }

    pub fn upsert_peer(&self, p: &PeerRow) -> Result<()> {
        self.db.lock().unwrap().execute(
            "INSERT INTO peers VALUES (?1,?2,?3,?4,?5,?6)
             ON CONFLICT(peer) DO UPDATE SET addrs=excluded.addrs,
               agent=COALESCE(excluded.agent, peers.agent),
               chainsync=MAX(peers.chainsync, excluded.chainsync), last_seen=excluded.last_seen",
            params![p.peer, serde_json::to_string(&p.addrs)?, p.agent, p.chainsync as i64, p.first_seen, p.last_seen],
        )?;
        Ok(())
    }

    pub fn peers(&self) -> Result<Vec<PeerRow>> {
        let db = self.db.lock().unwrap();
        let mut st = db.prepare("SELECT peer,addrs,agent,chainsync,first_seen,last_seen FROM peers")?;
        let rows = st.query_map([], |r| {
            Ok(PeerRow {
                peer: r.get(0)?,
                addrs: serde_json::from_str(&r.get::<_, String>(1)?).unwrap_or_default(),
                agent: r.get(2)?,
                chainsync: r.get::<_, i64>(3)? != 0,
                first_seen: r.get(4)?,
                last_seen: r.get(5)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn start_scan(&self) -> Result<i64> {
        let db = self.db.lock().unwrap();
        db.execute("INSERT INTO scans(started) VALUES (?1)", params![now_ms()])?;
        Ok(db.last_insert_rowid())
    }

    pub fn finish_scan(&self, scan: i64, obs: &[Observation]) -> Result<()> {
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        for o in obs {
            tx.execute(
                "INSERT INTO obs VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![o.scan, o.peer, o.t, o.tip, o.slot.map(|v| v as i64), o.height.map(|v| v as i64), o.err],
            )?;
        }
        let ok = obs.iter().filter(|o| o.tip.is_some()).count() as i64;
        let tips = obs.iter().filter_map(|o| o.tip.as_ref()).collect::<HashSet<_>>().len() as i64;
        tx.execute(
            "UPDATE scans SET finished=?2, queried=?3, ok=?4, tips=?5 WHERE id=?1",
            params![scan, now_ms(), obs.len() as i64, ok, tips],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn scans_since(&self, since_ms: i64) -> Result<Vec<Scan>> {
        let db = self.db.lock().unwrap();
        let mut st = db.prepare(
            "SELECT id,started,finished,queried,ok,tips FROM scans WHERE started>=?1 AND finished IS NOT NULL ORDER BY id",
        )?;
        let rows = st.query_map(params![since_ms], |r| {
            Ok(Scan { id: r.get(0)?, started: r.get(1)?, finished: r.get(2)?, queried: r.get(3)?, ok: r.get(4)?, tips: r.get(5)? })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Whether `ancestor` (at `ancestor_height`) is `tip` or one of its
    /// ancestors. Walks down from `tip` to that height.
    pub fn descends(&self, tip: &str, ancestor: &str, ancestor_height: u64) -> bool {
        let blocks = self.blocks.lock().unwrap();
        let mut cur = tip;
        loop {
            let Some(b) = blocks.get(cur) else { return false };
            if b.height <= ancestor_height {
                return b.height == ancestor_height && cur == ancestor;
            }
            cur = &b.parent;
        }
    }

    /// For every peer observed in [slot_ms, slot_ms + window_ms], the
    /// interval in which it first had `block` on its chain (by ancestry).
    pub fn compute_arrivals(&self, block: &Block, slot_ms: i64, window_ms: i64) -> Result<Vec<Arrival>> {
        let end = slot_ms + window_ms;
        let mut per: HashMap<String, Vec<(i64, bool)>> = HashMap::new();
        for sc in self.scans_between(slot_ms - 60_000, end)? {
            for o in self.observations(sc.id)? {
                if o.t < slot_ms || o.t > end {
                    continue;
                }
                let has = o.tip.as_deref().is_some_and(|t| self.descends(t, &block.id, block.height));
                per.entry(o.peer).or_default().push((o.t, has));
            }
        }
        Ok(per
            .into_iter()
            .map(|(peer, mut v)| {
                v.sort_unstable();
                let first = v.iter().position(|(_, h)| *h);
                Arrival {
                    first_with: first.map(|i| v[i].0),
                    last_without: v[..first.unwrap_or(v.len())].iter().rev().find(|(_, h)| !h).map(|(t, _)| *t),
                    seen: v.len() as u32,
                    peer,
                }
            })
            .collect())
    }

    /// Blocks whose observation window has closed but whose arrivals are not
    /// stored yet, lowest first. Blocks whose window ended before `first_scan_ms`
    /// were never observed and are skipped.
    pub fn arrivals_pending(
        &self,
        genesis_ms: i64,
        window_ms: i64,
        first_scan_ms: i64,
        now_ms: i64,
        limit: usize,
    ) -> Result<Vec<Block>> {
        let done: HashSet<String> = {
            let db = self.db.lock().unwrap();
            let mut st = db.prepare("SELECT block FROM arrivals_done")?;
            st.query_map([], |r| r.get(0))?.collect::<Result<_, _>>()?
        };
        let blocks = self.blocks.lock().unwrap();
        let mut v: Vec<Block> = blocks
            .values()
            .filter(|b| {
                let slot_ms = genesis_ms + b.slot as i64 * 1000;
                !b.parent.is_empty()
                    && slot_ms + window_ms < now_ms
                    && slot_ms + window_ms >= first_scan_ms
                    && !done.contains(&b.id)
            })
            .cloned()
            .collect();
        v.sort_by_key(|b| (b.height, b.slot));
        v.truncate(limit);
        Ok(v)
    }

    pub fn save_arrivals(&self, block: &str, window_ms: i64, rows: &[Arrival]) -> Result<()> {
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        for a in rows {
            tx.execute(
                "INSERT OR REPLACE INTO arrivals VALUES (?1,?2,?3,?4,?5)",
                params![block, a.peer, a.last_without, a.first_with, a.seen],
            )?;
        }
        tx.execute(
            "INSERT OR REPLACE INTO arrivals_done VALUES (?1,?2,?3,?4)",
            params![block, now_ms(), window_ms, rows.len() as i64],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Stored arrivals of `block`, or None if not finalized yet.
    pub fn arrivals(&self, block: &str) -> Result<Option<(i64, Vec<Arrival>)>> {
        let db = self.db.lock().unwrap();
        let Some(window_ms) = db
            .query_row("SELECT window_ms FROM arrivals_done WHERE block=?1", params![block], |r| r.get::<_, i64>(0))
            .optional()?
        else {
            return Ok(None);
        };
        let mut st = db.prepare("SELECT peer,last_without,first_with,seen FROM arrivals WHERE block=?1")?;
        let rows = st
            .query_map(params![block], |r| {
                Ok(Arrival { peer: r.get(0)?, last_without: r.get(1)?, first_with: r.get(2)?, seen: r.get(3)? })
            })?
            .collect::<Result<_, _>>()?;
        Ok(Some((window_ms, rows)))
    }

    /// Every stored arrival of blocks with slot >= `since_slot`, for bulk
    /// analysis: (block, height, slot, arrival).
    pub fn arrivals_since(&self, since_slot: u64) -> Result<Vec<(String, u64, u64, Arrival)>> {
        let rows: Vec<(String, Arrival)> = {
            let db = self.db.lock().unwrap();
            let mut st = db.prepare("SELECT block,peer,last_without,first_with,seen FROM arrivals")?;
            st.query_map([], |r| {
                Ok((r.get(0)?, Arrival { peer: r.get(1)?, last_without: r.get(2)?, first_with: r.get(3)?, seen: r.get(4)? }))
            })?
            .collect::<Result<_, _>>()?
        };
        let blocks = self.blocks.lock().unwrap();
        Ok(rows
            .into_iter()
            .filter_map(|(b, a)| {
                let blk = blocks.get(&b)?;
                (blk.slot >= since_slot).then(|| (b, blk.height, blk.slot, a))
            })
            .collect())
    }

    /// Finished scans that started in [from_ms, to_ms], oldest first.
    pub fn scans_between(&self, from_ms: i64, to_ms: i64) -> Result<Vec<Scan>> {
        let db = self.db.lock().unwrap();
        let mut st = db.prepare(
            "SELECT id,started,finished,queried,ok,tips FROM scans
             WHERE started>=?1 AND started<=?2 AND finished IS NOT NULL ORDER BY id",
        )?;
        let rows = st.query_map(params![from_ms, to_ms], |r| {
            Ok(Scan { id: r.get(0)?, started: r.get(1)?, finished: r.get(2)?, queried: r.get(3)?, ok: r.get(4)?, tips: r.get(5)? })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn first_scan_ms(&self) -> Result<Option<i64>> {
        Ok(self.db.lock().unwrap().query_row("SELECT MIN(started) FROM scans", [], |r| r.get(0))?)
    }

    pub fn scan(&self, id: Option<i64>) -> Result<Option<Scan>> {
        let db = self.db.lock().unwrap();
        let sql = "SELECT id,started,finished,queried,ok,tips FROM scans WHERE finished IS NOT NULL AND (?1 IS NULL OR id=?1) ORDER BY id DESC LIMIT 1";
        Ok(db
            .query_row(sql, params![id], |r| {
                Ok(Scan { id: r.get(0)?, started: r.get(1)?, finished: r.get(2)?, queried: r.get(3)?, ok: r.get(4)?, tips: r.get(5)? })
            })
            .optional()?)
    }

    pub fn observations(&self, scan: i64) -> Result<Vec<Observation>> {
        self.obs_query("SELECT scan,peer,t,tip,slot,height,err FROM obs WHERE scan=?1", params![scan])
    }

    pub fn peer_history(&self, peer: &str, limit: i64) -> Result<Vec<Observation>> {
        self.obs_query(
            "SELECT scan,peer,t,tip,slot,height,err FROM obs WHERE peer=?1 ORDER BY t DESC LIMIT ?2",
            params![peer, limit],
        )
    }

    /// Latest observation of every peer.
    pub fn latest_per_peer(&self) -> Result<Vec<Observation>> {
        self.obs_query(
            "SELECT o.scan,o.peer,o.t,o.tip,o.slot,o.height,o.err FROM obs o
             JOIN (SELECT peer, MAX(t) mt FROM obs GROUP BY peer) l ON o.peer=l.peer AND o.t=l.mt",
            [],
        )
    }

    fn obs_query<P: rusqlite::Params>(&self, sql: &str, p: P) -> Result<Vec<Observation>> {
        let db = self.db.lock().unwrap();
        let mut st = db.prepare(sql)?;
        let rows = st.query_map(p, |r| {
            Ok(Observation {
                scan: r.get(0)?,
                peer: r.get(1)?,
                t: r.get(2)?,
                tip: r.get(3)?,
                slot: r.get::<_, Option<i64>>(4)?.map(|v| v as u64),
                height: r.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                err: r.get(6)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }
}
