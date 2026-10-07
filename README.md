# tipscan

Network-wide block tree explorer for the Logos blockchain. It joins the
network as a passive peer, asks every reachable peer for its tip with chain
sync `GetTip`, downloads any branch it hasn't seen, and serves the tree, with
which peers sat on which tip at every scan.

    cargo build --release
    ./target/release/tipscan --data-dir data          # then open http://127.0.0.1:8097/

## How it works

- **Joining.** libp2p over QUIC with identify (`/logos-blockchain/<chain id>/identify`),
  Kademlia in client mode (`/logos-blockchain/<chain id>/kad`) and the chain sync stream
  protocol (`/logos-blockchain/<fork digest>/chainsync`). Defaults are the 0.3.0 testnet
  values. It dials the testnet bootstrap nodes and runs a random Kademlia walk every 20 s,
  adding every peer it learns of (from identify, routing updates and walks) to its peer book.
  It never gossips or serves blocks. Inbound `GetTip` gets `NodeNotOnline`, like a node
  that is still bootstrapping.
- **Scanning.** Every `--interval` seconds (default 60) it sends `GetTip` to every peer that
  is connected or has a public QUIC address, 64 at a time with a 15 s timeout, and records
  each answer or error as an observation of that scan.
- **Building the tree.** For each reported tip not yet stored, it sends `DownloadBlocks{target
  = tip, known = our tip, an anchor 1500 blocks down, a few more ancestors and recent branch
  heads}`. The peer streams from the latest common ancestor it has, at most 1000 blocks per
  request, so one round usually returns exactly the missing branch.
  - A peer on a branch that forked below its own LIB only looks in immutable storage for a
    start block. When it answers `StartBlockNotFound`, the anchor is moved deeper (5000,
    15000 blocks, then genesis).
  - Each round's last block becomes the next anchor, so a deep start walks forward through
    blocks we already have.
  - On first run the whole chain is synced from genesis (about 25 s for 16.7k blocks), so
    heights are exact.
  - Blocks are decoded with the node's own `lb-core` types (parent, slot, leader key,
    transaction count).
- **Arrivals.** Once a block's observation window (`--arrival-window`, default 600 s) has
  closed, a background job stores, for every peer seen in that window, the interval in which
  it first had the block on its chain: `last_without` (last ping without it) and `first_with`
  (first ping with it), judged by ancestry. These rows go in the `arrivals` table, one per
  block x peer, and are kept permanently, so they outlive any pruning of raw pings. Blocks
  from before the first scan have no rows.
- **Storage.** `data/tipscan.sqlite` holds blocks, peers, scans, observations and arrivals;
  `data/node.key` keeps the peer id stable across restarts. Observations are never pruned:
  about 500 rows a minute at the current network size.

## API

| endpoint | returns |
|---|---|
| `/api/state?hours=H&scan=N` | One scan (default: the latest): tips with the peers on each, errors by kind, blocks in the window, plus every tip's path down to the tips' common ancestor |
| `/api/scans?hours=H` | Scan list for the time scrubber |
| `/api/peers` | Every peer seen, its addresses and agent, and its latest observation |
| `/api/peer/<peer id>` | That peer's last 500 observations |
| `/api/block/<id>/spread` | One block's per-peer arrival intervals: stored rows once finalized, computed from raw pings while it is still spreading |
| `/api/arrivals?hours=H` | Every finalized block x peer arrival with slot in the last H hours, for bulk analysis |

## Viewer

`web/index.html` is compiled into the binary; `--web-dir web` serves it from disk instead,
for editing. It shows:

- **The tree**, with each branch drawn wider the more peers are on it, and a badge with
  the peer count at each tip.
- **A scan scrubber**, to see where every peer was at any past scan, with a live mode.
- **Side tabs** for tips (lag, peer count, fork point), peers (searchable, with each peer's
  tip history collapsed into runs) and the clicked block.
- **A banner** when peers sit on a branch that forked more than k=120 blocks below the main
  tip.

## Limits

- **Reach.** Only peers with a public address (or that are connected to us) can be queried.
  NATed nodes that never advertise a dialable address are invisible.
- **Kademlia mode.** In client mode we aren't added to other peers' routing tables, so
  peers don't dial us.
- **Wire types.** The request and block-response envelopes are private in
  `lb-cryptarchia-sync` and are mirrored in `src/wire.rs`. A change to their variant
  order upstream would need the mirror updated.
- **Source dependency.** It builds against the node's own crates, pulled from
  `github.com/logos-blockchain/logos-blockchain` at the commit pinned in `Cargo.toml` (the one the
  0.3.0 testnet runs), with the same toolchain (`rust-toolchain.toml`). Bump `rev` to match another
  network's version.
