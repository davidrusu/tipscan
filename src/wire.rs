//! Chain sync wire format, mirroring `lb-cryptarchia-sync`.
//!
//! Every message is a little-endian `u32` length followed by the node's
//! configured bincode encoding (`lb-binary-codec`). The request and
//! block-response envelopes are private in the sync crate, so they are
//! mirrored here with the same variant order; their payload types are the
//! crate's own.

use anyhow::{Context as _, Result, bail};
use bytes::Bytes;
use futures::{AsyncReadExt as _, AsyncWriteExt as _};
use lb_binary_codec::bincode::{DeserializeOp as _, SerializeOp as _};
use lb_core::{
    block::Block,
    header::HeaderId,
    mantle::{SignedOps, ledger::verification_mode::StandardMode, transactions::states::Unverified},
    proofs::leader_proof::LeaderProof as _,
};
use lb_cryptarchia_sync::{BlocksUnavailableReason, DownloadBlocksRequest, GetTipResponse};
use serde::{Deserialize, Serialize};

/// `lb_cryptarchia_sync::libp2p::messages::RequestMessage`.
#[derive(Debug, Serialize, Deserialize)]
pub enum RequestMessage {
    DownloadBlocksRequest(DownloadBlocksRequest),
    GetTip,
}

/// `lb_cryptarchia_sync::libp2p::messages::DownloadBlocksResponse`.
#[derive(Debug, Serialize, Deserialize)]
pub enum DownloadBlocksResponse {
    Block(Bytes),
    NoMoreBlocks,
    Failure(BlocksUnavailableReason),
}

/// Largest frame accepted from a peer (a full block is well under this).
const MAX_FRAME: usize = 8 << 20;

pub async fn write_msg<W, M>(w: &mut W, msg: &M) -> Result<()>
where
    W: futures::AsyncWrite + Unpin,
    M: Serialize,
{
    let body = msg.to_bytes().context("encode")?;
    w.write_all(&(body.len() as u32).to_le_bytes()).await?;
    w.write_all(&body).await?;
    w.flush().await?;
    Ok(())
}

pub async fn read_msg<R, M>(r: &mut R) -> Result<M>
where
    R: futures::AsyncRead + Unpin,
    M: serde::de::DeserializeOwned,
{
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let len = u32::from_le_bytes(len) as usize;
    if len > MAX_FRAME {
        bail!("frame of {len} bytes exceeds {MAX_FRAME}");
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    M::from_bytes(&buf).context("decode")
}

/// The parts of a block the explorer keeps.
#[derive(Debug, Clone)]
pub struct BlockSummary {
    pub id: HeaderId,
    pub parent: HeaderId,
    pub slot: u64,
    pub leader: String,
    pub txs: u32,
}

pub fn decode_block(bytes: &[u8]) -> Result<BlockSummary> {
    let block: Block<SignedOps<Unverified, StandardMode>> =
        Block::from_bytes(bytes).context("decode block")?;
    let h = block.header();
    Ok(BlockSummary {
        id: h.id(),
        parent: h.parent(),
        slot: u64::from(h.slot()),
        leader: hex::encode(h.leader_proof().leader_key().to_bytes()),
        txs: block.transactions_iter().len() as u32,
    })
}

pub fn tip_of(resp: GetTipResponse) -> std::result::Result<(HeaderId, u64, u64), String> {
    match resp {
        GetTipResponse::Tip { tip, slot, height } => Ok((tip, u64::from(slot), height)),
        GetTipResponse::Failure(reason) => Err(reason.to_string()),
    }
}

pub fn hid(s: &str) -> Result<HeaderId> {
    let b: [u8; 32] = hex::decode(s)?.try_into().map_err(|_| anyhow::anyhow!("header id must be 32 bytes"))?;
    Ok(HeaderId::from(b))
}

pub fn hex_id(id: &HeaderId) -> String {
    hex::encode(<[u8; 32]>::from(*id))
}
