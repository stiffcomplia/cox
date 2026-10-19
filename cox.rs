use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Context, Result};
use jito_protos::bundle::{bundle_result::Result as BundleVerdict, Bundle, BundleResult};
use jito_protos::convert::proto_packet_from_versioned_tx;
use jito_protos::searcher::searcher_service_client::SearcherServiceClient;
use jito_protos::searcher::{
    GetTipAccountsRequest, SendBundleRequest, SubscribeBundleResultsRequest,
};
use jito_searcher_client::{get_searcher_client_auth, token_authenticator::ClientInterceptor};
use solana_sdk::transaction::{Transaction, VersionedTransaction};
use solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer, system_instruction};
use tonic::{service::interceptor::InterceptedService, transport::Channel, Streaming};
use tracing::{debug, error, info, instrument, warn};

pub type SearcherClient = SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>;
type Outcomes = Arc<Mutex<HashMap<String, Outcome>>>;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
const RETRY_BACKOFF: Duration = Duration::from_millis(750);
const BUNDLE_CAP: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Outcome {
    Sent,
    Accepted,
    Rejected,
    Dropped,
    Processed,
    Finalized,
}

pub struct BundleSender {
    client: SearcherClient,
    payer: Arc<Keypair>,
    tips: Vec<Pubkey>,
    cursor: usize,
    outcomes: Outcomes,
}

impl BundleSender {
    #[instrument(skip_all, fields(url = %cfg.block_engine_url))]
    pub async fn connect(cfg: &crate::settings::Jito, payer: Arc<Keypair>) -> Result<Self> {
        let mut client = dial(&cfg.block_engine_url).await?;
        let tips = tip_accounts(&mut client).await?;
        let stream = client
            .subscribe_bundle_results(SubscribeBundleResultsRequest {})
            .await
            .context("subscribe_bundle_results")?
            .into_inner();
        let outcomes: Outcomes = Arc::default();
        tokio::spawn(track(stream, Arc::clone(&outcomes)));
        info!(payer = %payer.pubkey(), tips = tips.len(), "executor ready");
        Ok(Self {
            client,
            payer,
            tips,
            cursor: 0,
            outcomes,
        })
    }

    #[instrument(skip_all, fields(txs = txs.len(), tip))]
    pub async fn submit(&mut self, txs: &[VersionedTransaction], tip: u64) -> Result<String> {
        ensure!(
            txs.len() < BUNDLE_CAP,
            "no slot left for the tip tx (cap {BUNDLE_CAP})"
        );
        let blockhash = *txs
            .first()
            .context("empty bundle")?
            .message
            .recent_blockhash();
        let tip_account = self.tips[self.cursor % self.tips.len()];
        self.cursor = self.cursor.wrapping_add(1);
        let payer = self.payer.pubkey();
        let tip_tx = Transaction::new_signed_with_payer(
            &[system_instruction::transfer(&payer, &tip_account, tip)],
            Some(&payer),
            &[self.payer.as_ref()],
            blockhash,
        );
        let mut full = txs.to_vec();
        full.push(VersionedTransaction::from(tip_tx));
        let bundle = Some(bundle_from(&full));
        let uuid = self
            .client
            .send_bundle(SendBundleRequest { bundle })
            .await
            .context("send_bundle")?
            .into_inner()
            .uuid;
        lock(&self.outcomes).insert(uuid.clone(), Outcome::Sent);
        info!(%uuid, %tip_account, "bundle sent");
        Ok(uuid)
    }

    pub fn land_rate(&self) -> (usize, usize, f64) {
        let map = lock(&self.outcomes);
        let sent = map.len();
        let landed = map.values().filter(|o| **o >= Outcome::Processed).count();
        (sent, landed, landed as f64 / sent.max(1) as f64)
    }
}

async fn track(mut stream: Streaming<BundleResult>, outcomes: Outcomes) {
    loop {
        let res = match stream.message().await {
            Ok(Some(res)) => res,
            Ok(None) => break warn!("stream eof"),
            Err(status) => break error!(error = %status, "lost the result channel"),
        };
        let next = match res.result {
            Some(BundleVerdict::Accepted(_)) => Outcome::Accepted,
            Some(BundleVerdict::Rejected(_)) => Outcome::Rejected,
            Some(BundleVerdict::Dropped(_)) => Outcome::Dropped,
            Some(BundleVerdict::Processed(_)) => Outcome::Processed,
            Some(BundleVerdict::Finalized(_)) => Outcome::Finalized,
            None => continue,
        };
        if let Some(cur) = lock(&outcomes).get_mut(&res.bundle_id) {
            if next > *cur {
                debug!(uuid = %res.bundle_id, from = ?*cur, to = ?next, "bundle outcome");
                *cur = next;
            }
        }
    }
}

fn lock(outcomes: &Outcomes) -> MutexGuard<'_, HashMap<String, Outcome>> {
    outcomes
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn searcher_keypair() -> Result<Keypair> {
    let raw = std::env::var("SEARCHER_KEY").context("SEARCHER_KEY is not set")?;
    let bytes = bs58::decode(raw.trim())
        .into_vec()
        .context("SEARCHER_KEY is not base58")?;
    Keypair::from_bytes(&bytes).map_err(|e| anyhow!("SEARCHER_KEY is not a keypair: {e}"))
}

async fn dial(url: &str) -> Result<SearcherClient> {
    let kp = Arc::new(searcher_keypair()?);
    info!(searcher = %kp.pubkey(), "dialing block engine");
    for attempt in 1..=2u8 {
        match tokio::time::timeout(CONNECT_TIMEOUT, get_searcher_client_auth(url, &kp)).await {
            Ok(Ok(client)) => {
                info!(attempt, "handshake ok");
                return Ok(client);
            }
            Ok(Err(e)) => warn!(attempt, error = %e, "auth/connect failed"),
            Err(_) => warn!(attempt, "connect timed out after {CONNECT_TIMEOUT:?}"),
        }
        if attempt == 1 {
            tokio::time::sleep(RETRY_BACKOFF).await;
        }
    }
    bail!("block engine {url} unavailable after 2 attempts")
}

#[instrument(skip_all)]
async fn tip_accounts(client: &mut SearcherClient) -> Result<Vec<Pubkey>> {
    let accounts = client
        .get_tip_accounts(GetTipAccountsRequest {})
        .await
        .context("get_tip_accounts")?
        .into_inner()
        .accounts
        .iter()
        .map(|s| {
            s.parse()
                .with_context(|| format!("unparseable tip acct {s}"))
        })
        .collect::<Result<Vec<Pubkey>>>()?;
    ensure!(!accounts.is_empty(), "engine gave an empty tip list");
    info!(count = accounts.len(), "tip accounts loaded");
    Ok(accounts)
}

pub fn bundle_from(txs: &[VersionedTransaction]) -> Bundle {
    Bundle {
        header: None,
        packets: txs.iter().map(proto_packet_from_versioned_tx).collect(),
    }
}
