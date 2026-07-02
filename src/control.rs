use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    config::ControlConfig,
    shadow::ShadowLedger,
    types::{MarketTrade, PositionState, ResolvedFollowContext},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FollowerAccount {
    pub id: String,
    pub label: String,
    pub address: String,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceAddress {
    pub id: String,
    pub label: String,
    pub address: String,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyStrategy {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub follower_account_id: String,
    pub source_address_id: String,
    pub market_selectors: Vec<String>,
    pub size_multiplier_bps: u32,
    pub min_order_size: i64,
    pub maker_slippage_bps: u32,
    pub taker_slippage_bps: u32,
    pub activation_pnl_bps: i64,
    pub trailing_drawdown_bps: u32,
    pub allow_taker_on_sweep: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketRegistration {
    pub id: String,
    pub label: String,
    pub base_asset: String,
    pub timeframe: String,
    pub enabled: bool,
    pub condition_id: Option<String>,
    pub token_ids: Vec<String>,
    pub slug: Option<String>,
    pub question: Option<String>,
    pub selectors: Vec<String>,
    pub source: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlPlaneState {
    pub follower_accounts: Vec<FollowerAccount>,
    pub source_addresses: Vec<SourceAddress>,
    pub strategies: Vec<CopyStrategy>,
    pub markets: Vec<MarketRegistration>,
}

impl Default for ControlPlaneState {
    fn default() -> Self {
        Self {
            follower_accounts: vec![FollowerAccount {
                id: "follower-demo".to_string(),
                label: "demo-follower".to_string(),
                address: "0x0000000000000000000000000000000000000000".to_string(),
                enabled: true,
                created_at: Utc::now(),
            }],
            source_addresses: vec![SourceAddress {
                id: "source-btc5m".to_string(),
                label: "btc-5m-target".to_string(),
                address: "0xf6ab262d360c85f98782b84a04db35c6e18d3579".to_string(),
                enabled: true,
                created_at: Utc::now(),
            }],
            strategies: vec![CopyStrategy {
                id: "strategy-btc5m-default".to_string(),
                name: "BTC 5min Default".to_string(),
                enabled: true,
                follower_account_id: "follower-demo".to_string(),
                source_address_id: "source-btc5m".to_string(),
                market_selectors: vec!["btc".to_string(), "5min".to_string()],
                size_multiplier_bps: 10_000,
                min_order_size: 5,
                maker_slippage_bps: 200,
                taker_slippage_bps: 350,
                activation_pnl_bps: 3_000,
                trailing_drawdown_bps: 1_000,
                allow_taker_on_sweep: true,
                created_at: Utc::now(),
            }],
            markets: vec![MarketRegistration {
                id: "btc-5min".to_string(),
                label: "BTC 5min".to_string(),
                base_asset: "BTC".to_string(),
                timeframe: "5min".to_string(),
                enabled: true,
                condition_id: None,
                token_ids: Vec::new(),
                slug: None,
                question: None,
                selectors: vec!["btc".to_string(), "bitcoin".to_string(), "5min".to_string(), "5 min".to_string()],
                source: "bootstrap".to_string(),
                updated_at: Utc::now(),
            }],
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardOverview {
    pub follower_accounts: Vec<FollowerAccount>,
    pub source_addresses: Vec<SourceAddress>,
    pub strategies: Vec<CopyStrategy>,
    pub markets: Vec<MarketRegistration>,
    pub positions: Vec<PositionState>,
}

#[derive(Debug)]
pub struct ControlPlane {
    path: PathBuf,
    state: Arc<RwLock<ControlPlaneState>>,
    next_id: AtomicU64,
}

impl ControlPlane {
    pub fn load(config: &ControlConfig) -> Result<Self> {
        let path = PathBuf::from(&config.state_file);
        let state = if path.exists() {
            let raw = fs::read_to_string(&path).with_context(|| format!("read control plane {}", path.display()))?;
            serde_json::from_str(&raw).with_context(|| format!("parse control plane {}", path.display()))?
        } else {
            let state = ControlPlaneState::default();
            persist_state(&path, &state)?;
            state
        };

        Ok(Self {
            path,
            state: Arc::new(RwLock::new(state)),
            next_id: AtomicU64::new(1),
        })
    }

    pub async fn snapshot(&self) -> ControlPlaneState {
        self.state.read().await.clone()
    }

    pub async fn resolve_follow_context(&self, trade: &MarketTrade) -> Option<ResolvedFollowContext> {
        let state = self.state.read().await;
        let market = state.markets.iter().find(|market| market.enabled && market_matches_trade(market, trade))?;
        let source = state
            .source_addresses
            .iter()
            .find(|source| source.enabled && trade.touches_address(&source.address))?;
        let strategy = state.strategies.iter().find(|strategy| {
            strategy.enabled
                && strategy.source_address_id == source.id
                && strategy.market_selectors.iter().all(|selector| market_matches_selector(market, trade, selector))
                && state
                    .follower_accounts
                    .iter()
                    .any(|account| account.enabled && account.id == strategy.follower_account_id)
        })?;

        Some(ResolvedFollowContext {
            follower_account_id: strategy.follower_account_id.clone(),
            source_address_id: source.id.clone(),
            strategy_id: strategy.id.clone(),
            state_key: format!(
                "{}:{}:{}:{}",
                strategy.follower_account_id, source.id, trade.market_id, trade.asset_id
            ),
            size_multiplier_bps: strategy.size_multiplier_bps,
            min_order_size: strategy.min_order_size,
            maker_slippage_bps: strategy.maker_slippage_bps,
            taker_slippage_bps: strategy.taker_slippage_bps,
            activation_pnl_bps: strategy.activation_pnl_bps,
            trailing_drawdown_bps: strategy.trailing_drawdown_bps,
            allow_taker_on_sweep: strategy.allow_taker_on_sweep,
        })
    }

    pub async fn upsert_market(&self, market: MarketRegistration) -> Result<()> {
        let mut state = self.state.write().await;
        if let Some(existing) = state.markets.iter_mut().find(|item| item.id == market.id) {
            *existing = market;
        } else {
            state.markets.push(market);
        }
        persist_state(&self.path, &state)?;
        Ok(())
    }

    pub async fn add_follower_account(&self, label: String, address: String) -> Result<FollowerAccount> {
        let mut state = self.state.write().await;
        let account = FollowerAccount {
            id: self.next("follower"),
            label,
            address: normalize_address(&address),
            enabled: true,
            created_at: Utc::now(),
        };
        state.follower_accounts.push(account.clone());
        persist_state(&self.path, &state)?;
        Ok(account)
    }

    pub async fn add_source_address(&self, label: String, address: String) -> Result<SourceAddress> {
        let mut state = self.state.write().await;
        let source = SourceAddress {
            id: self.next("source"),
            label,
            address: normalize_address(&address),
            enabled: true,
            created_at: Utc::now(),
        };
        state.source_addresses.push(source.clone());
        persist_state(&self.path, &state)?;
        Ok(source)
    }

    pub async fn add_strategy(&self, strategy: CopyStrategyDraft) -> Result<CopyStrategy> {
        let mut state = self.state.write().await;
        let created = CopyStrategy {
            id: self.next("strategy"),
            name: strategy.name,
            enabled: true,
            follower_account_id: strategy.follower_account_id,
            source_address_id: strategy.source_address_id,
            market_selectors: strategy.market_selectors,
            size_multiplier_bps: strategy.size_multiplier_bps,
            min_order_size: strategy.min_order_size,
            maker_slippage_bps: strategy.maker_slippage_bps,
            taker_slippage_bps: strategy.taker_slippage_bps,
            activation_pnl_bps: strategy.activation_pnl_bps,
            trailing_drawdown_bps: strategy.trailing_drawdown_bps,
            allow_taker_on_sweep: strategy.allow_taker_on_sweep,
            created_at: Utc::now(),
        };
        state.strategies.push(created.clone());
        persist_state(&self.path, &state)?;
        Ok(created)
    }

    pub async fn dashboard_overview(&self, ledger: &ShadowLedger) -> DashboardOverview {
        let state = self.snapshot().await;
        DashboardOverview {
            follower_accounts: state.follower_accounts,
            source_addresses: state.source_addresses,
            strategies: state.strategies,
            markets: state.markets,
            positions: ledger.all_positions(),
        }
    }

    fn next(&self, prefix: &str) -> String {
        format!("{prefix}-{}", self.next_id.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateFollowerAccountRequest {
    pub label: String,
    pub address: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateSourceAddressRequest {
    pub label: String,
    pub address: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CopyStrategyDraft {
    pub name: String,
    pub follower_account_id: String,
    pub source_address_id: String,
    pub market_selectors: Vec<String>,
    pub size_multiplier_bps: u32,
    pub min_order_size: i64,
    pub maker_slippage_bps: u32,
    pub taker_slippage_bps: u32,
    pub activation_pnl_bps: i64,
    pub trailing_drawdown_bps: u32,
    pub allow_taker_on_sweep: bool,
}

fn persist_state(path: &Path, state: &ControlPlaneState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create dir {}", parent.display()))?;
    }
    let raw = serde_json::to_string_pretty(state)?;
    fs::write(path, raw).with_context(|| format!("write control plane {}", path.display()))?;
    Ok(())
}

fn normalize_address(address: &str) -> String {
    address.trim().to_ascii_lowercase()
}

fn market_matches_trade(market: &MarketRegistration, trade: &MarketTrade) -> bool {
    market
        .condition_id
        .as_ref()
        .map(|condition_id| condition_id.eq_ignore_ascii_case(&trade.market_id))
        .unwrap_or(false)
        || market_matches_selector(market, trade, &market.base_asset)
}

fn market_matches_selector(market: &MarketRegistration, trade: &MarketTrade, selector: &str) -> bool {
    let selector = selector.to_ascii_lowercase();
    let slug = market.slug.as_deref().unwrap_or_default().to_ascii_lowercase();
    let question = market.question.as_deref().unwrap_or_default().to_ascii_lowercase();
    let joined = format!(
        "{} {} {} {} {} {}",
        market.id.to_ascii_lowercase(),
        market.label.to_ascii_lowercase(),
        market.base_asset.to_ascii_lowercase(),
        market.timeframe.to_ascii_lowercase(),
        slug,
        question
    );
    joined.contains(&selector)
        || trade.market_id.to_ascii_lowercase().contains(&selector)
        || trade.asset_id.to_ascii_lowercase().contains(&selector)
        || market.selectors.iter().any(|item| item.eq_ignore_ascii_case(&selector))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::config::ControlConfig;

    use super::ControlPlane;

    #[test]
    fn load_bootstraps_state_file() {
        let path = format!("/tmp/polymarket-copybot-control-{}.json", std::process::id());
        let _ = fs::remove_file(&path);
        let _control = ControlPlane::load(&ControlConfig {
            state_file: path.clone(),
            dashboard_bind: "127.0.0.1:0".to_string(),
        })
        .expect("load control plane");
        assert!(std::path::Path::new(&path).exists());
    }
}
