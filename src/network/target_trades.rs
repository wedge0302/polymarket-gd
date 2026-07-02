use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use tokio::{sync::mpsc, time::MissedTickBehavior};
use tracing::warn;

use crate::{
    config::TargetSourceConfig,
    control::ControlPlane,
    types::{MarketTrade, ShadowCommand, Side},
};

#[derive(Debug)]
pub struct TargetTradePoller {
    config: TargetSourceConfig,
    control: Arc<ControlPlane>,
    client: Client,
    trade_tx: mpsc::Sender<MarketTrade>,
    shadow_tx: mpsc::Sender<ShadowCommand>,
}

impl TargetTradePoller {
    pub fn new(
        config: TargetSourceConfig,
        control: Arc<ControlPlane>,
        trade_tx: mpsc::Sender<MarketTrade>,
        shadow_tx: mpsc::Sender<ShadowCommand>,
    ) -> Result<Self> {
        let client = Client::builder()
            .tcp_nodelay(true)
            .pool_max_idle_per_host(32)
            .build()
            .context("build data-api client")?;
        Ok(Self {
            config,
            control,
            client,
            trade_tx,
            shadow_tx,
        })
    }

    pub async fn run(self) -> Result<()> {
        if self.config.bootstrap_positions {
            self.bootstrap_positions().await?;
        }

        let mut cursors = HashMap::<String, i64>::new();
        let mut recent_keys = RecentTradeKeys::new(self.config.dedupe_cache_size);
        let mut interval = tokio::time::interval(self.config.poll_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            let snapshot = self.control.snapshot().await;
            let markets: Vec<String> = snapshot
                .markets
                .iter()
                .filter(|market| market.enabled)
                .filter_map(|market| market.condition_id.clone())
                .collect();
            if markets.is_empty() {
                continue;
            }
            let joined_markets = markets.join(",");

            for source in snapshot.source_addresses.iter().filter(|source| source.enabled) {
                let source_key = source.address.to_ascii_lowercase();
                let cursor = *cursors.get(&source_key).unwrap_or(&0);
                match self.fetch_trades_for_source(&source.address, &joined_markets).await {
                    Ok(mut response) => {
                        response.sort_by_key(|trade| trade.timestamp);
                        let mut max_seen_ts = cursor;

                        for trade in response {
                            max_seen_ts = max_seen_ts.max(trade.timestamp);
                            let dedupe = trade.dedupe_key();
                            if trade.timestamp < cursor {
                                continue;
                            }
                            if trade.timestamp == cursor && recent_keys.contains(&dedupe) {
                                continue;
                            }
                            if recent_keys.insert(dedupe) {
                                let _ = self.trade_tx.send(trade.into_market_trade()).await;
                            }
                        }

                        cursors.insert(source_key, max_seen_ts);
                    }
                    Err(err) => {
                        warn!(source = %source.address, ?err, "target trade poll failed");
                    }
                }
            }
        }
    }

    async fn fetch_trades_for_source(&self, source_address: &str, joined_markets: &str) -> Result<Vec<DataTrade>> {
        let limit = self.config.trade_batch_size.to_string();
        self.client
            .get(format!("{}/trades", self.config.data_api_url))
            .query(&[
                ("user", source_address),
                ("limit", limit.as_str()),
                ("takerOnly", "false"),
                ("market", joined_markets),
            ])
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<DataTrade>>()
            .await
            .context("decode trades response")
    }

    async fn bootstrap_positions(&self) -> Result<()> {
        let snapshot = self.control.snapshot().await;
        let markets: HashSet<String> = snapshot
            .markets
            .iter()
            .filter(|market| market.enabled)
            .filter_map(|market| market.condition_id.clone())
            .collect();

        for source in snapshot.source_addresses.iter().filter(|source| source.enabled) {
            let limit = self.config.bootstrap_positions_limit.to_string();
            let response = self
                .client
                .get(format!("{}/positions", self.config.data_api_url))
                .query(&[
                    ("user", source.address.as_str()),
                    ("limit", limit.as_str()),
                ])
                .send()
                .await?
                .error_for_status()?
                .json::<Vec<DataPosition>>()
                .await?;

            for position in response {
                if !markets.contains(&position.condition_id) || position.size.abs() < 0.5 {
                    continue;
                }
                let synthetic_trade = MarketTrade {
                    market_id: position.condition_id.clone(),
                    asset_id: position.asset.clone(),
                    price: position.avg_price,
                    size: position.size.round() as i64,
                    side: Side::Buy,
                    maker_address: source.address.clone(),
                    taker_address: source.address.clone(),
                    sweep: false,
                };
                if let Some(context) = self.control.resolve_follow_context(&synthetic_trade).await {
                    let _ = self
                        .shadow_tx
                        .send(ShadowCommand::Bootstrap {
                            state_key: context.state_key,
                            follower_account_id: context.follower_account_id,
                            source_address_id: context.source_address_id,
                            strategy_id: context.strategy_id,
                            market_id: position.condition_id,
                            asset_id: position.asset,
                            ideal_position: position.size.round() as i64,
                            reference_price: position.avg_price,
                            activation_pnl_bps: context.activation_pnl_bps,
                            trailing_drawdown_bps: context.trailing_drawdown_bps,
                        })
                        .await;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct DataTrade {
    #[serde(rename = "proxyWallet")]
    proxy_wallet: String,
    asset: String,
    #[serde(rename = "conditionId")]
    condition_id: String,
    size: f64,
    price: f64,
    timestamp: i64,
    side: String,
}

impl DataTrade {
    fn dedupe_key(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}:{}",
            self.proxy_wallet, self.condition_id, self.asset, self.timestamp, self.price, self.size
        )
    }

    fn into_market_trade(self) -> MarketTrade {
        MarketTrade {
            market_id: self.condition_id,
            asset_id: self.asset,
            price: self.price,
            size: self.size.round() as i64,
            side: if self.side.eq_ignore_ascii_case("BUY") {
                Side::Buy
            } else {
                Side::Sell
            },
            maker_address: self.proxy_wallet.clone(),
            taker_address: self.proxy_wallet,
            sweep: false,
        }
    }
}

#[derive(Debug, Deserialize)]
struct DataPosition {
    asset: String,
    #[serde(rename = "conditionId")]
    condition_id: String,
    size: f64,
    #[serde(rename = "avgPrice")]
    avg_price: f64,
}

#[derive(Debug)]
struct RecentTradeKeys {
    cap: usize,
    queue: VecDeque<String>,
    set: HashSet<String>,
}

impl RecentTradeKeys {
    fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(128),
            queue: VecDeque::new(),
            set: HashSet::new(),
        }
    }

    fn contains(&self, key: &str) -> bool {
        self.set.contains(key)
    }

    fn insert(&mut self, key: String) -> bool {
        if !self.set.insert(key.clone()) {
            return false;
        }
        self.queue.push_back(key);
        while self.queue.len() > self.cap {
            if let Some(oldest) = self.queue.pop_front() {
                self.set.remove(&oldest);
            }
        }
        true
    }
}
