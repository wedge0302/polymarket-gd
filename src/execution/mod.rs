use std::{str::FromStr, sync::Arc};

use alloy::primitives::Address;
use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
use dashmap::DashMap;
use polymarket_client_sdk_v2::{
    POLYGON,
    auth::ExposeSecret,
    clob::{
        Client as ClobClient, Config as ClobConfig,
        types::{Amount, OrderType, Side as ClobSide, SignatureType},
    },
    types::{Decimal, U256},
};
use tokio::{sync::mpsc, time::Instant};
use tracing::{info, warn};

use crate::{
    config::ExecutionConfig,
    network::orderbook::OrderBookCache,
    types::{EntrySignal, Side},
};

#[derive(Debug, Clone)]
pub enum ExecutionCommand {
    Entry(EntrySignal),
    Exit(ExitOrder),
}

#[derive(Debug, Clone)]
pub struct ExitOrder {
    pub state_key: String,
    pub asset_id: String,
    pub quantity: i64,
    pub reason: ExitReason,
}

#[derive(Debug, Clone)]
pub enum ExitReason {
    TrailingStop,
}

#[derive(Debug, Clone)]
pub struct UserChannelAuth {
    pub api_key: String,
    pub secret: String,
    pub passphrase: String,
}

#[derive(Debug, Clone)]
pub struct OrderRegistryEntry {
    pub state_key: String,
    pub side: Side,
}

#[derive(Debug, Default)]
pub struct OrderRegistry {
    inner: DashMap<String, OrderRegistryEntry>,
}

impl OrderRegistry {
    pub fn insert(&self, order_id: String, entry: OrderRegistryEntry) {
        self.inner.insert(order_id, entry);
    }

    pub fn remove(&self, order_id: &str) -> Option<OrderRegistryEntry> {
        self.inner.remove(order_id).map(|(_, value)| value)
    }

    pub fn get(&self, order_id: &str) -> Option<OrderRegistryEntry> {
        self.inner.get(order_id).map(|entry| entry.clone())
    }
}

#[derive(Debug)]
struct LiveOrder {
    order_id: String,
    market_id: String,
    price: f64,
    expires_at: Instant,
}

type LiveClient = ClobClient<polymarket_client_sdk_v2::auth::state::Authenticated<polymarket_client_sdk_v2::auth::Normal>>;

#[derive(Clone, Debug)]
struct TradingSession {
    client: LiveClient,
    private_key: String,
}

pub struct ExecutionEngine {
    config: ExecutionConfig,
    orderbooks: Arc<OrderBookCache>,
    registry: Arc<OrderRegistry>,
    rx: mpsc::Receiver<ExecutionCommand>,
    session: Option<TradingSession>,
}

impl ExecutionEngine {
    pub async fn new(
        config: ExecutionConfig,
        orderbooks: Arc<OrderBookCache>,
        registry: Arc<OrderRegistry>,
        rx: mpsc::Receiver<ExecutionCommand>,
    ) -> Result<Self> {
        let session = if config.enabled {
            Some(connect_trading_session(&config).await?)
        } else {
            None
        };
        Ok(Self {
            config,
            orderbooks,
            registry,
            rx,
            session,
        })
    }

    pub fn user_channel_auth(&self) -> Option<UserChannelAuth> {
        let session = self.session.as_ref()?;
        Some(UserChannelAuth {
            api_key: session.client.credentials().key().to_string(),
            secret: session.client.credentials().secret().expose_secret().to_string(),
            passphrase: session.client.credentials().passphrase().expose_secret().to_string(),
        })
    }

    pub async fn run(mut self) -> Result<()> {
        let live_orders = DashMap::<String, LiveOrder>::new();
        let mut gc_tick = tokio::time::interval(std::time::Duration::from_secs(1));

        loop {
            tokio::select! {
                Some(command) = self.rx.recv() => {
                    match command {
                        ExecutionCommand::Entry(signal) => {
                            self.execute_entry(signal, &live_orders).await?;
                        }
                        ExecutionCommand::Exit(exit) => {
                            self.execute_exit(exit).await?;
                        }
                    }
                }
                _ = gc_tick.tick() => {
                    let now = Instant::now();
                    let stale: Vec<(String, String, String)> = live_orders
                        .iter()
                        .filter_map(|entry| {
                            let order = entry.value();
                            let ttl_expired = order.expires_at <= now;
                            let drifted = self
                                .orderbooks
                                .is_stale_price(&order.market_id, order.price, self.config.stale_requote_bps);
                            if ttl_expired || drifted {
                                Some((entry.key().clone(), order.order_id.clone(), order.market_id.clone()))
                            } else {
                                None
                            }
                        })
                        .collect();

                    for (state_key, order_id, market_id) in stale {
                        warn!(state_key, order_id, market_id, "cancel stale order");
                        let _ = self.cancel_order(&order_id).await;
                        live_orders.remove(&state_key);
                        self.registry.remove(&order_id);
                    }
                }
                else => break,
            }
        }

        Ok(())
    }

    async fn execute_entry(&self, signal: EntrySignal, live_orders: &DashMap<String, LiveOrder>) -> Result<()> {
        let Some(session) = self.session.as_ref() else {
            warn!(state_key = %signal.state_key, "execution disabled, dropping entry signal");
            return Ok(());
        };

        let side = if signal.diff > 0 { Side::Buy } else { Side::Sell };
        let post_only = !(signal.sweep && signal.allow_taker_on_sweep);
        let qty = Decimal::from_str(&signal.diff.abs().to_string())?;
        let price = self.resolve_limit_price(&signal, side);
        let token_id = U256::from_str(&signal.asset_id)?;

        let response = if post_only {
            let signer = signer_from_private_key(&session.private_key)?;
            session
                .client
                .limit_order()
                .token_id(token_id)
                .side(to_clob_side(side))
                .price(decimal_from_f64(price)?)
                .size(qty)
                .order_type(OrderType::GTC)
                .post_only(true)
                .build_sign_and_post(&signer)
                .await?
        } else {
            let signer = signer_from_private_key(&session.private_key)?;
            let amount = if matches!(side, Side::Buy) {
                Amount::usdc(decimal_from_f64(price * signal.diff.abs() as f64)?)?
            } else {
                Amount::shares(qty)?
            };
            session
                .client
                .market_order()
                .token_id(token_id)
                .side(to_clob_side(side))
                .amount(amount)
                .order_type(OrderType::FAK)
                .build_sign_and_post(&signer)
                .await?
        };

        info!(
            state_key = %signal.state_key,
            order_id = %response.order_id,
            status = %response.status,
            market_id = %signal.market_id,
            asset_id = %signal.asset_id,
            post_only,
            "order posted"
        );

        self.registry.insert(
            response.order_id.to_string(),
            OrderRegistryEntry {
                state_key: signal.state_key.clone(),
                side,
            },
        );
        live_orders.insert(
            signal.state_key.clone(),
            LiveOrder {
                order_id: response.order_id.to_string(),
                market_id: signal.market_id,
                price,
                expires_at: Instant::now() + self.config.order_ttl,
            },
        );
        Ok(())
    }

    async fn execute_exit(&self, exit: ExitOrder) -> Result<()> {
        let Some(session) = self.session.as_ref() else {
            warn!(state_key = %exit.state_key, "execution disabled, dropping exit signal");
            return Ok(());
        };
        let token_id = U256::from_str(&exit.asset_id)?;
        let qty = Decimal::from_str(&exit.quantity.to_string())?;
        let signer = signer_from_private_key(&session.private_key)?;
        let response = session
            .client
            .market_order()
            .token_id(token_id)
            .side(ClobSide::Sell)
            .amount(Amount::shares(qty)?)
            .order_type(OrderType::FAK)
            .build_sign_and_post(&signer)
            .await?;

        info!(
            state_key = %exit.state_key,
            order_id = %response.order_id,
            status = %response.status,
            reason = ?exit.reason,
            "exit order posted"
        );
        self.registry.insert(
            response.order_id.to_string(),
            OrderRegistryEntry {
                state_key: exit.state_key,
                side: Side::Sell,
            },
        );
        Ok(())
    }

    fn resolve_limit_price(&self, signal: &EntrySignal, side: Side) -> f64 {
        let abs_qty = signal.diff.unsigned_abs() as f64;
        let vwap = self
            .orderbooks
            .mid_price(&signal.market_id)
            .unwrap_or(signal.reference_price);
        let _ = abs_qty;
        let slippage = if signal.sweep && signal.allow_taker_on_sweep {
            signal.taker_slippage_bps
        } else {
            signal.maker_slippage_bps
        } as f64
            / 10_000.0;
        match side {
            Side::Buy => (vwap * (1.0 + slippage)).min(0.99),
            Side::Sell => (vwap * (1.0 - slippage)).max(0.01),
        }
    }

    async fn cancel_order(&self, order_id: &str) -> Result<()> {
        let Some(session) = self.session.as_ref() else {
            return Ok(());
        };
        let _ = session.client.cancel_order(order_id).await?;
        Ok(())
    }
}

async fn connect_trading_session(config: &ExecutionConfig) -> Result<TradingSession> {
    let private_key = std::env::var(&config.private_key_env)
        .with_context(|| format!("missing {}", config.private_key_env))?;
    let signer = signer_from_private_key(&private_key)?;
    let mut builder = ClobClient::new(
        &config.clob_api_url,
        ClobConfig::builder().use_server_time(true).build(),
    )?
    .authentication_builder(&signer)
    .signature_type(match config.signature_type {
        0 => SignatureType::Eoa,
        1 => SignatureType::Proxy,
        2 => SignatureType::GnosisSafe,
        3 => SignatureType::Poly1271,
        value => anyhow::bail!("unsupported signature_type {value}"),
    });

    if let Some(address) = config.funder_address.as_deref() {
        builder = builder.funder(Address::from_str(address)?);
    }

    let client = builder.authenticate().await?;
    Ok(TradingSession { client, private_key })
}

fn to_clob_side(side: Side) -> ClobSide {
    match side {
        Side::Buy => ClobSide::Buy,
        Side::Sell => ClobSide::Sell,
    }
}

fn decimal_from_f64(value: f64) -> Result<Decimal> {
    Decimal::from_str(&format!("{value:.6}")).context("invalid decimal conversion")
}

fn signer_from_private_key(private_key: &str) -> Result<PrivateKeySigner> {
    Ok(PrivateKeySigner::from_str(private_key)?.with_chain_id(Some(POLYGON)))
}
