mod config;
mod control;
mod dashboard;
mod execution;
mod markets;
mod network;
mod risk;
mod shadow;
mod types;

use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use config::BotConfig;
use control::ControlPlane;
use dashboard::DashboardState;
use execution::{ExecutionCommand, ExecutionEngine, OrderRegistry};
use markets::MarketDiscovery;
use network::{orderbook::OrderBookCache, target_trades::TargetTradePoller, ws::WsManager};
use risk::RiskManager;
use shadow::ShadowLedger;
use tokio::sync::mpsc;
use tracing::{info, warn};
use types::{BookUpdate, MarketTrade, RiskEvent, ShadowCommand};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .init();

    let config = Arc::new(BotConfig::default());
    let control = Arc::new(ControlPlane::load(&config.control)?);
    let ledger = Arc::new(ShadowLedger::new(config.shadow.clone(), control.clone()));
    let orderbooks = Arc::new(OrderBookCache::default());
    let order_registry = Arc::new(OrderRegistry::default());

    if let Ok(discovery) = MarketDiscovery::new() {
        if discovery.discover_btc_5min(&control).await?.is_none() {
            warn!("btc 5min market not discovered from gamma api; keeping bootstrap registration");
        }
    }

    let (trade_tx, trade_rx) = mpsc::channel::<MarketTrade>(config.channels.trade_buffer);
    let (book_tx, book_rx) = mpsc::channel::<BookUpdate>(config.channels.book_buffer);
    let (shadow_tx, shadow_rx) = mpsc::channel::<ShadowCommand>(config.channels.shadow_buffer);
    let (exec_tx, exec_rx) = mpsc::channel::<ExecutionCommand>(config.channels.execution_buffer);
    let (risk_tx, risk_rx) = mpsc::channel::<RiskEvent>(config.channels.risk_buffer);

    let execution = ExecutionEngine::new(
        config.execution.clone(),
        orderbooks.clone(),
        order_registry.clone(),
        exec_rx,
    )
    .await?;
    let user_auth = execution.user_channel_auth();
    let ws_manager = WsManager::new(
        config.ws.clone(),
        control.clone(),
        order_registry.clone(),
        user_auth,
        book_tx,
        shadow_tx.clone(),
    );
    let target_poller = TargetTradePoller::new(
        config.target_source.clone(),
        control.clone(),
        trade_tx,
        shadow_tx.clone(),
    )?;
    let risk = RiskManager::new(config.risk.clone(), ledger.clone(), risk_rx, exec_tx.clone());
    let dashboard_state = DashboardState {
        control: control.clone(),
        ledger: ledger.clone(),
    };
    let dashboard_addr: SocketAddr = config.control.dashboard_bind.parse()?;

    let ledger_task = {
        let ledger = ledger.clone();
        let exec_tx = exec_tx.clone();
        let risk_tx = risk_tx.clone();
        tokio::spawn(async move { ledger.run(trade_rx, shadow_rx, exec_tx, risk_tx).await })
    };

    let book_task = {
        let orderbooks = orderbooks.clone();
        let risk_tx = risk_tx.clone();
        tokio::spawn(async move { orderbooks.run(book_rx, risk_tx).await })
    };

    let ws_task = tokio::spawn(async move { ws_manager.run().await });
    let target_task = tokio::spawn(async move { target_poller.run().await });
    let exec_task = tokio::spawn(async move { execution.run().await });
    let risk_task = tokio::spawn(async move { risk.run().await });
    let dashboard_task = tokio::spawn(async move { dashboard::serve(dashboard_addr, dashboard_state).await });

    info!(dashboard = %config.control.dashboard_bind, "polymarket copybot started");
    tokio::signal::ctrl_c().await?;
    info!("shutdown signal received");

    ledger_task.abort();
    book_task.abort();
    ws_task.abort();
    target_task.abort();
    exec_task.abort();
    risk_task.abort();
    dashboard_task.abort();

    Ok(())
}
