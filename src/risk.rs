use anyhow::Result;
use tokio::sync::mpsc;
use tracing::info;

use crate::{
    config::RiskConfig,
    execution::{ExecutionCommand, ExitOrder, ExitReason},
    shadow::ShadowLedger,
    types::{RiskEvent, SyncMode},
};

#[derive(Debug)]
pub struct RiskManager {
    config: RiskConfig,
    ledger: std::sync::Arc<ShadowLedger>,
    rx: mpsc::Receiver<RiskEvent>,
    exec_tx: mpsc::Sender<ExecutionCommand>,
}

impl RiskManager {
    pub fn new(
        config: RiskConfig,
        ledger: std::sync::Arc<ShadowLedger>,
        rx: mpsc::Receiver<RiskEvent>,
        exec_tx: mpsc::Sender<ExecutionCommand>,
    ) -> Self {
        Self {
            config,
            ledger,
            rx,
            exec_tx,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        while let Some(event) = self.rx.recv().await {
            match event {
                RiskEvent::Trade(trade) => {
                    self.evaluate_market(&trade.market_id, trade.price).await?;
                }
                RiskEvent::Book(book) => {
                    self.evaluate_market(&book.market_id, (book.best_bid + book.best_ask) / 2.0)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn evaluate_market(&self, market_id: &str, mark_price: f64) -> Result<()> {
        for state in self.ledger.states_for_market(market_id) {
            if state.actual_position == 0 || state.avg_entry_price <= f64::EPSILON {
                continue;
            }

            let activation_pnl_bps = if state.activation_pnl_bps == 0 {
                self.config.activation_pnl_bps
            } else {
                state.activation_pnl_bps
            };
            let trailing_drawdown_bps = if state.trailing_drawdown_bps == 0 {
                self.config.trailing_drawdown_bps
            } else {
                state.trailing_drawdown_bps
            };
            let pnl_bps = (((mark_price - state.avg_entry_price) / state.avg_entry_price) * 10_000.0) as i64;
            if state.sync_mode == SyncMode::Following && pnl_bps >= activation_pnl_bps {
                self.ledger.mark_independent(&state.state_key, mark_price);
                info!(state_key = %state.state_key, pnl_bps, "risk manager promoted market to independent");
                continue;
            }

            if state.sync_mode != SyncMode::Independent {
                continue;
            }

            let high_water_mark = state.high_water_mark.unwrap_or(mark_price).max(mark_price);
            self.ledger.update_high_water_mark(&state.state_key, high_water_mark);
            let drawdown_bps = (((high_water_mark - mark_price) / high_water_mark) * 10_000.0) as u32;

            if drawdown_bps >= trailing_drawdown_bps {
                self.exec_tx
                    .send(ExecutionCommand::Exit(ExitOrder {
                        state_key: state.state_key,
                        asset_id: state.asset_id,
                        quantity: state.actual_position.abs(),
                        reason: ExitReason::TrailingStop,
                    }))
                    .await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use tokio::sync::mpsc;

    use crate::{
        config::{ControlConfig, RiskConfig, ShadowConfig},
        control::ControlPlane,
        shadow::ShadowLedger,
        types::{BookUpdate, MarketTrade, ResolvedFollowContext, Side},
    };

    use super::RiskManager;

    #[tokio::test]
    async fn transitions_to_independent_on_profit_activation() {
        let path = format!("/tmp/polymarket-copybot-risk-{}.json", std::process::id());
        let _ = fs::remove_file(&path);
        let control = Arc::new(ControlPlane::load(&ControlConfig {
            state_file: path,
            dashboard_bind: "127.0.0.1:0".to_string(),
        }).expect("control plane"));
        let ledger = Arc::new(ShadowLedger::new(ShadowConfig::default(), control));
        let trade = MarketTrade {
            market_id: "m1".into(),
            asset_id: "a1".into(),
            price: 0.50,
            size: 10,
            side: Side::Buy,
            maker_address: String::new(),
            taker_address: String::new(),
            sweep: false,
        };
        let state_key = "f1:s1:m1:a1";
        let _ = ledger.apply_target_trade(
            trade,
            ResolvedFollowContext {
                follower_account_id: "f1".into(),
                source_address_id: "s1".into(),
                strategy_id: "st1".into(),
                state_key: state_key.into(),
                size_multiplier_bps: 10_000,
                min_order_size: 5,
                maker_slippage_bps: 200,
                taker_slippage_bps: 350,
                activation_pnl_bps: 1_000,
                trailing_drawdown_bps: 1_000,
                allow_taker_on_sweep: true,
            },
        );
        ledger.on_shadow_command(crate::types::ShadowCommand::Fill {
            state_key: state_key.to_string(),
            filled_qty: 10,
            fill_price: 0.50,
        });

        let (risk_tx, risk_rx) = mpsc::channel(8);
        let (exec_tx, _exec_rx) = mpsc::channel(8);
        let manager = RiskManager::new(
            RiskConfig {
                activation_pnl_bps: 1_000,
                trailing_drawdown_bps: 1_000,
            },
            ledger.clone(),
            risk_rx,
            exec_tx,
        );

        let handle = tokio::spawn(async move { manager.run().await });
        let _ = risk_tx
            .send(crate::types::RiskEvent::Book(BookUpdate {
                market_id: "m1".into(),
                best_bid: 0.56,
                best_ask: 0.56,
            }))
            .await;
        drop(risk_tx);
        let _ = handle.await;

        assert_eq!(
            ledger.states_for_market("m1")[0].sync_mode,
            crate::types::SyncMode::Independent
        );
    }
}
