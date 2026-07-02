use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tracing::debug;

use crate::{
    config::ShadowConfig,
    control::ControlPlane,
    execution::ExecutionCommand,
    types::{
        EntrySignal, MarketTrade, PositionState, ResolvedFollowContext, RiskEvent, ShadowCommand, SyncMode,
    },
};

#[derive(Debug)]
pub struct ShadowLedger {
    config: ShadowConfig,
    control: Arc<ControlPlane>,
    states: Arc<DashMap<String, PositionState>>,
}

impl ShadowLedger {
    pub fn new(config: ShadowConfig, control: Arc<ControlPlane>) -> Self {
        Self {
            config,
            control,
            states: Arc::new(DashMap::new()),
        }
    }

    pub async fn run(
        &self,
        mut trade_rx: mpsc::Receiver<MarketTrade>,
        mut shadow_rx: mpsc::Receiver<ShadowCommand>,
        exec_tx: mpsc::Sender<ExecutionCommand>,
        risk_tx: mpsc::Sender<RiskEvent>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                Some(trade) = trade_rx.recv() => {
                    let maybe_signal = self.on_target_trade(trade.clone()).await;
                    let _ = risk_tx.send(RiskEvent::Trade(trade)).await;
                    if let Some(signal) = maybe_signal {
                        let _ = exec_tx.send(ExecutionCommand::Entry(signal)).await;
                    }
                }
                Some(command) = shadow_rx.recv() => {
                    self.on_shadow_command(command);
                }
                else => break,
            }
        }
        Ok(())
    }

    pub fn all_positions(&self) -> Vec<PositionState> {
        self.states.iter().map(|entry| entry.clone()).collect()
    }

    pub fn states_for_market(&self, market_id: &str) -> Vec<PositionState> {
        self.states
            .iter()
            .filter(|entry| entry.market_id == market_id)
            .map(|entry| entry.clone())
            .collect()
    }

    pub async fn on_target_trade(&self, trade: MarketTrade) -> Option<EntrySignal> {
        let context = self.control.resolve_follow_context(&trade).await?;
        self.apply_target_trade(trade, context)
    }

    pub fn apply_target_trade(&self, trade: MarketTrade, context: ResolvedFollowContext) -> Option<EntrySignal> {
        let size_multiplier_bps = if context.size_multiplier_bps == 0 {
            self.config.size_multiplier_bps
        } else {
            context.size_multiplier_bps
        };
        let min_order_size = if context.min_order_size <= 0 {
            self.config.min_order_size
        } else {
            context.min_order_size
        };
        let scaled_delta = trade.side.sign() * scale_size(trade.size, size_multiplier_bps);
        let mut state = self.states.entry(context.state_key.clone()).or_insert_with(|| PositionState {
            state_key: context.state_key.clone(),
            follower_account_id: context.follower_account_id.clone(),
            source_address_id: context.source_address_id.clone(),
            strategy_id: context.strategy_id.clone(),
            market_id: trade.market_id.clone(),
            asset_id: trade.asset_id.clone(),
            ideal_position: 0,
            actual_position: 0,
            avg_entry_price: 0.0,
            activation_pnl_bps: context.activation_pnl_bps,
            trailing_drawdown_bps: context.trailing_drawdown_bps,
            sync_mode: SyncMode::Following,
            last_price: trade.price,
            high_water_mark: None,
            updated_at: Utc::now(),
        });

        state.ideal_position += scaled_delta;
        state.last_price = trade.price;
        state.updated_at = Utc::now();
        let diff = state.diff();
        debug!(state_key = %state.state_key, market_id = %state.market_id, diff, "shadow diff updated");

        if state.sync_mode != SyncMode::Following || diff.abs() < min_order_size {
            return None;
        }

        Some(EntrySignal {
            state_key: state.state_key.clone(),
            market_id: state.market_id.clone(),
            asset_id: state.asset_id.clone(),
            diff,
            reference_price: trade.price,
            maker_slippage_bps: context.maker_slippage_bps,
            taker_slippage_bps: context.taker_slippage_bps,
            allow_taker_on_sweep: context.allow_taker_on_sweep,
            sweep: trade.sweep,
        })
    }

    pub(crate) fn on_shadow_command(&self, command: ShadowCommand) {
        match command {
            ShadowCommand::Fill {
                state_key,
                filled_qty,
                fill_price,
            } => {
                if let Some(mut state) = self.states.get_mut(&state_key) {
                    let next_position = state.actual_position + filled_qty;
                    if next_position != 0 {
                        let weighted_cost =
                            (state.avg_entry_price * state.actual_position as f64) + (fill_price * filled_qty as f64);
                        state.avg_entry_price = weighted_cost / next_position as f64;
                    } else {
                        state.avg_entry_price = 0.0;
                    }
                    state.actual_position = next_position;
                    state.last_price = fill_price;
                    state.updated_at = Utc::now();
                }
            }
            ShadowCommand::Bootstrap {
                state_key,
                follower_account_id,
                source_address_id,
                strategy_id,
                market_id,
                asset_id,
                ideal_position,
                reference_price,
                activation_pnl_bps,
                trailing_drawdown_bps,
            } => {
                self.states.entry(state_key.clone()).or_insert_with(|| PositionState {
                    state_key,
                    follower_account_id,
                    source_address_id,
                    strategy_id,
                    market_id,
                    asset_id,
                    ideal_position,
                    actual_position: 0,
                    avg_entry_price: 0.0,
                    activation_pnl_bps,
                    trailing_drawdown_bps,
                    sync_mode: SyncMode::Following,
                    last_price: reference_price,
                    high_water_mark: None,
                    updated_at: Utc::now(),
                });
            }
        }
    }

    pub fn mark_independent(&self, state_key: &str, high_water_mark: f64) {
        if let Some(mut state) = self.states.get_mut(state_key) {
            state.sync_mode = SyncMode::Independent;
            state.high_water_mark = Some(high_water_mark);
            state.updated_at = Utc::now();
        }
    }

    pub fn update_high_water_mark(&self, state_key: &str, high_water_mark: f64) {
        if let Some(mut state) = self.states.get_mut(state_key) {
            state.high_water_mark = Some(
                state
                    .high_water_mark
                    .map(|current| current.max(high_water_mark))
                    .unwrap_or(high_water_mark),
            );
            state.last_price = high_water_mark;
            state.updated_at = Utc::now();
        }
    }
}

fn scale_size(size: i64, multiplier_bps: u32) -> i64 {
    size * multiplier_bps as i64 / 10_000
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use pretty_assertions::assert_eq;

    use crate::{
        config::{ControlConfig, ShadowConfig},
        control::ControlPlane,
        types::{MarketTrade, ResolvedFollowContext, Side, SyncMode},
    };

    use super::ShadowLedger;

    fn test_control() -> Arc<ControlPlane> {
        let path = format!("/tmp/polymarket-copybot-shadow-{}.json", std::process::id());
        let _ = fs::remove_file(&path);
        Arc::new(ControlPlane::load(&ControlConfig {
            state_file: path,
            dashboard_bind: "127.0.0.1:0".to_string(),
        }).expect("control plane"))
    }

    #[test]
    fn batches_below_minimum_threshold() {
        let ledger = ShadowLedger::new(
            ShadowConfig {
                size_multiplier_bps: 10_000,
                min_order_size: 5,
            },
            test_control(),
        );

        let trade = |size| MarketTrade {
            market_id: "m1".into(),
            asset_id: "a1".into(),
            price: 0.52,
            size,
            side: Side::Buy,
            maker_address: String::new(),
            taker_address: String::new(),
            sweep: false,
        };
        let ctx = ResolvedFollowContext {
            follower_account_id: "f1".into(),
            source_address_id: "s1".into(),
            strategy_id: "st1".into(),
            state_key: "f1:s1:m1:a1".into(),
            size_multiplier_bps: 10_000,
            min_order_size: 5,
            maker_slippage_bps: 200,
            taker_slippage_bps: 350,
            activation_pnl_bps: 3000,
            trailing_drawdown_bps: 1000,
            allow_taker_on_sweep: true,
        };

        assert!(ledger.apply_target_trade(trade(2), ctx.clone()).is_none());
        assert!(ledger.apply_target_trade(trade(2), ctx.clone()).is_none());
        let signal = ledger.apply_target_trade(trade(1), ctx).expect("should trigger at 5");
        assert_eq!(signal.diff, 5);
    }

    #[test]
    fn independent_mode_suppresses_follow_signals() {
        let ledger = ShadowLedger::new(ShadowConfig::default(), test_control());
        let trade = MarketTrade {
            market_id: "m2".into(),
            asset_id: "a2".into(),
            price: 0.61,
            size: 10,
            side: Side::Buy,
            maker_address: String::new(),
            taker_address: String::new(),
            sweep: false,
        };
        let ctx = ResolvedFollowContext {
            follower_account_id: "f1".into(),
            source_address_id: "s1".into(),
            strategy_id: "st1".into(),
            state_key: "f1:s1:m2:a2".into(),
            size_multiplier_bps: 10_000,
            min_order_size: 5,
            maker_slippage_bps: 200,
            taker_slippage_bps: 350,
            activation_pnl_bps: 3000,
            trailing_drawdown_bps: 1000,
            allow_taker_on_sweep: true,
        };

        let _ = ledger.apply_target_trade(trade.clone(), ctx);
        ledger.mark_independent("f1:s1:m2:a2", 0.7);
        assert_eq!(ledger.states_for_market("m2")[0].sync_mode, SyncMode::Independent);
        assert!(ledger.apply_target_trade(
            trade,
            ResolvedFollowContext {
                follower_account_id: "f1".into(),
                source_address_id: "s1".into(),
                strategy_id: "st1".into(),
                state_key: "f1:s1:m2:a2".into(),
                size_multiplier_bps: 10_000,
                min_order_size: 5,
                maker_slippage_bps: 200,
                taker_slippage_bps: 350,
                activation_pnl_bps: 3000,
                trailing_drawdown_bps: 1000,
                allow_taker_on_sweep: true,
            }
        ).is_none());
    }
}
