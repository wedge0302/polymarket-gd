use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn sign(self) -> i64 {
        match self {
            Side::Buy => 1,
            Side::Sell => -1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncMode {
    Following,
    Independent,
    Closed,
}

#[derive(Debug, Clone)]
pub struct MarketTrade {
    pub market_id: String,
    pub asset_id: String,
    pub price: f64,
    pub size: i64,
    pub side: Side,
    pub maker_address: String,
    pub taker_address: String,
    pub sweep: bool,
}

impl MarketTrade {
    pub fn touches_address(&self, address: &str) -> bool {
        self.maker_address.eq_ignore_ascii_case(address) || self.taker_address.eq_ignore_ascii_case(address)
    }
}

#[derive(Debug, Clone)]
pub struct BookUpdate {
    pub market_id: String,
    pub best_bid: f64,
    pub best_ask: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionState {
    pub state_key: String,
    pub follower_account_id: String,
    pub source_address_id: String,
    pub strategy_id: String,
    pub market_id: String,
    pub asset_id: String,
    pub ideal_position: i64,
    pub actual_position: i64,
    pub avg_entry_price: f64,
    pub activation_pnl_bps: i64,
    pub trailing_drawdown_bps: u32,
    pub sync_mode: SyncMode,
    pub last_price: f64,
    pub high_water_mark: Option<f64>,
    pub updated_at: DateTime<Utc>,
}

impl PositionState {
    pub fn diff(&self) -> i64 {
        self.ideal_position - self.actual_position
    }
}

#[derive(Debug, Clone)]
pub struct EntrySignal {
    pub state_key: String,
    pub market_id: String,
    pub asset_id: String,
    pub diff: i64,
    pub reference_price: f64,
    pub maker_slippage_bps: u32,
    pub taker_slippage_bps: u32,
    pub allow_taker_on_sweep: bool,
    pub sweep: bool,
}

#[derive(Debug, Clone)]
pub enum ShadowCommand {
    Bootstrap {
        state_key: String,
        follower_account_id: String,
        source_address_id: String,
        strategy_id: String,
        market_id: String,
        asset_id: String,
        ideal_position: i64,
        reference_price: f64,
        activation_pnl_bps: i64,
        trailing_drawdown_bps: u32,
    },
    Fill {
        state_key: String,
        filled_qty: i64,
        fill_price: f64,
    },
}

#[derive(Debug, Clone)]
pub enum RiskEvent {
    Trade(MarketTrade),
    Book(BookUpdate),
}

#[derive(Debug, Clone)]
pub struct ResolvedFollowContext {
    pub follower_account_id: String,
    pub source_address_id: String,
    pub strategy_id: String,
    pub state_key: String,
    pub size_multiplier_bps: u32,
    pub min_order_size: i64,
    pub maker_slippage_bps: u32,
    pub taker_slippage_bps: u32,
    pub activation_pnl_bps: i64,
    pub trailing_drawdown_bps: u32,
    pub allow_taker_on_sweep: bool,
}
