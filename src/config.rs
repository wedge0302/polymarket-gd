use std::time::Duration;

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .and_then(|value| match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u8(key: &str, default: u8) -> u8 {
    std::env::var(key).ok().and_then(|value| value.parse().ok()).unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|value| value.parse().ok()).unwrap_or(default)
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key).ok().and_then(|value| value.parse().ok()).unwrap_or(default)
}

#[derive(Debug, Clone)]
pub struct BotConfig {
    pub ws: WsConfig,
    pub target_source: TargetSourceConfig,
    pub shadow: ShadowConfig,
    pub execution: ExecutionConfig,
    pub risk: RiskConfig,
    pub channels: ChannelConfig,
    pub control: ControlConfig,
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            ws: WsConfig::default(),
            target_source: TargetSourceConfig::default(),
            shadow: ShadowConfig::default(),
            execution: ExecutionConfig::default(),
            risk: RiskConfig::default(),
            channels: ChannelConfig::default(),
            control: ControlConfig::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WsConfig {
    pub market_ws_url: String,
    pub user_ws_url: String,
    pub enable_user_channel: bool,
    pub ping_interval: Duration,
    pub pong_timeout: Duration,
    pub reconnect_backoff_initial: Duration,
    pub reconnect_backoff_max: Duration,
}

impl Default for WsConfig {
    fn default() -> Self {
        Self {
            market_ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string(),
            user_ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/user".to_string(),
            enable_user_channel: env_bool("POLY_ENABLE_USER_WS", true),
            ping_interval: Duration::from_secs(5),
            pong_timeout: Duration::from_secs(15),
            reconnect_backoff_initial: Duration::from_millis(250),
            reconnect_backoff_max: Duration::from_secs(8),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShadowConfig {
    pub size_multiplier_bps: u32,
    pub min_order_size: i64,
}

impl Default for ShadowConfig {
    fn default() -> Self {
        Self {
            size_multiplier_bps: 10_000,
            min_order_size: 5,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    pub enabled: bool,
    pub clob_api_url: String,
    pub private_key_env: String,
    pub funder_address: Option<String>,
    pub signature_type: u8,
    pub order_ttl: Duration,
    pub stale_requote_bps: u32,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            enabled: env_bool("POLY_EXECUTION_ENABLED", false),
            clob_api_url: env_string("POLY_CLOB_API_URL", "https://clob-v2.polymarket.com/"),
            private_key_env: env_string("POLY_PRIVATE_KEY_ENV", "POLYMARKET_PRIVATE_KEY"),
            funder_address: std::env::var("POLY_FUNDER_ADDRESS").ok(),
            signature_type: env_u8("POLY_SIGNATURE_TYPE", 1),
            order_ttl: Duration::from_secs(60),
            stale_requote_bps: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TargetSourceConfig {
    pub data_api_url: String,
    pub poll_interval: Duration,
    pub trade_batch_size: u32,
    pub dedupe_cache_size: usize,
    pub bootstrap_positions: bool,
    pub bootstrap_positions_limit: usize,
}

impl Default for TargetSourceConfig {
    fn default() -> Self {
        Self {
            data_api_url: "https://data-api.polymarket.com".to_string(),
            poll_interval: Duration::from_millis(400),
            trade_batch_size: env_u32("POLY_TRADE_BATCH_SIZE", 100),
            dedupe_cache_size: env_usize("POLY_TRADE_DEDUPE_CACHE_SIZE", 4096),
            bootstrap_positions: env_bool("POLY_BOOTSTRAP_POSITIONS", true),
            bootstrap_positions_limit: env_usize("POLY_BOOTSTRAP_POSITIONS_LIMIT", 50),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RiskConfig {
    pub activation_pnl_bps: i64,
    pub trailing_drawdown_bps: u32,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            activation_pnl_bps: 3_000,
            trailing_drawdown_bps: 1_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChannelConfig {
    pub trade_buffer: usize,
    pub book_buffer: usize,
    pub shadow_buffer: usize,
    pub execution_buffer: usize,
    pub risk_buffer: usize,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            trade_buffer: 4096,
            book_buffer: 4096,
            shadow_buffer: 1024,
            execution_buffer: 1024,
            risk_buffer: 2048,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ControlConfig {
    pub state_file: String,
    pub dashboard_bind: String,
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self {
            state_file: env_string("POLY_CONTROL_STATE_FILE", "./data/control-plane.json"),
            dashboard_bind: env_string("POLY_DASHBOARD_BIND", "127.0.0.1:8080"),
        }
    }
}
