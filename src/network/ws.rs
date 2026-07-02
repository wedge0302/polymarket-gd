use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, time::sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::{
    config::WsConfig,
    control::ControlPlane,
    execution::{OrderRegistry, UserChannelAuth},
    types::{BookUpdate, ShadowCommand},
};

#[derive(Debug)]
pub struct WsManager {
    config: WsConfig,
    control: Arc<ControlPlane>,
    order_registry: Arc<OrderRegistry>,
    user_auth: Option<UserChannelAuth>,
    book_tx: mpsc::Sender<BookUpdate>,
    shadow_tx: mpsc::Sender<ShadowCommand>,
}

impl WsManager {
    pub fn new(
        config: WsConfig,
        control: Arc<ControlPlane>,
        order_registry: Arc<OrderRegistry>,
        user_auth: Option<UserChannelAuth>,
        book_tx: mpsc::Sender<BookUpdate>,
        shadow_tx: mpsc::Sender<ShadowCommand>,
    ) -> Self {
        Self {
            config,
            control,
            order_registry,
            user_auth,
            book_tx,
            shadow_tx,
        }
    }

    pub async fn run(self) -> Result<()> {
        let market_task = self.market_loop();
        if self.config.enable_user_channel && self.user_auth.is_some() {
            let user_task = self.user_loop();
            tokio::try_join!(market_task, user_task)?;
        } else {
            market_task.await?;
        }
        Ok(())
    }

    async fn market_loop(&self) -> Result<()> {
        self.stream_with_backoff(&self.config.market_ws_url, StreamKind::Market)
            .await
    }

    async fn user_loop(&self) -> Result<()> {
        self.stream_with_backoff(&self.config.user_ws_url, StreamKind::User)
            .await
    }

    async fn stream_with_backoff(&self, url: &str, kind: StreamKind) -> Result<()> {
        let mut backoff = self.config.reconnect_backoff_initial;
        loop {
            match self.stream_once(url, kind).await {
                Ok(()) => backoff = self.config.reconnect_backoff_initial,
                Err(err) => {
                    warn!(url, ?kind, ?err, "ws disconnected");
                    sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(self.config.reconnect_backoff_max);
                }
            }
        }
    }

    async fn stream_once(&self, url: &str, kind: StreamKind) -> Result<()> {
        let (stream, _) = connect_async(url).await.with_context(|| format!("connect {url}"))?;
        info!(url, ?kind, "ws connected");
        let (mut write, mut read) = stream.split();
        self.send_subscribe(&mut write, kind).await?;
        let mut ping_interval = tokio::time::interval(self.config.ping_interval);
        let mut last_pong = tokio::time::Instant::now();

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if last_pong.elapsed() > self.config.pong_timeout {
                        anyhow::bail!("pong watchdog exceeded {:?}", self.config.pong_timeout);
                    }
                    write.send(Message::Text("{\"type\":\"PING\"}".to_string())).await?;
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if text.contains("PONG") {
                                last_pong = tokio::time::Instant::now();
                            }
                            self.handle_text(kind, text.as_bytes()).await?;
                        }
                        Some(Ok(Message::Binary(bytes))) => {
                            self.handle_text(kind, &bytes).await?;
                        }
                        Some(Ok(Message::Pong(_))) => {
                            last_pong = tokio::time::Instant::now();
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            write.send(Message::Pong(payload)).await?;
                        }
                        Some(Ok(Message::Close(frame))) => {
                            warn!(?frame, url, ?kind, "ws close frame");
                            return Ok(());
                        }
                        Some(Ok(_)) => {}
                        Some(Err(err)) => return Err(err.into()),
                        None => anyhow::bail!("stream ended"),
                    }
                }
            }
        }
    }

    async fn send_subscribe(
        &self,
        write: &mut futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
            Message,
        >,
        kind: StreamKind,
    ) -> Result<()> {
        let snapshot = self.control.snapshot().await;
        match kind {
            StreamKind::Market => {
                let asset_ids: Vec<String> = snapshot
                    .markets
                    .into_iter()
                    .filter(|market| market.enabled)
                    .flat_map(|market| market.token_ids)
                    .collect();
                if asset_ids.is_empty() {
                    warn!("no market asset ids configured for market ws subscribe");
                    return Ok(());
                }
                let payload = serde_json::to_string(&MarketSubscribePayload {
                    assets_ids: asset_ids,
                    kind: "market".to_string(),
                    custom_feature_enabled: true,
                })?;
                write.send(Message::Text(payload)).await?;
            }
            StreamKind::User => {
                let Some(auth) = self.user_auth.as_ref() else {
                    return Ok(());
                };
                let markets: Vec<String> = snapshot
                    .markets
                    .into_iter()
                    .filter(|market| market.enabled)
                    .filter_map(|market| market.condition_id)
                    .collect();
                if markets.is_empty() {
                    return Ok(());
                }
                let payload = serde_json::to_string(&UserSubscribePayload {
                    auth: UserSubscribeAuth {
                        api_key: auth.api_key.clone(),
                        secret: auth.secret.clone(),
                        passphrase: auth.passphrase.clone(),
                    },
                    markets,
                    kind: "user".to_string(),
                })?;
                write.send(Message::Text(payload)).await?;
            }
        }
        Ok(())
    }

    async fn handle_text(&self, kind: StreamKind, payload: &[u8]) -> Result<()> {
        match kind {
            StreamKind::Market => {
                if let Ok(book) = serde_json::from_slice::<BestBidAskEnvelope>(payload) {
                    let update = BookUpdate {
                        market_id: book.market_id,
                        best_bid: book.best_bid.parse().unwrap_or_default(),
                        best_ask: book.best_ask.parse().unwrap_or_default(),
                    };
                    let _ = self.book_tx.send(update).await;
                    return Ok(());
                }

                if let Ok(book) = serde_json::from_slice::<BookEnvelope>(payload) {
                    let best_bid = book
                        .bids
                        .first()
                        .and_then(|level| level.price.parse().ok())
                        .unwrap_or_default();
                    let best_ask = book
                        .asks
                        .first()
                        .and_then(|level| level.price.parse().ok())
                        .unwrap_or_default();
                    let update = BookUpdate {
                        market_id: book.market_id,
                        best_bid,
                        best_ask,
                    };
                    let _ = self.book_tx.send(update).await;
                    return Ok(());
                }
            }
            StreamKind::User => {
                if let Ok(trade) = serde_json::from_slice::<UserTradeEnvelope>(payload) {
                    self.handle_user_trade(trade).await;
                    return Ok(());
                }
                if let Ok(order) = serde_json::from_slice::<UserOrderEnvelope>(payload) {
                    if order.kind.eq_ignore_ascii_case("CANCELLATION") {
                        self.order_registry.remove(&order.id);
                    }
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    async fn handle_user_trade(&self, trade: UserTradeEnvelope) {
        if !trade.status.eq_ignore_ascii_case("MATCHED") {
            return;
        }

        if let Some(entry) = self.order_registry.get(&trade.taker_order_id) {
            let signed_qty = entry.side.sign() * trade.size.parse::<f64>().unwrap_or_default().round() as i64;
            let _ = self
                .shadow_tx
                .send(ShadowCommand::Fill {
                    state_key: entry.state_key,
                    filled_qty: signed_qty,
                    fill_price: trade.price.parse().unwrap_or_default(),
                })
                .await;
        }

        for maker in trade.maker_orders {
            if let Some(entry) = self.order_registry.get(&maker.order_id) {
                let signed_qty =
                    entry.side.sign() * maker.matched_amount.parse::<f64>().unwrap_or_default().round() as i64;
                let _ = self
                    .shadow_tx
                    .send(ShadowCommand::Fill {
                        state_key: entry.state_key,
                        filled_qty: signed_qty,
                        fill_price: maker.price.parse().unwrap_or_default(),
                    })
                    .await;
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum StreamKind {
    Market,
    User,
}

#[derive(Debug, Serialize)]
struct MarketSubscribePayload {
    assets_ids: Vec<String>,
    #[serde(rename = "type")]
    kind: String,
    custom_feature_enabled: bool,
}

#[derive(Debug, Serialize)]
struct UserSubscribePayload {
    auth: UserSubscribeAuth,
    markets: Vec<String>,
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Debug, Serialize)]
struct UserSubscribeAuth {
    #[serde(rename = "apiKey")]
    api_key: String,
    secret: String,
    passphrase: String,
}

#[derive(Debug, Deserialize)]
struct BestBidAskEnvelope {
    #[serde(rename = "market", alias = "condition_id")]
    market_id: String,
    best_bid: String,
    best_ask: String,
}

#[derive(Debug, Deserialize)]
struct BookEnvelope {
    #[serde(rename = "market", alias = "condition_id")]
    market_id: String,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
}

#[derive(Debug, Deserialize)]
struct BookLevel {
    price: String,
}

#[derive(Debug, Deserialize)]
struct UserTradeEnvelope {
    price: String,
    size: String,
    status: String,
    taker_order_id: String,
    #[serde(default)]
    maker_orders: Vec<UserMakerOrder>,
}

#[derive(Debug, Deserialize)]
struct UserMakerOrder {
    order_id: String,
    matched_amount: String,
    price: String,
}

#[derive(Debug, Deserialize)]
struct UserOrderEnvelope {
    id: String,
    #[serde(rename = "type")]
    kind: String,
}
