use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tracing::debug;

use crate::types::{BookUpdate, RiskEvent};

#[derive(Debug, Clone)]
pub struct TopOfBook {
    pub best_bid: f64,
    pub best_ask: f64,
}

#[derive(Debug, Default)]
pub struct OrderBookCache {
    books: Arc<DashMap<String, TopOfBook>>,
}

impl OrderBookCache {
    pub async fn run(&self, mut rx: mpsc::Receiver<BookUpdate>, risk_tx: mpsc::Sender<RiskEvent>) {
        while let Some(update) = rx.recv().await {
            self.books.insert(
                update.market_id.clone(),
                TopOfBook {
                    best_bid: update.best_bid,
                    best_ask: update.best_ask,
                },
            );
            let _ = risk_tx.send(RiskEvent::Book(update)).await;
        }
    }

    pub fn best_bid_ask(&self, market_id: &str) -> Option<TopOfBook> {
        self.books.get(market_id).map(|entry| entry.clone())
    }

    pub fn mid_price(&self, market_id: &str) -> Option<f64> {
        self.best_bid_ask(market_id)
            .map(|top| (top.best_bid + top.best_ask) / 2.0)
    }

    pub fn is_stale_price(&self, market_id: &str, order_price: f64, stale_requote_bps: u32) -> bool {
        let Some(top) = self.best_bid_ask(market_id) else {
            return false;
        };
        let mid = (top.best_bid + top.best_ask) / 2.0;
        if mid <= f64::EPSILON {
            return false;
        }
        let drift_bps = ((mid - order_price).abs() / mid) * 10_000.0;
        debug!(market_id, order_price, mid, drift_bps, "stale check");
        drift_bps >= stale_requote_bps as f64
    }
}
