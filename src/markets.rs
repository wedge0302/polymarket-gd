use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;

use crate::control::{ControlPlane, MarketRegistration};

#[derive(Debug)]
pub struct MarketDiscovery {
    client: Client,
}

impl MarketDiscovery {
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .tcp_nodelay(true)
            .pool_max_idle_per_host(8)
            .build()
            .context("build reqwest client")?;
        Ok(Self { client })
    }

    pub async fn discover_btc_5min(&self, control: &ControlPlane) -> Result<Option<MarketRegistration>> {
        self.discover_updown_market(control, "btc-5min", "BTC", "5min", "btc-updown-5m-")
            .await
    }

    async fn discover_updown_market(
        &self,
        control: &ControlPlane,
        registration_id: &str,
        base_asset: &str,
        timeframe: &str,
        slug_prefix: &str,
    ) -> Result<Option<MarketRegistration>> {
        let mut cursor: Option<String> = None;
        let mut candidates = Vec::new();

        for _ in 0..32 {
            let mut request = self
                .client
                .get("https://gamma-api.polymarket.com/events/keyset")
                .query(&[("limit", "100"), ("active", "true"), ("closed", "false")]);
            if let Some(ref after_cursor) = cursor {
                request = request.query(&[("after_cursor", after_cursor)]);
            }

            let response = request
                .send()
                .await?
                .error_for_status()?
                .json::<GammaEventsPage>()
                .await?;

            for event in response.events {
                let slug = event.slug.clone().unwrap_or_default();
                let title = event.title.clone().unwrap_or_default();
                if slug.starts_with(slug_prefix) || title.to_ascii_lowercase().contains("bitcoin up or down") {
                    if let Some(market) = event.markets.first().cloned() {
                        candidates.push((event, market));
                    }
                }
            }

            cursor = response.next_cursor;
            if !candidates.is_empty() || cursor.is_none() {
                break;
            }
        }

        let now = Utc::now();
        let best = candidates
            .into_iter()
            .filter_map(|(event, market)| {
                let end_at = market
                    .end_date
                    .as_deref()
                    .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                    .map(|value| value.with_timezone(&Utc));
                Some((event, market, end_at?))
            })
            .filter(|(_, _, end_at)| *end_at >= now - chrono::TimeDelta::minutes(5))
            .min_by_key(|(_, _, end_at)| (*end_at - now).num_milliseconds().abs());

        let registration = best.map(|(event, market, _)| MarketRegistration {
            id: registration_id.to_string(),
            label: format!("{base_asset} {timeframe}"),
            base_asset: base_asset.to_string(),
            timeframe: timeframe.to_string(),
            enabled: true,
            condition_id: market.condition_id.clone(),
            token_ids: parse_json_array(market.clob_token_ids.clone()),
            slug: market.slug.clone().or(event.slug.clone()),
            question: market.question.clone().or(event.title.clone()),
            selectors: vec![
                base_asset.to_ascii_lowercase(),
                format!("{base_asset}-updown-5m").to_ascii_lowercase(),
                "bitcoin".to_string(),
                "up or down".to_string(),
                "5m".to_string(),
            ],
            source: "gamma-events-keyset".to_string(),
            updated_at: Utc::now(),
        });

        if let Some(market) = registration.clone() {
            control.upsert_market(market.clone()).await?;
        }

        Ok(registration)
    }
}

#[derive(Debug, Deserialize)]
struct GammaEventsPage {
    events: Vec<GammaEvent>,
    next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct GammaEvent {
    title: Option<String>,
    slug: Option<String>,
    #[serde(default)]
    markets: Vec<GammaMarket>,
}

#[derive(Debug, Clone, Deserialize)]
struct GammaMarket {
    #[serde(rename = "conditionId")]
    condition_id: Option<String>,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<String>,
    slug: Option<String>,
    question: Option<String>,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
}

fn parse_json_array(raw: Option<String>) -> Vec<String> {
    raw.and_then(|text| serde_json::from_str::<Vec<String>>(&text).ok())
        .unwrap_or_default()
}
