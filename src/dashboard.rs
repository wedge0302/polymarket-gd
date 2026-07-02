use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::{
    extract::State,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};

use crate::{
    control::{
        ControlPlane, CopyStrategyDraft, CreateFollowerAccountRequest, CreateSourceAddressRequest,
    },
    markets::MarketDiscovery,
    shadow::ShadowLedger,
};

#[derive(Clone)]
pub struct DashboardState {
    pub control: Arc<ControlPlane>,
    pub ledger: Arc<ShadowLedger>,
}

pub async fn serve(addr: SocketAddr, state: DashboardState) -> Result<()> {
    let app = Router::new()
        .route("/", get(index))
        .route("/api/dashboard", get(get_dashboard))
        .route("/api/follower-accounts", get(list_follower_accounts).post(create_follower_account))
        .route("/api/source-addresses", get(list_source_addresses).post(create_source_address))
        .route("/api/strategies", get(list_strategies).post(create_strategy))
        .route("/api/markets", get(list_markets))
        .route("/api/markets/discover/btc-5min", post(discover_btc_5min))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> impl IntoResponse {
    Html(DASHBOARD_HTML)
}

async fn get_dashboard(State(state): State<DashboardState>) -> Json<serde_json::Value> {
    Json(serde_json::to_value(state.control.dashboard_overview(&state.ledger).await).unwrap_or_default())
}

async fn list_follower_accounts(State(state): State<DashboardState>) -> Json<serde_json::Value> {
    let snapshot = state.control.snapshot().await;
    Json(serde_json::json!(snapshot.follower_accounts))
}

async fn create_follower_account(
    State(state): State<DashboardState>,
    Json(payload): Json<CreateFollowerAccountRequest>,
) -> Json<serde_json::Value> {
    let created = state
        .control
        .add_follower_account(payload.label, payload.address)
        .await;
    Json(result_json(created))
}

async fn list_source_addresses(State(state): State<DashboardState>) -> Json<serde_json::Value> {
    let snapshot = state.control.snapshot().await;
    Json(serde_json::json!(snapshot.source_addresses))
}

async fn create_source_address(
    State(state): State<DashboardState>,
    Json(payload): Json<CreateSourceAddressRequest>,
) -> Json<serde_json::Value> {
    let created = state.control.add_source_address(payload.label, payload.address).await;
    Json(result_json(created))
}

async fn list_strategies(State(state): State<DashboardState>) -> Json<serde_json::Value> {
    let snapshot = state.control.snapshot().await;
    Json(serde_json::json!(snapshot.strategies))
}

async fn create_strategy(
    State(state): State<DashboardState>,
    Json(payload): Json<CopyStrategyDraft>,
) -> Json<serde_json::Value> {
    let created = state.control.add_strategy(payload).await;
    Json(result_json(created))
}

async fn list_markets(State(state): State<DashboardState>) -> Json<serde_json::Value> {
    let snapshot = state.control.snapshot().await;
    Json(serde_json::json!(snapshot.markets))
}

async fn discover_btc_5min(State(state): State<DashboardState>) -> Json<serde_json::Value> {
    let result = async {
        let discovery = MarketDiscovery::new()?;
        discovery.discover_btc_5min(&state.control).await
    }
    .await;
    Json(result_json(result))
}

fn result_json<T: serde::Serialize>(result: Result<T>) -> serde_json::Value {
    match result {
        Ok(value) => serde_json::json!({ "ok": true, "data": value }),
        Err(err) => serde_json::json!({ "ok": false, "error": err.to_string() }),
    }
}

const DASHBOARD_HTML: &str = r#"<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Polymarket Copybot Dashboard</title>
  <style>
    :root {
      --bg: #f7f4ed;
      --panel: rgba(255,255,255,0.82);
      --ink: #1f1e1a;
      --muted: #746c5f;
      --accent: #0f766e;
      --accent-2: #c2410c;
      --line: rgba(31,30,26,0.12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Georgia, "Iowan Old Style", "Palatino Linotype", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(15,118,110,0.16), transparent 28%),
        radial-gradient(circle at bottom right, rgba(194,65,12,0.14), transparent 24%),
        linear-gradient(180deg, #fdfbf7 0%, var(--bg) 100%);
    }
    main { max-width: 1180px; margin: 0 auto; padding: 32px 20px 60px; }
    h1 { font-size: 44px; margin: 0 0 8px; }
    p { color: var(--muted); margin: 0 0 20px; }
    .grid { display: grid; gap: 16px; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 10px 30px rgba(31,30,26,0.06);
      backdrop-filter: blur(10px);
    }
    .hero { margin-bottom: 18px; }
    .stat { font-size: 32px; margin: 10px 0 0; }
    button {
      border: 0;
      background: var(--accent);
      color: white;
      border-radius: 999px;
      padding: 10px 16px;
      cursor: pointer;
    }
    pre {
      white-space: pre-wrap;
      word-break: break-word;
      font-size: 13px;
      line-height: 1.5;
      background: rgba(31,30,26,0.04);
      border-radius: 12px;
      padding: 12px;
      margin: 0;
      max-height: 360px;
      overflow: auto;
    }
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <h1>Copy Trading Control Board</h1>
      <p>本地账户、被跟单地址、策略、市场注册和仓位快照都在这里统一查看。</p>
    </section>
    <section class="grid">
      <div class="panel"><div>本地账户</div><div id="followers" class="stat">-</div></div>
      <div class="panel"><div>被跟单地址</div><div id="sources" class="stat">-</div></div>
      <div class="panel"><div>策略</div><div id="strategies" class="stat">-</div></div>
      <div class="panel"><div>市场</div><div id="markets" class="stat">-</div></div>
    </section>
    <section class="grid" style="margin-top: 18px;">
      <div class="panel">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
          <strong>总览 JSON</strong>
          <button id="refresh">刷新</button>
        </div>
        <pre id="payload">loading...</pre>
      </div>
      <div class="panel">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
          <strong>BTC 5min 自动发现</strong>
          <button id="discover">重新发现</button>
        </div>
        <pre id="discover-output">尚未执行</pre>
      </div>
    </section>
  </main>
  <script>
    async function refresh() {
      const res = await fetch('/api/dashboard');
      const data = await res.json();
      document.getElementById('followers').textContent = data.follower_accounts.length;
      document.getElementById('sources').textContent = data.source_addresses.length;
      document.getElementById('strategies').textContent = data.strategies.length;
      document.getElementById('markets').textContent = data.markets.length;
      document.getElementById('payload').textContent = JSON.stringify(data, null, 2);
    }
    document.getElementById('refresh').addEventListener('click', refresh);
    document.getElementById('discover').addEventListener('click', async () => {
      const res = await fetch('/api/markets/discover/btc-5min', { method: 'POST' });
      const data = await res.json();
      document.getElementById('discover-output').textContent = JSON.stringify(data, null, 2);
      refresh();
    });
    refresh();
  </script>
</body>
</html>"#;
