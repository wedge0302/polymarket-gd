# Polymarket Copybot

一个面向 Polymarket 的 Rust 跟单机器人骨架，当前默认以 `BTC 5min` 市场为首个落地场景。

## 当前架构

- 目标地址成交源：`Data API /trades` 增量轮询
- 市场价格源：CLOB `market` WebSocket
- 自身订单/成交回报：CLOB `user` WebSocket
- 状态核心：`Shadow Ledger`
- 下单执行：官方 Rust SDK `polymarket_client_sdk_v2`
- 控制面：本地 JSON 状态文件 + Axum 看板

## 为什么改成轮询

Polymarket 当前公开文档里：

- `market channel` 只提供订单簿和价格事件
- `user channel` 只提供当前认证账户自己的订单和成交
- 没有公开、稳定、可依赖的“第三方钱包成交 WebSocket”

因此本项目把“跟谁”的输入层固定为 `Data API /trades` 轮询，把低延迟预算优先留给：

- 盘口感知
- 真正的下单和撤单
- 本地状态机更新

## 代码结构

- `src/main.rs`
  负责装配全局组件、启动任务和控制面。
- `src/network/target_trades.rs`
  目标地址成交轮询器。按地址增量拉取 `/trades`，做时间戳游标和去重缓存。
- `src/network/ws.rs`
  盘口和用户通道 WebSocket 管理器。
- `src/network/orderbook.rs`
  本地 top-of-book 缓存与 stale price 判断。
- `src/shadow.rs`
  Shadow Ledger，维护 `Ideal_Position` / `Actual_Position` / `Sync_Mode`。
- `src/execution/mod.rs`
  真正的下单、撤单、TTL 回收、价格漂移撤单。
- `src/risk.rs`
  独立止盈与 trailing stop 逻辑。
- `src/control.rs`
  跟单地址、用户地址、策略、市场注册的控制面状态。
- `src/dashboard.rs`
  本地管理面板和 REST API。
- `src/markets.rs`
  BTC 5min 市场自动发现。

## 轮询策略

- 默认轮询间隔：`400ms`
- 默认单次拉取：`100` 条成交
- 轮询粒度：按启用中的 source address 拉取
- 市场过滤：只请求控制面里启用市场的 `condition_id`
- 增量机制：
  - 为每个 source address 保存本地最新时间戳游标
  - 同时间戳下再用有限大小去重缓存兜底
- 启动补仓：
  - 可选读取 `/positions`
  - 只用于初始化 `Ideal_Position`

这样做的目标是：

- 不依赖 undocumented stream
- 不全量扫历史
- 在单市场/少量地址场景下尽量贴近实时

## 环境变量

### 基础

- `POLY_DASHBOARD_BIND`
  默认 `127.0.0.1:8080`
- `POLY_CONTROL_STATE_FILE`
  默认 `./data/control-plane.json`

### 轮询

- `POLY_BOOTSTRAP_POSITIONS`
  默认 `true`
- `POLY_BOOTSTRAP_POSITIONS_LIMIT`
  默认 `50`
- `POLY_TRADE_BATCH_SIZE`
  默认 `100`
- `POLY_TRADE_DEDUPE_CACHE_SIZE`
  默认 `4096`

### 真实执行

- `POLY_EXECUTION_ENABLED`
  默认 `false`
- `POLYMARKET_PRIVATE_KEY`
  跟单账户私钥
- `POLY_PRIVATE_KEY_ENV`
  默认读取 `POLYMARKET_PRIVATE_KEY`
- `POLY_FUNDER_ADDRESS`
  代理钱包/资金地址模式下可配置
- `POLY_SIGNATURE_TYPE`
  `0=EOA` `1=Proxy` `2=GnosisSafe` `3=Poly1271`
- `POLY_CLOB_API_URL`
  默认 `https://clob-v2.polymarket.com/`
- `POLY_ENABLE_USER_WS`
  默认 `true`

## 本地运行

```bash
cargo check
cargo run
```

启动后：

- 看板地址：`http://127.0.0.1:8080`
- 控制面状态文件：`./data/control-plane.json`

## 默认预置

默认控制面已经预置：

- 被跟单地址：`0xf6ab262d360c85f98782b84a04db35c6e18d3579`
- 市场：`btc-5min`
- 策略：`BTC 5min Default`

程序启动时会自动尝试从 Gamma 重新发现最新的 `btc-updown-5m-*` 市场并回填 `condition_id` 与 token ids。

## REST 控制面

- `GET /api/dashboard`
- `GET/POST /api/follower-accounts`
- `GET/POST /api/source-addresses`
- `GET/POST /api/strategies`
- `GET /api/markets`
- `POST /api/markets/discover/btc-5min`

## 推送到 GitHub 前建议

1. 把真实私钥放到本地环境变量，不要写进仓库。
2. 先确认 `data/control-plane.json` 里没有真实账户信息。
3. 如果准备开源，建议把默认 demo 地址保留，把真实地址通过看板再添加。

## 已知限制

- 目标地址成交源目前不是 WebSocket，而是 Data API 轮询。
- 轮询延迟取决于官方 `/trades` 刷新速度和 Cloudflare 节流策略。
- 当前优先覆盖单市场、少量地址、高频跟单场景；批量地址扩展还需要进一步做统一调度和节流编排。
