# Arbitrage Engine

An asyncio arbitrage engine for crypto spot markets. It finds **triangular** (within one exchange) and **cross-exchange** opportunities from live order books, re-validates each one before trading, and executes with strict slippage limits. It includes a paper-trading simulator, a Binance Spot gateway (testnet by default), and a web dashboard with a kill switch.

> **Status:** the simulator and paper trading are complete and covered by a self-test. The Binance gateway has been written for the testnet and has **not** been verified on a live account. See [Limitations](#limitations).

## Features

- **Triangular arbitrage.** All three-leg cycles `start -> A -> B -> start` are built automatically from the configured markets, in both directions.
- **Cross-exchange arbitrage.** Buy on one exchange and sell on another at the same time. Inventory is held on both sides, so no transfer is needed in the trade path. A configurable `--transfer-bps` charge accounts for the cost of rebalancing inventory later.
- **Depth-aware pricing.** Each opportunity is priced by walking the order book level by level, not by using the best bid or ask alone.
- **Cheap pre-filter.** A best-price check runs first. Depth can only make a price worse, so the filter never discards a profitable trade. Only cycles that contain the updated symbol are re-scanned.
- **Size ladder.** Trade sizes start at `--min-notional` and double up to `--max-notional`. The engine keeps the most profitable size.
- **Re-check before sending.** The trader recomputes the opportunity on fresh books. It then computes the worst case if every leg fills at its limit price, and rejects the trade if that loss exceeds `--max-loss-bps`.
- **IOC orders with a slippage cap.** Every order is immediate-or-cancel with a limit price derived from `--slippage-bps`.
- **Failure handling.**
  - If a triangular leg fails after the first, the position is unwound: leg 2 reverses leg 1, and leg 3 is retried with a wider limit.
  - Cross-exchange legs are sent concurrently. Fill mismatches are logged.
  - Locks are taken in a fixed order, so two exchanges cannot deadlock.
- **Staleness protection.** Old books, crossed books (bid >= ask), and opportunities that waited too long in the queue are discarded. When the queue is full, the oldest opportunity is dropped.
- **Resilient feeds.** WebSocket reconnects use exponential backoff (1 s to 30 s), and books are cleared on every disconnect.
- **Exchange rules.** Binance orders are rounded to `LOT_SIZE`, `PRICE_FILTER` and minimum-notional filters using `Decimal`.
- **Dashboard.** P&L chart, trade log, wallets, feed health, latency metrics, and a pause/resume control.

## Architecture

    Feed (WebSocket / simulator)
        -> OrderBook (in memory, bisect-sorted levels)
        -> Engine (finds opportunities on every book update)
        -> asyncio.Queue
        -> Trader (worker pool: re-check, limits, IOC execution, unwind)

| Component | Role |
|---|---|
| `OrderBook` | Price-to-size dicts plus sorted price lists. Supports snapshots, deltas, book walking and simulated IOC fills. |
| `Engine` | Called synchronously on each book update. Scans affected triangular cycles and cross-exchange pairs, then submits the best opportunity. |
| `Trader` | Workers take opportunities from the queue and execute them through an exchange `Gateway`. |
| `Gateway` | `PaperGateway` (simulated fills with random 4-15 ms latency) or `BinanceGateway` (signed REST orders). |
| Feeds | `SimFeed`, `BinanceFeed` (`depth10@100ms`), `OkxFeed` (`books5`), `BybitFeed` (`orderbook.50`, snapshot and delta). |

## Quick start

Python 3.10 or newer is required. The core engine and simulator use only the standard library.

Run the built-in checks first:

    python arbbot.py --selftest

Run the simulation with two simulated exchanges and paper trading for 20 seconds:

    python arbbot.py

Run until Ctrl+C:

    python arbbot.py --duration 0

## Live market data

Install the optional packages (`websockets` is required for live data, `uvloop` and `orjson` are speed-ups):

    pip install -r requirements.txt

Stream real order books from three exchanges and paper-trade against them:

    python arbbot.py --mode live --exchanges binance,okx,bybit --duration 60

In live mode, exchanges without a trading gateway produce **signals only**. They are logged but never traded.

## Trading modes

| `--trade` | Behavior |
|---|---|
| `paper` (default) | Simulated fills against the current order books, with simulated latency and fees. |
| `binance-testnet` | Real orders on the Binance Spot testnet. |
| `binance-mainnet` | Real orders with real money. Requires `ARB_CONFIRM_MAINNET=YES`. |

Binance trading reads credentials from the environment:

    export BINANCE_API_KEY="..."
    export BINANCE_API_SECRET="..."
    python arbbot.py --mode live --exchanges binance --trade binance-testnet

On Windows PowerShell, use `$env:BINANCE_API_KEY="..."` instead of `export`.

Start on the testnet with small sizes. Never give the key withdrawal permissions.

## Web dashboard

`server.py` runs the bot and a FastAPI dashboard in a single process and a single event loop, so request handlers read bot state without data races.

    pip install fastapi uvicorn
    python server.py

Open `http://127.0.0.1:8080/` and sign in as `admin`. If `DASHBOARD_PASSWORD` is not set, a random password is generated and printed in the log.

The dashboard reads its page from `templates/dashboard.html`.

| Variable | Default | Purpose |
|---|---|---|
| `ARB_ARGS` | `--mode sim --duration 0` | Bot arguments, same as the command line of `arbbot.py`. |
| `DASHBOARD_USER` | `admin` | Login name. |
| `DASHBOARD_PASSWORD` | generated | Login password. |
| `DASHBOARD_TITLE` | `Arbitrage Bot` | Page title. |
| `DASHBOARD_HOST` | `0.0.0.0` | Bind address. Use `127.0.0.1` to keep it local. |
| `DASHBOARD_PORT_INTERNAL` | `8080` | Port the server listens on. |
| `DASHBOARD_PORT` | `8080` | Port shown in the startup log message. |

Example with a custom password and live feeds:

    export DASHBOARD_PASSWORD="choose-a-password"
    export ARB_ARGS="--mode live --exchanges binance,okx,bybit --duration 0"
    python server.py

Dashboard and API behavior:

- HTTP Basic authentication on every page and API route. Password comparison uses `secrets.compare_digest`.
- `POST /api/pause` and `POST /api/resume` require an `X-Requested-With: dashboard` header, which blocks cross-site request forgery.
- A red banner appears in real-money mode, and resuming asks for confirmation.
- An emergency-stop state pauses trading when the loss limit is exceeded. It stays paused until you resume manually.
- `GET /health` is unauthenticated and returns the bot status and uptime, for use with uptime monitors.
- All values rendered in the page are HTML-escaped.

## Configuration

| Flag | Default | Description |
|---|---|---|
| `--mode` | `sim` | `sim` or `live`. |
| `--exchanges` | `binance,okx,bybit` | Exchanges for live mode. |
| `--trade` | `paper` | `paper`, `binance-testnet` or `binance-mainnet`. |
| `--symbols` | `BTCUSDT,ETHUSDT,ETHBTC,SOLUSDT,SOLBTC,SOLETH` | Markets to watch. |
| `--start-asset` | `USDT` | Asset that cycles start and end in. |
| `--duration` | `20` | Seconds to run. `0` runs until Ctrl+C. |
| `--fee-bps` | `10` | Taker fee per leg. |
| `--min-profit-bps` | `5` | Minimum net profit for an opportunity. |
| `--min-notional` / `--max-notional` | `50` / `5000` | Smallest and largest trade size, in the start asset. |
| `--slippage-bps` | `3` | Slippage tolerance per leg (sets the order limit price). |
| `--max-loss-bps` | `8` | Reject a trade if the worst case at limit prices is worse than this. |
| `--unwind-slippage-bps` | `50` | Limit width for emergency unwinds. |
| `--transfer-bps` | `2` | Cost charged to cross-exchange trades for rebalancing inventory. |
| `--max-age-ms` | `250` | Order books older than this are ignored. |
| `--max-opp-age-ms` | `100` | Opportunities older than this in the queue are dropped. |
| `--cooldown-ms` | `25` | Minimum gap between signals for the same route. |
| `--workers` / `--queue-size` | `4` / `8` | Trader concurrency and queue length. |
| `--no-cross` | off | Disable cross-exchange arbitrage. |
| `--sim-rate` | `600` | Simulated book updates per second per exchange. |
| `--report-sec` | `5` | Interval of the statistics line. |
| `--selftest` | off | Run the built-in checks and exit. |
| `-v` | off | Debug logging. |

## Metrics

The bot logs a statistics line every `--report-sec` seconds: book updates per second, scan latency (p50 and p99, in microseconds), opportunities found, trades executed (wins and losses), and counters for stale, re-check-rejected, slippage-rejected, dropped and aborted trades, plus P&L per asset. At shutdown it also prints the median and p99 of detect-to-fill time.

In paper mode, detect-to-fill time includes the simulated 4-15 ms network delay per order. It is not a measurement of real exchange latency.

## Self-test

`python arbbot.py --selftest` verifies:

- Order book snapshots, deltas, level removal and crossed-book rejection.
- Depth walking for buys and sells, and insufficient-depth handling.
- Liquidity consumption by simulated fills.
- Triangular cycle detection and the profit formula, including fees.
- That the worst-case limit-price result is worse than the expected result.
- That no opportunity is found when prices are consistent.
- That stale books are rejected.
- Paper IOC fills, fees, and that an IOC below the market does not fill.

## Project structure

    arbbot.py                 Engine, order books, feeds, gateways, trader, CLI, self-test
    server.py                 FastAPI dashboard and bot in one process
    templates/dashboard.html  Single-page dashboard (Tailwind CDN, vanilla JS)
    requirements.txt          Optional and dashboard dependencies

## Limitations

- **Simulator results prove nothing about profit.** The simulator injects artificial price dislocations of 40-90 bps. Simulated P&L only shows that the pipeline works.
- **Spot markets only.** There are no futures, margin or withdrawals.
- **Real orders are supported on Binance only.** OKX and Bybit are market-data feeds. In non-paper modes they produce signals only.
- **No automatic inventory rebalancing.** Cross-exchange trading assumes balances on both exchanges. Rebalancing cost is modeled with `--transfer-bps`, not executed.
- **The Binance gateway is unverified on a live account.** Test on the testnet with small sizes first.
- **Python limits latency.** The engine is built for correctness and low overhead, but a Python event loop and REST order placement cannot match colocated, compiled trading systems.

## Disclaimer

This software is provided for educational and research purposes. Trading involves a substantial risk of loss. Past or simulated results do not guarantee future results. You are responsible for compliance with exchange terms and local regulations. Use at your own risk.