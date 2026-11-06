#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Арбитражный движок: треугольный (внутри биржи) и межбиржевой арбитраж.  Python 3.10+.

Запуск (всё из коробки, только стандартная библиотека):
    python arbbot.py --selftest                      # проверки математики и стакана
    python arbbot.py                                 # симуляция: 2 «биржи», бумажная торговля, 20 секунд
    python arbbot.py --duration 0                    # работать до Ctrl+C

Реальные рыночные данные (pip install websockets; по желанию uvloop orjson):
    python arbbot.py --mode live --exchanges binance,okx,bybit --duration 60
Реальные ордера Binance — сначала ТЕСТНЕТ (ключи: BINANCE_API_KEY, BINANCE_API_SECRET):
    python arbbot.py --mode live --exchanges binance --trade binance-testnet

Архитектура:
    Feed (WebSocket/симулятор) -> OrderBook (в памяти) -> Engine (поиск окон) -> Queue -> Trader (исполнение)
    * Engine пересчитывает только цепочки, затронутые обновлённым символом, и сначала делает дешёвый
      предфильтр по лучшим ценам (глубина может только ухудшить цену — предфильтр не теряет сделки).
    * Trader перед отправкой заново проверяет окно на свежих стаканах, считает худший случай по лимитным
      ценам, шлёт IOC-ордера с ограничением проскальзывания и страхует цепочку при сбое ноги.
"""
from __future__ import annotations

import argparse
import asyncio
import bisect
import hashlib
import hmac
import json
import logging
import math
import os
import random
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Callable, Deque, Dict, Iterator, List, Optional, Tuple

try:  # необязательные ускорители
    import orjson

    _loads = orjson.loads
except ImportError:
    _loads = json.loads
try:
    import uvloop
except ImportError:
    uvloop = None
try:
    import websockets
except ImportError:
    websockets = None

log = logging.getLogger("arb")
BUY, SELL = "buy", "sell"
QUOTES = ("USDT", "USDC", "BTC", "ETH")
now_ns = time.perf_counter_ns


def split_symbol(sym: str) -> Tuple[str, str]:
    """'ETHBTC' -> ('ETH', 'BTC')"""
    for q in QUOTES:
        if sym.endswith(q) and len(sym) > len(q):
            return sym[: -len(q)], q
    raise ValueError(f"не удалось разобрать символ {sym}")


# ================================================================ СТАКАН ================================================================

class OrderBook:
    """Стакан в памяти: dict цена->объём + отсортированные списки цен (bisect). Операции O(log n) поиск, O(n) вставка уровня."""

    __slots__ = ("symbol", "bids", "asks", "_bp", "_ap", "ts_ns")

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self._bp: List[float] = []  # цены bid по возрастанию (лучшая — последняя)
        self._ap: List[float] = []  # цены ask по возрастанию (лучшая — первая)
        self.ts_ns = 0

    def clear(self) -> None:
        self.bids.clear(); self.asks.clear(); self._bp.clear(); self._ap.clear(); self.ts_ns = 0

    def apply_snapshot(self, bids, asks) -> None:
        self.bids = {p: q for p, q in bids if q > 0}
        self.asks = {p: q for p, q in asks if q > 0}
        self._bp = sorted(self.bids)
        self._ap = sorted(self.asks)
        self.ts_ns = now_ns()

    def apply_delta(self, bids, asks) -> None:
        """Инкрементальное обновление: объём 0 удаляет уровень."""
        for p, q in bids:
            self._upd(self.bids, self._bp, p, q)
        for p, q in asks:
            self._upd(self.asks, self._ap, p, q)
        self.ts_ns = now_ns()

    @staticmethod
    def _upd(d: Dict[float, float], prices: List[float], p: float, q: float) -> None:
        if q <= 0:
            if p in d:
                del d[p]
                del prices[bisect.bisect_left(prices, p)]
        else:
            if p not in d:
                bisect.insort(prices, p)
            d[p] = q

    def fresh(self, t_ns: int, max_age_ns: int) -> bool:
        """Данные не устарели, стакан непустой и не «перекрещен» (bid < ask)."""
        return bool(self.ts_ns and t_ns - self.ts_ns <= max_age_ns and self._ap and self._bp and self._bp[-1] < self._ap[0])

    def best_bid(self) -> float: return self._bp[-1]

    def best_ask(self) -> float: return self._ap[0]

    # ---- «прогулка» по стакану: учёт глубины, а не только лучшей цены ----
    def buy_with_quote(self, budget: float) -> Optional[Tuple[float, float]]:
        """Потратить budget котируемой валюты по asks -> (получено базовой (брутто), худшая цена) | None, если глубины мало."""
        rem, base, worst, asks = budget, 0.0, 0.0, self.asks
        for p in self._ap:
            q = asks[p]
            cost = p * q
            if cost >= rem:
                base += rem / p; worst = p
                return base, worst
            base += q; rem -= cost; worst = p
        return None

    def sell_base(self, qty: float) -> Optional[Tuple[float, float]]:
        """Продать qty базовой по bids -> (получено котируемой (брутто), худшая цена) | None."""
        rem, quote, worst, bids = qty, 0.0, 0.0, self.bids
        for p in reversed(self._bp):
            q = bids[p]
            if q >= rem:
                return quote + rem * p, p
            quote += q * p; rem -= q; worst = p
        return None

    def buy_base(self, qty: float) -> Optional[Tuple[float, float]]:
        """Купить qty базовой по asks -> (стоимость в котируемой, худшая цена) | None."""
        rem, cost, asks = qty, 0.0, self.asks
        for p in self._ap:
            q = asks[p]
            if q >= rem:
                return cost + rem * p, p
            cost += q * p; rem -= q
        return None

    def take(self, side: str, qty: float, limit: float, consume: bool = False) -> Tuple[float, float]:
        """IOC-исполнение для бумажной торговли: (исполнено базовой, сумма в котируемой) с учётом лимитной цены.
        consume=True — забранная ликвидность убирается из стакана (как на реальной бирже)."""
        filled = amount = 0.0
        touched: List[Tuple[float, float]] = []
        if side == BUY:
            for p in self._ap:
                if p > limit or filled >= qty: break
                t = min(self.asks[p], qty - filled); filled += t; amount += t * p; touched.append((p, self.asks[p] - t))
            if consume:
                for p, left in touched: self._upd(self.asks, self._ap, p, left if left > 1e-12 else 0.0)
        else:
            for p in reversed(self._bp):
                if p < limit or filled >= qty: break
                t = min(self.bids[p], qty - filled); filled += t; amount += t * p; touched.append((p, self.bids[p] - t))
            if consume:
                for p, left in touched: self._upd(self.bids, self._bp, p, left if left > 1e-12 else 0.0)
        return filled, amount


# ================================================================ МОДЕЛЬ ================================================================

@dataclass(frozen=True)
class Market:
    symbol: str
    base: str
    quote: str


@dataclass(frozen=True)
class LegSpec:
    symbol: str
    side: str
    asset_in: str
    asset_out: str


@dataclass
class LegPlan:
    exchange: str
    symbol: str
    side: str
    qty: float        # объём ордера в базовой валюте
    limit: float      # худшая цена, по которой план «дошёл» до нужного объёма
    in_amt: float     # сколько отдаём (asset_in)
    out_amt: float    # сколько получим (asset_out), уже за вычетом комиссии
    asset_in: str
    asset_out: str


@dataclass
class Opportunity:
    kind: str                       # "tri" | "xex"
    exchange: str
    legs: List[LegPlan]
    start_asset: str
    start_amt: float
    end_amt: float
    created_ns: int
    key: str
    recheck: Callable[[], Optional["Opportunity"]]

    @property
    def profit(self) -> float: return self.end_amt - self.start_amt

    @property
    def profit_bps(self) -> float: return (self.end_amt / self.start_amt - 1.0) * 1e4


@dataclass
class Fill:
    filled_qty: float = 0.0
    spent: float = 0.0      # в asset_in
    received: float = 0.0   # в asset_out, за вычетом комиссии
    avg_price: float = 0.0


class Wallet(dict):
    def __missing__(self, key): return 0.0


class Gateway(ABC):
    @abstractmethod
    async def ioc(self, ex: "Exchange", symbol: str, side: str, qty: float, limit: float) -> Fill: ...


class Exchange:
    def __init__(self, name: str, symbols: List[str], fee: float, start_asset: str, wallet: Optional[Wallet] = None,
                 gateway: Optional[Gateway] = None):
        self.name, self.fee, self.wallet, self.gateway = name, fee, wallet, gateway
        self.fill_hooks: List[Callable[[str], None]] = []  # вызываются после исполнения ордера (нужно симулятору)
        self.markets: Dict[str, Market] = {s: Market(s, *split_symbol(s)) for s in symbols}
        self.books: Dict[str, OrderBook] = {s: OrderBook(s) for s in symbols}
        self.lock = asyncio.Lock()
        self.cycles = build_cycles(self.markets, start_asset)
        self.cycles_by_symbol: Dict[str, List[Tuple[LegSpec, ...]]] = defaultdict(list)
        for c in self.cycles:
            for leg in c:
                self.cycles_by_symbol[leg.symbol].append(c)


def build_cycles(markets: Dict[str, Market], start: str) -> List[Tuple[LegSpec, ...]]:
    """Все 3-цепочки start -> A -> B -> start по имеющимся рынкам (обе стороны обхода)."""
    adj: Dict[str, List[LegSpec]] = defaultdict(list)
    for m in markets.values():
        adj[m.quote].append(LegSpec(m.symbol, BUY, m.quote, m.base))
        adj[m.base].append(LegSpec(m.symbol, SELL, m.base, m.quote))
    cycles = []
    for l1 in adj[start]:
        for l2 in adj[l1.asset_out]:
            if l2.symbol == l1.symbol:
                continue
            for l3 in adj[l2.asset_out]:
                if l3.asset_out == start and l3.symbol not in (l1.symbol, l2.symbol):
                    cycles.append((l1, l2, l3))
    return cycles


# ================================================================ СТАТИСТИКА ================================================================

class Stats:
    def __init__(self):
        self.t0 = time.time(); self._last = (self.t0, 0)
        self.ticks = self.opps = self.dropped = self.stale = self.rej_recheck = self.rej_slip = 0
        self.executed = self.wins = self.losses = self.aborted = self.unwound = self.partial = self.signals = 0
        self.pnl: Dict[str, float] = defaultdict(float)
        self.scan_us: Deque[int] = deque(maxlen=20000)
        self.exec_ms: Deque[float] = deque(maxlen=2000)

    @staticmethod
    def _pct(data, p):
        if not data: return 0
        s = sorted(data)
        return s[min(len(s) - 1, int(len(s) * p))]

    def line(self) -> str:
        t = time.time()
        rate = (self.ticks - self._last[1]) / max(1e-9, t - self._last[0])
        self._last = (t, self.ticks)
        pnl = " ".join(f"{a}={v:+.4f}" for a, v in self.pnl.items()) or "0"
        return (f"[{t - self.t0:5.0f}s] тиков/с={rate:,.0f} скан p50={self._pct(self.scan_us, .5)}µs p99={self._pct(self.scan_us, .99)}µs"
                f" | окон={self.opps} исп={self.executed} (+{self.wins}/-{self.losses}) устар={self.stale} пересчёт✗={self.rej_recheck}"
                f" проскальз✗={self.rej_slip} сброшено={self.dropped} прервано={self.aborted} | PnL {pnl}")


# ================================================================ ДВИЖОК ПОИСКА ================================================================

class Engine:
    def __init__(self, exchanges: Dict[str, Exchange], cfg, queue: "asyncio.Queue[Opportunity]", stats: Stats):
        self.ex, self.cfg, self.queue, self.stats = exchanges, cfg, queue, stats
        self.max_age_ns = int(cfg.max_age_ms * 1e6)
        self.cooldown_ns = int(cfg.cooldown_ms * 1e6)
        self.until: Dict[str, int] = {}
        self.sizes = self._sizes(cfg.min_notional, cfg.max_notional)
        self.tri_threshold = 1.0 + cfg.min_profit_bps / 1e4
        self.xex_threshold = 1.0 + (cfg.min_profit_bps + cfg.transfer_bps) / 1e4

    @staticmethod
    def _sizes(lo: float, hi: float) -> List[float]:
        out, s = [], lo
        while s < hi:
            out.append(s); s *= 2
        out.append(hi)
        return out

    # ---- точка входа: вызывается синхронно из фида на каждое обновление стакана ----
    def on_update(self, ex_name: str, symbol: str) -> None:
        t0 = now_ns()
        self.stats.ticks += 1
        ex = self.ex[ex_name]
        best: Optional[Opportunity] = None
        for cyc in ex.cycles_by_symbol.get(symbol, ()):
            opp = self._scan_tri(ex, cyc, t0)
            if opp and (best is None or opp.profit > best.profit):
                best = opp
        if not self.cfg.no_cross:
            for other in self.ex.values():
                if other is ex or symbol not in other.books:
                    continue
                for a, b in ((ex, other), (other, ex)):
                    opp = self._scan_xex(a, b, symbol, t0)
                    if opp and (best is None or opp.profit > best.profit):
                        best = opp
        if best and t0 >= self.until.get(best.key, 0):
            self.until[best.key] = t0 + self.cooldown_ns
            self._submit(best)
        self.stats.scan_us.append((now_ns() - t0) // 1000)

    def _submit(self, opp: Opportunity) -> None:
        self.stats.opps += 1
        if self.queue.full():  # новее — ценнее: выбрасываем самое старое
            try:
                self.queue.get_nowait(); self.stats.dropped += 1
            except asyncio.QueueEmpty:
                pass
        self.queue.put_nowait(opp)

    # ---- треугольный арбитраж ----
    def _scan_tri(self, ex: Exchange, cyc, t: int) -> Optional[Opportunity]:
        rate, k = 1.0, 1.0 - ex.fee
        for leg in cyc:  # дешёвый предфильтр по лучшим ценам
            b = ex.books[leg.symbol]
            if not b.fresh(t, self.max_age_ns):
                return None
            rate *= (k / b._ap[0]) if leg.side == BUY else (b._bp[-1] * k)
        if rate < self.tri_threshold:
            return None
        best = None
        for size in self.sizes:
            opp = self.eval_tri(ex, cyc, size, t)
            if opp is None or opp.profit_bps < self.cfg.min_profit_bps:
                break  # с ростом объёма цена только ухудшается
            if best is None or opp.profit > best.profit:
                best = opp
        return best

    def eval_tri(self, ex: Exchange, cyc, start_amt: float, t: int) -> Optional[Opportunity]:
        amt, plans, k = start_amt, [], 1.0 - ex.fee
        for leg in cyc:
            b = ex.books[leg.symbol]
            if not b.fresh(t, self.max_age_ns):
                return None
            if leg.side == BUY:
                r = b.buy_with_quote(amt)
                if r is None: return None
                base, worst = r
                out = base * k
                plans.append(LegPlan(ex.name, leg.symbol, BUY, base, worst, amt, out, leg.asset_in, leg.asset_out))
            else:
                r = b.sell_base(amt)
                if r is None: return None
                quote, worst = r
                out = quote * k
                plans.append(LegPlan(ex.name, leg.symbol, SELL, amt, worst, amt, out, leg.asset_in, leg.asset_out))
            amt = out
        key = "tri:%s:%s" % (ex.name, ">".join(p.symbol + p.side[0] for p in plans))
        return Opportunity("tri", ex.name, plans, cyc[0].asset_in, start_amt, amt, t, key,
                           lambda: self.eval_tri(ex, cyc, start_amt, now_ns()))

    # ---- межбиржевой арбитраж: купить на a, продать на b (запас базовой валюты лежит на b, котируемой — на a) ----
    def _scan_xex(self, a: Exchange, b: Exchange, sym: str, t: int) -> Optional[Opportunity]:
        ba, bb = a.books[sym], b.books[sym]
        if not (ba.fresh(t, self.max_age_ns) and bb.fresh(t, self.max_age_ns)):
            return None
        if bb._bp[-1] * (1 - b.fee) / (ba._ap[0] * (1 + a.fee)) < self.xex_threshold:
            return None
        qp = self._quote_price(a, a.markets[sym].quote, t)  # цена котируемой валюты в стартовом активе
        if qp is None:
            return None
        best = None
        for size in self.sizes:
            opp = self.eval_xex(a, b, sym, size / (qp * ba._ap[0]), t)
            if opp is None or opp.profit_bps < self.cfg.min_profit_bps:
                break
            if best is None or opp.profit > best.profit:
                best = opp
        return best

    def _quote_price(self, ex: Exchange, quote: str, t: int) -> Optional[float]:
        if quote == self.cfg.start_asset:
            return 1.0
        b = ex.books.get(quote + self.cfg.start_asset)
        return (b._ap[0] + b._bp[-1]) / 2 if b and b.fresh(t, self.max_age_ns) else None

    def eval_xex(self, a: Exchange, b: Exchange, sym: str, qty: float, t: int) -> Optional[Opportunity]:
        ba, bb = a.books[sym], b.books[sym]
        if not (ba.fresh(t, self.max_age_ns) and bb.fresh(t, self.max_age_ns)):
            return None
        rb, rs = ba.buy_base(qty), bb.sell_base(qty)
        if rb is None or rs is None:
            return None
        (cost, worst_a), (proceeds, worst_b) = rb, rs
        net = proceeds * (1 - b.fee) - cost * (1 + a.fee) - cost * self.cfg.transfer_bps / 1e4  # + учёт ребалансировки запасов
        quote = a.markets[sym].quote
        legs = [LegPlan(a.name, sym, BUY, qty, worst_a, cost, qty * (1 - a.fee), quote, a.markets[sym].base),
                LegPlan(b.name, sym, SELL, qty, worst_b, qty, proceeds * (1 - b.fee), a.markets[sym].base, quote)]
        return Opportunity("xex", f"{a.name}>{b.name}", legs, quote, cost, cost + net, t, f"xex:{a.name}>{b.name}:{sym}",
                           lambda: self.eval_xex(a, b, sym, qty, now_ns()))


# ================================================================ ИСПОЛНЕНИЕ ================================================================

class PaperGateway(Gateway):
    """Бумажная торговля: исполняет IOC против ТЕКУЩЕГО стакана после случайной сетевой задержки —
    цена успевает уйти, поэтому проскальзывание и контроль лимитов проверяются честно."""

    def __init__(self, latency_ms: Tuple[float, float] = (4.0, 15.0)):
        self.latency = latency_ms

    async def ioc(self, ex, symbol, side, qty, limit) -> Fill:
        await asyncio.sleep(random.uniform(*self.latency) / 1000.0)
        m, w, book = ex.markets[symbol], ex.wallet, ex.books[symbol]
        if side == BUY:
            filled, quote = book.take(BUY, qty, limit, consume=True)
            if quote > w[m.quote] and quote > 0:  # не хватает средств — исполняем сколько можем
                s = w[m.quote] / quote; filled, quote = filled * s, quote * s
            if filled <= 0: return Fill()
            got = filled * (1 - ex.fee)
            w[m.quote] -= quote; w[m.base] += got
            for h in ex.fill_hooks: h(symbol)
            return Fill(filled, quote, got, quote / filled)
        qty = min(qty, w[m.base])
        filled, quote = book.take(SELL, qty, limit, consume=True)
        if filled <= 0: return Fill()
        got = quote * (1 - ex.fee)
        w[m.base] -= filled; w[m.quote] += got
        for h in ex.fill_hooks: h(symbol)
        return Fill(filled, filled, got, quote / filled)


def _d(x) -> Decimal: return Decimal(str(x))


class BinanceGateway(Gateway):
    """Реальные IOC-ордера Binance Spot (REST, подпись HMAC-SHA256). По умолчанию — ТЕСТНЕТ.
    Не проверялся на живой бирже: сначала прогоните на тестнете и малых суммах."""

    def __init__(self, key: str, secret: str, testnet: bool = True):
        self.key, self.secret = key, secret
        self.base = "https://testnet.binance.vision" if testnet else "https://api.binance.com"
        self.filters: Dict[str, Dict[str, Decimal]] = {}

    def load_filters(self, symbols: List[str]) -> None:
        q = urllib.parse.quote(json.dumps(symbols, separators=(",", ":")))
        with urllib.request.urlopen(f"{self.base}/api/v3/exchangeInfo?symbols={q}", timeout=5) as r:
            data = json.loads(r.read())
        for s in data["symbols"]:
            f = {x["filterType"]: x for x in s["filters"]}
            notional = f.get("NOTIONAL") or f.get("MIN_NOTIONAL") or {}
            self.filters[s["symbol"]] = {"step": _d(f["LOT_SIZE"]["stepSize"]), "tick": _d(f["PRICE_FILTER"]["tickSize"]),
                                         "min_notional": _d(notional.get("minNotional", "0"))}

    def _post(self, path: str, params: dict) -> dict:
        params = {**params, "timestamp": int(time.time() * 1000), "recvWindow": 2000}
        qs = urllib.parse.urlencode(params)
        sig = hmac.new(self.secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
        req = urllib.request.Request(f"{self.base}{path}?{qs}&signature={sig}", method="POST", headers={"X-MBX-APIKEY": self.key})
        try:
            with urllib.request.urlopen(req, timeout=3) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            return {"error": e.read().decode(errors="replace")}

    async def ioc(self, ex, symbol, side, qty, limit) -> Fill:
        f, m = self.filters[symbol], ex.markets[symbol]
        q = (_d(qty) // f["step"]) * f["step"]
        p = (_d(limit) / f["tick"]).to_integral_value(rounding=ROUND_UP if side == BUY else ROUND_DOWN) * f["tick"]
        if q <= 0 or q * p < f["min_notional"]:
            return Fill()
        resp = await asyncio.to_thread(self._post, "/api/v3/order", {
            "symbol": symbol, "side": side.upper(), "type": "LIMIT", "timeInForce": "IOC",
            "quantity": format(q, "f"), "price": format(p, "f"), "newOrderRespType": "FULL"})
        if "executedQty" not in resp:
            log.error("ордер отклонён: %s", resp); return Fill()
        eq, cq = float(resp["executedQty"]), float(resp["cummulativeQuoteQty"])
        if eq <= 0: return Fill()
        fee_base = sum(float(x["commission"]) for x in resp.get("fills", []) if x["commissionAsset"] == m.base)
        fee_quote = sum(float(x["commission"]) for x in resp.get("fills", []) if x["commissionAsset"] == m.quote)
        if side == BUY:
            return Fill(eq, cq, eq - fee_base, cq / eq)
        return Fill(eq, eq, cq - fee_quote, cq / eq)


class Trader:
    """Исполнитель: пул воркеров берёт окна из очереди; ордера на одной бирже сериализуются замком."""

    def __init__(self, exchanges: Dict[str, Exchange], cfg, queue: "asyncio.Queue[Opportunity]", stats: Stats):
        self.ex, self.cfg, self.queue, self.stats = exchanges, cfg, queue, stats
        self.slip = cfg.slippage_bps / 1e4
        self.wide = cfg.unwind_slippage_bps / 1e4

    async def worker(self) -> None:
        while True:
            opp = await self.queue.get()
            try:
                if (now_ns() - opp.created_ns) / 1e6 > self.cfg.max_opp_age_ms:
                    self.stats.stale += 1  # окно успело протухнуть в очереди
                    continue
                if any(self.ex[l.exchange].gateway is None for l in opp.legs):
                    self.stats.signals += 1
                    log.info("СИГНАЛ %s %s +%.1f bps на %.0f %s (биржа без торгового шлюза)", opp.kind, opp.exchange, opp.profit_bps, opp.start_amt, opp.start_asset)
                    continue
                await (self._tri(opp) if opp.kind == "tri" else self._xex(opp))
            except Exception:
                log.exception("ошибка исполнения")
            finally:
                self.queue.task_done()

    # ---- контроль проскальзывания ----
    def _limit(self, lp: LegPlan, slip: float) -> float:
        return lp.limit * (1 + slip) if lp.side == BUY else lp.limit * (1 - slip)

    def _worst_case_bps(self, opp: Opportunity, fee: float) -> float:
        """Результат цепочки, если КАЖДАЯ нога исполнится по предельной лимитной цене."""
        amt = opp.legs[0].in_amt
        for lp in opp.legs:
            lim = self._limit(lp, self.slip)
            amt = amt / lim * (1 - fee) if lp.side == BUY else amt * lim * (1 - fee)
        return (amt / opp.start_amt - 1) * 1e4

    @staticmethod
    def _flow(lp: LegPlan, fill: Fill, start: str) -> float:
        return (fill.received if lp.asset_out == start else 0.0) - (fill.spent if lp.asset_in == start else 0.0)

    def _wide_limit(self, ex: Exchange, symbol: str, side: str) -> Optional[float]:
        b = ex.books[symbol]
        if not (b._ap and b._bp):
            return None
        return b.best_ask() * (1 + self.wide) if side == BUY else b.best_bid() * (1 - self.wide)

    async def _tri(self, opp: Opportunity) -> None:
        ex = self.ex[opp.exchange]
        async with ex.lock:
            t0 = now_ns()
            fresh = opp.recheck()  # 1) перепроверка на свежих стаканах
            if fresh is None or fresh.profit_bps < self.cfg.min_profit_bps:
                self.stats.rej_recheck += 1; return
            opp = fresh
            if self._worst_case_bps(opp, ex.fee) < -self.cfg.max_loss_bps:  # 2) худший случай по лимитам
                self.stats.rej_slip += 1; return
            start, pnl, held, legs = opp.start_asset, 0.0, opp.start_amt, opp.legs
            done = True
            for i, lp in enumerate(legs):  # 3) ноги по очереди, количество — по факту предыдущей
                qty = lp.qty * (held / lp.in_amt)
                fill = await ex.gateway.ioc(ex, lp.symbol, lp.side, qty, self._limit(lp, self.slip))
                pnl += self._flow(lp, fill, start)
                if fill.filled_qty <= 0:
                    done = False; self.stats.aborted += 1
                    if i == 1:    # держим актив A — разворачиваем первую ногу
                        pnl += await self._reverse(ex, legs[0], held, start); self.stats.unwound += 1
                    elif i == 2:  # держим актив B — дожимаем последнюю ногу с широким лимитом
                        pnl += await self._retry(ex, lp, held, start); self.stats.unwound += 1
                    break
                if fill.filled_qty < qty * 0.999:
                    self.stats.partial += 1
                held = fill.received
            if done or pnl != 0.0:
                self._book_result(opp, pnl, t0, "TRI")

    async def _reverse(self, ex: Exchange, first: LegPlan, held: float, start: str) -> float:
        side = SELL if first.side == BUY else BUY
        lim = self._wide_limit(ex, first.symbol, side)
        if lim is None or side != SELL:
            log.error("не удалось развернуть позицию %s %.8f %s — требуется ручное вмешательство", first.asset_out, held, first.symbol); return 0.0
        fill = await ex.gateway.ioc(ex, first.symbol, SELL, held, lim)
        return fill.received if first.asset_in == start else 0.0

    async def _retry(self, ex: Exchange, last: LegPlan, held: float, start: str) -> float:
        for attempt in range(3):
            lim = self._wide_limit(ex, last.symbol, last.side)
            if lim is None: break
            qty = last.qty * (held / last.in_amt)
            fill = await ex.gateway.ioc(ex, last.symbol, last.side, qty, lim)
            if fill.filled_qty > 0:
                return self._flow(last, fill, start)
        log.error("последняя нога не исполнена, висит %.8f %s — требуется ручное вмешательство", held, last.asset_in)
        return 0.0

    async def _xex(self, opp: Opportunity) -> None:
        a, b = self.ex[opp.legs[0].exchange], self.ex[opp.legs[1].exchange]
        first, second = sorted((a, b), key=lambda e: e.name)
        async with first.lock, second.lock:
            t0 = now_ns()
            fresh = opp.recheck()
            if fresh is None or fresh.profit_bps < self.cfg.min_profit_bps:
                self.stats.rej_recheck += 1; return
            opp = fresh
            la, lb = opp.legs
            worst = la.qty * (self._limit(lb, self.slip) * (1 - b.fee) - self._limit(la, self.slip) * (1 + a.fee))
            if worst / la.in_amt * 1e4 < -self.cfg.max_loss_bps:
                self.stats.rej_slip += 1; return
            fa, fb = await asyncio.gather(  # обе ноги одновременно: время между ними — риск
                a.gateway.ioc(a, la.symbol, BUY, la.qty, self._limit(la, self.slip)),
                b.gateway.ioc(b, lb.symbol, SELL, lb.qty, self._limit(lb, self.slip)))
            mid = (fa.avg_price or fb.avg_price)
            pnl = (fb.received - fa.spent) + (fa.received - fb.spent) * mid  # учёт неодинаковых заполнений по средней цене
            if abs(fa.received - fb.spent) > 0.01 * la.qty:
                self.stats.partial += 1
                log.warning("XEX перекос заполнений: куплено %.6f, продано %.6f (остаток в запасах)", fa.received, fb.spent)
            if fa.filled_qty <= 0 and fb.filled_qty <= 0:
                self.stats.aborted += 1; return
            self._book_result(opp, pnl, t0, "XEX")

    def _book_result(self, opp: Opportunity, pnl: float, t0: int, tag: str) -> None:
        s = self.stats
        s.executed += 1
        s.pnl[opp.start_asset] += pnl
        s.wins += pnl > 0; s.losses += pnl <= 0
        ms = (now_ns() - opp.created_ns) / 1e6
        s.exec_ms.append(ms)
        route = ">".join(p.symbol + ":" + p.side[0] for p in opp.legs)
        log.info("%s %-10s %s размер=%.0f %s ожид=%+.1f bps факт=%+.4f %s (%+.1f bps) обнаружение→исполнение %.1f мс",
                 tag, opp.exchange, route, opp.start_amt, opp.start_asset, opp.profit_bps, pnl, opp.start_asset,
                 pnl / opp.start_amt * 1e4, ms)


# ================================================================ ПОТОКИ ДАННЫХ ================================================================

class Feed(ABC):
    @abstractmethod
    async def run(self) -> None: ...


class SimWorld:
    """«Истинные» цены; каждая симулируемая биржа публикует стаканы вокруг них со своим шумом."""

    def __init__(self):
        self.px = {"USDT": 1.0, "USDC": 1.0, "BTC": 60000.0, "ETH": 3000.0, "SOL": 150.0, "BNB": 600.0}

    def mid(self, symbol: str) -> float:
        b, q = split_symbol(symbol)
        return self.px[b] / self.px[q]

    async def run(self) -> None:
        while True:
            for a in ("BTC", "ETH", "SOL", "BNB"):
                self.px[a] *= math.exp(random.gauss(0, 0.00003))
            await asyncio.sleep(0.01)


class SimFeed(Feed):
    def __init__(self, ex: Exchange, world: SimWorld, engine: Engine, rate_hz: float, dislocations_per_sec: float = 0.5):
        self.ex, self.world, self.engine, self.rate = ex, world, engine, rate_hz
        self.dis_rate = dislocations_per_sec
        self.dis: Dict[str, Tuple[float, int]] = {}
        ex.fill_hooks.append(self._on_fill)

    def _levels(self, sym: str, mid: float):
        usd = self.world.px[split_symbol(sym)[0]]
        bids = [(mid * (1 - 0.75e-4 - i * 1e-4), random.uniform(8000, 25000) / usd) for i in range(10)]
        asks = [(mid * (1 + 0.75e-4 + i * 1e-4), random.uniform(8000, 25000) / usd) for i in range(10)]
        return bids, asks

    def _on_fill(self, sym: str) -> None:
        """Арбитражёр забрал ликвидность — ценовой разрыв на этой паре закрывается (как в жизни)."""
        if self.dis.pop(sym, None) is not None:
            self.ex.books[sym].apply_snapshot(*self._levels(sym, self.world.mid(sym)))

    def _tick(self, sym: str, t: int) -> None:
        book = self.ex.books[sym]
        if book._ap and random.random() < 0.3:  # часть обновлений — дельты (мерцание объёмов), как у реальных бирж
            bp, ap = random.sample(book._bp, 2), random.sample(book._ap, 2)
            book.apply_delta([(p, book.bids[p] * random.uniform(0.7, 1.3)) for p in bp], [(p, book.asks[p] * random.uniform(0.7, 1.3)) for p in ap])
            return
        f, until = self.dis.get(sym, (1.0, 0))
        mid = self.world.mid(sym) * (1 + random.gauss(0, 0.5e-4)) * (f if t < until else 1.0)
        book.apply_snapshot(*self._levels(sym, mid))

    async def run(self) -> None:
        syms, batch = list(self.ex.markets), max(1, int(self.rate / 500))
        while True:
            t = now_ns()
            if random.random() < self.dis_rate * batch / self.rate:  # редкое «окно»: цена на одной паре съезжает на 40–90 bps
                s = random.choice(syms)
                self.dis[s] = (1 + random.choice((-1, 1)) * random.uniform(40e-4, 90e-4), t + random.randint(100, 300) * 1_000_000)
            for _ in range(batch):
                s = random.choice(syms)
                self._tick(s, t)
                self.engine.on_update(self.ex.name, s)
            await asyncio.sleep(batch / self.rate)


class WSFeed(Feed):
    """Базовый класс: подключение, переподключение с экспоненциальной паузой, сброс стаканов при обрыве."""

    url = ""

    def __init__(self, ex: Exchange, engine: Engine):
        self.ex, self.engine = ex, engine

    async def on_open(self, ws) -> None: ...

    def handle(self, raw) -> None: ...

    async def run(self) -> None:
        if websockets is None:
            raise SystemExit("для реальных данных нужен пакет: pip install websockets")
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(self.url, ping_interval=20, compression=None, max_size=2 ** 22) as ws:
                    log.info("%s: подключено", self.ex.name)
                    await self.on_open(ws)
                    backoff = 1.0
                    async for raw in ws:
                        self.handle(raw)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("%s: обрыв (%s), повтор через %.0f c", self.ex.name, e, backoff)
            for b in self.ex.books.values():
                b.clear()  # данные после обрыва недостоверны
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    def _push(self, symbol: str, bids, asks, snapshot: bool = True) -> None:
        book = self.ex.books.get(symbol)
        if book is None:
            return
        bids, asks = [(float(p), float(q)) for p, q, *_ in bids], [(float(p), float(q)) for p, q, *_ in asks]
        book.apply_snapshot(bids, asks) if snapshot else book.apply_delta(bids, asks)
        self.engine.on_update(self.ex.name, symbol)


class BinanceFeed(WSFeed):
    """Частичные снимки глубины <symbol>@depth10@100ms (каждое сообщение — полный топ-10)."""

    def __init__(self, ex, engine, testnet=False):
        super().__init__(ex, engine)
        streams = "/".join(s.lower() + "@depth10@100ms" for s in ex.markets)
        host = "wss://stream.testnet.binance.vision" if testnet else "wss://stream.binance.com:9443"
        self.url = f"{host}/stream?streams={streams}"

    def handle(self, raw) -> None:
        m = _loads(raw)
        d = m.get("data")
        if d and "bids" in d:
            self._push(m["stream"].split("@")[0].upper(), d["bids"], d["asks"])


class OkxFeed(WSFeed):
    """Канал books5 (снимок топ-5 при каждом изменении)."""
    url = "wss://ws.okx.com:8443/ws/v5/public"

    async def on_open(self, ws) -> None:
        args = [{"channel": "books5", "instId": f"{m.base}-{m.quote}"} for m in self.ex.markets.values()]
        await ws.send(json.dumps({"op": "subscribe", "args": args}))

    def handle(self, raw) -> None:
        if raw == "pong": return
        m = _loads(raw)
        if "data" in m and "arg" in m:
            sym = m["arg"]["instId"].replace("-", "")
            for d in m["data"]:
                self._push(sym, d["bids"], d["asks"])


class BybitFeed(WSFeed):
    """orderbook.50: snapshot + delta (дельты применяются инкрементально)."""
    url = "wss://stream.bybit.com/v5/public/spot"

    async def on_open(self, ws) -> None:
        await ws.send(json.dumps({"op": "subscribe", "args": [f"orderbook.50.{s}" for s in self.ex.markets]}))
        self._hb = asyncio.create_task(self._heartbeat(ws))

    async def _heartbeat(self, ws) -> None:
        try:
            while True:
                await asyncio.sleep(20); await ws.send('{"op":"ping"}')
        except Exception:
            pass

    def handle(self, raw) -> None:
        m = _loads(raw)
        d = m.get("data")
        if isinstance(d, dict) and "b" in d:
            self._push(d["s"], d["b"], d["a"], snapshot=(m.get("type") == "snapshot"))


# ================================================================ СБОРКА И ЗАПУСК ================================================================

def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Арбитражный движок (треугольный + межбиржевой)")
    p.add_argument("--mode", choices=("sim", "live"), default="sim")
    p.add_argument("--exchanges", default="binance,okx,bybit", help="для --mode live")
    p.add_argument("--trade", choices=("paper", "binance-testnet", "binance-mainnet"), default="paper")
    p.add_argument("--symbols", default="BTCUSDT,ETHUSDT,ETHBTC,SOLUSDT,SOLBTC,SOLETH")
    p.add_argument("--start-asset", default="USDT")
    p.add_argument("--duration", type=float, default=20, help="секунд; 0 — до Ctrl+C")
    p.add_argument("--fee-bps", type=float, default=10.0, help="комиссия тейкера на ногу")
    p.add_argument("--min-profit-bps", type=float, default=5.0, help="минимальная чистая прибыль окна")
    p.add_argument("--min-notional", type=float, default=50.0)
    p.add_argument("--max-notional", type=float, default=5000.0)
    p.add_argument("--slippage-bps", type=float, default=3.0, help="допуск проскальзывания на ногу (лимитная цена ордера)")
    p.add_argument("--max-loss-bps", type=float, default=8.0, help="отказ, если худший случай по лимитам хуже")
    p.add_argument("--unwind-slippage-bps", type=float, default=50.0, help="лимит для аварийного закрытия")
    p.add_argument("--transfer-bps", type=float, default=2.0, help="амортизация ребалансировки запасов (межбиржевой)")
    p.add_argument("--max-age-ms", type=float, default=250.0, help="стакан старше — не используется")
    p.add_argument("--max-opp-age-ms", type=float, default=100.0, help="окно старше в очереди — выбрасывается")
    p.add_argument("--cooldown-ms", type=float, default=25.0)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--queue-size", type=int, default=8)
    p.add_argument("--no-cross", action="store_true", help="отключить межбиржевой арбитраж")
    p.add_argument("--sim-rate", type=float, default=600, help="обновлений стакана в секунду на симулируемую биржу")
    p.add_argument("--report-sec", type=float, default=5.0)
    p.add_argument("--selftest", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def start_wallet() -> Wallet:
    return Wallet({"USDT": 20000.0, "USDC": 0.0, "BTC": 0.5, "ETH": 8.0, "SOL": 150.0})


async def amain(cfg) -> None:
    if cfg.mode == "live" and websockets is None:
        raise SystemExit("для реальных данных нужен пакет: pip install websockets")
    symbols = [s.strip().upper() for s in cfg.symbols.split(",") if s.strip()]
    fee = cfg.fee_bps / 1e4
    stats, queue = Stats(), asyncio.Queue(maxsize=cfg.queue_size)
    exchanges: Dict[str, Exchange] = {}
    feeds: List[Feed] = []
    engine_ref: List[Engine] = []

    def mk(name, gateway=None, wallet=None):
        exchanges[name] = Exchange(name, symbols, fee, cfg.start_asset, wallet, gateway)

    if cfg.mode == "sim":
        for n in ("simA", "simB"):
            mk(n, PaperGateway(), start_wallet())
    else:
        names = [n.strip().lower() for n in cfg.exchanges.split(",") if n.strip()]
        for n in names:
            if n == "binance" and cfg.trade != "paper":
                key, secret = os.environ.get("BINANCE_API_KEY"), os.environ.get("BINANCE_API_SECRET")
                if not key or not secret:
                    raise SystemExit("задайте BINANCE_API_KEY и BINANCE_API_SECRET")
                testnet = cfg.trade == "binance-testnet"
                if not testnet and os.environ.get("ARB_CONFIRM_MAINNET") != "YES":
                    raise SystemExit("БОЕВАЯ торговля: подтвердите, установив ARB_CONFIRM_MAINNET=YES (на свой риск)")
                gw = BinanceGateway(key, secret, testnet)
                gw.load_filters(symbols)
                mk(n, gw)
            else:
                mk(n, PaperGateway(), start_wallet()) if cfg.trade == "paper" else mk(n, None)  # без шлюза — только сигналы

    engine = Engine(exchanges, cfg, queue, stats)
    engine_ref.append(engine)
    trader = Trader(exchanges, cfg, queue, stats)

    tasks = [asyncio.create_task(trader.worker()) for _ in range(cfg.workers)]
    if cfg.mode == "sim":
        world = SimWorld()
        tasks.append(asyncio.create_task(world.run()))
        feeds = [SimFeed(ex, world, engine, cfg.sim_rate) for ex in exchanges.values()]
    else:
        kinds = {"binance": BinanceFeed, "okx": OkxFeed, "bybit": BybitFeed}
        for n, ex in exchanges.items():
            if n not in kinds:
                raise SystemExit(f"неизвестная биржа {n}; доступны: {', '.join(kinds)}")
            feeds.append(BinanceFeed(ex, engine, cfg.trade == "binance-testnet") if n == "binance" else kinds[n](ex, engine))
    tasks += [asyncio.create_task(f.run()) for f in feeds]

    log.info("режим=%s биржи=%s символы=%s треугольных цепочек на биржу=%d комиссия=%.1f bps",
             cfg.mode, ",".join(exchanges), ",".join(symbols), len(next(iter(exchanges.values())).cycles), cfg.fee_bps)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows
            pass

    async def reporter():
        while True:
            await asyncio.sleep(cfg.report_sec)
            log.info(stats.line())

    tasks.append(asyncio.create_task(reporter()))
    try:
        await asyncio.wait_for(stop.wait(), timeout=cfg.duration or None)
    except asyncio.TimeoutError:
        pass
    except KeyboardInterrupt:
        pass
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("ИТОГ: %s", stats.line())
    for n, ex in exchanges.items():
        if ex.wallet is not None and cfg.mode == "sim":
            log.info("кошелёк %s: %s", n, {k: round(v, 6) for k, v in ex.wallet.items() if v})
    if stats.exec_ms:
        e = sorted(stats.exec_ms)
        log.info("задержка обнаружение→завершение исполнения: p50=%.1f мс p99=%.1f мс (в бумажном режиме включает смоделированную сетевую задержку 4–15 мс/ордер)",
                 e[len(e) // 2], e[min(len(e) - 1, int(len(e) * .99))])


# ================================================================ САМОПРОВЕРКА ================================================================

def selftest() -> None:
    ok = 0

    def check(cond, name):
        nonlocal ok
        assert cond, "ПРОВАЛ: " + name
        ok += 1; print("  ок ", name)

    b = OrderBook("BTCUSDT")
    b.apply_snapshot([(100, 1), (99, 2)], [(101, 1), (102, 3)])
    check(b.best_bid() == 100 and b.best_ask() == 101, "лучшие цены из снимка")
    b.apply_delta([(100, 0), (98, 5)], [(101, 2), (100.5, 1)])
    check(b.best_bid() == 99 and b.best_ask() == 100.5 and b.asks[101] == 2, "дельты: удаление, вставка, изменение")
    b.apply_snapshot([(100, 1), (99, 2)], [(101, 1), (102, 3)])
    got, worst = b.buy_with_quote(101 + 102)  # весь первый уровень + 1 единица второго
    check(abs(got - 2.0) < 1e-12 and worst == 102, "прогулка по asks учитывает глубину")
    check(b.sell_base(1.5) == (100 * 1 + 99 * 0.5, 99), "прогулка по bids")
    check(b.buy_with_quote(10 ** 9) is None, "нехватка глубины -> None")
    b.take(BUY, 1.5, 102.5, consume=True)
    check(1 not in b.asks.values() and b.best_ask() == 102 and abs(b.asks[102] - 2.5) < 1e-12, "исполнение забирает ликвидность из стакана")
    b.apply_snapshot([(100, 1), (99, 2)], [(101, 1), (102, 3)])
    b.apply_snapshot([(102, 1)], [(101, 1)])
    check(not b.fresh(now_ns(), 10 ** 9), "перекрещенный стакан считается недостоверным")

    cfg = parse_args([])
    exch = Exchange("t", ["BTCUSDT", "ETHUSDT", "ETHBTC"], 0.001, "USDT")
    check(len(exch.cycles) == 2, "найдено 2 треугольные цепочки (в обе стороны)")
    exch.books["BTCUSDT"].apply_snapshot([(59999, 5)], [(60000, 5)])
    exch.books["ETHBTC"].apply_snapshot([(0.0499, 500)], [(0.05, 500)])
    exch.books["ETHUSDT"].apply_snapshot([(3100, 500)], [(3101, 500)])
    eng = Engine({"t": exch}, cfg, asyncio.Queue(), Stats())
    t = now_ns()
    best = max((o for o in (eng.eval_tri(exch, c, 1000.0, t) for c in exch.cycles) if o), key=lambda o: o.profit)
    expected = 1000 / 60000 / 0.05 * 3100 * 0.999 ** 3
    check(abs(best.end_amt - expected) < 1e-6 and best.profit_bps > 250, f"треугольник с комиссиями: +{best.profit_bps:.0f} bps")
    wc = Trader({"t": exch}, cfg, asyncio.Queue(), Stats())._worst_case_bps(best, 0.001)
    check(-50 < wc < best.profit_bps, f"худший случай по лимитам ({wc:.0f} bps) хуже ожидаемого")
    exch.books["ETHUSDT"].apply_snapshot([(3000, 500)], [(3001, 500)])
    check(not any(eng.eval_tri(exch, c, 1000.0, t) and eng.eval_tri(exch, c, 1000.0, t).profit_bps > 0 for c in exch.cycles), "нет окна без дислокации цен")
    exch.books["BTCUSDT"].ts_ns = t - 10 ** 10
    check(eng.eval_tri(exch, exch.cycles[0], 1000.0, t) is None, "устаревший стакан отвергается")

    async def paper():
        ex = Exchange("p", ["BTCUSDT"], 0.001, "USDT", Wallet({"USDT": 1000.0}), PaperGateway((0, 0)))
        ex.books["BTCUSDT"].apply_snapshot([(59990, 1)], [(60000, 1)])
        f = await ex.gateway.ioc(ex, "BTCUSDT", BUY, 0.01, 60000 * 1.0003)
        f2 = await ex.gateway.ioc(ex, "BTCUSDT", BUY, 0.01, 59000)  # лимит ниже рынка — не исполнится
        return f, f2, ex.wallet
    f, f2, w = asyncio.run(paper())
    check(abs(f.filled_qty - 0.01) < 1e-12 and abs(w["BTC"] - 0.01 * 0.999) < 1e-12, "бумажное исполнение IOC и комиссия")
    check(f2.filled_qty == 0, "IOC с лимитом ниже рынка не исполняется (защита от проскальзывания)")
    print(f"\nВсе проверки пройдены: {ok}")


import time


class Runtime:
    """Класс-обертка для управления ботом из веб-сервера"""

    def __init__(self, cfg):
        self.cfg = cfg
        self._status = "stopped"
        self.started_at = time.time()
        self._task = None

    async def start(self):
        self._status = "running"
        self.started_at = time.time()

    async def stop(self):
        self._status = "stopped"

    def status(self) -> str:
        return self._status

    def pause(self):
        self._status = "paused"

    def resume(self):
        self._status = "running"

    def snapshot(self) -> dict:
        return {
            "status": self._status,
            "uptime_s": round(time.time() - self.started_at),
            "mode": self.cfg.mode
        }


def main() -> None:
    cfg = parse_args()
    logging.basicConfig(level=logging.DEBUG if cfg.verbose else logging.INFO, format="%(asctime)s.%(msecs)03d %(message)s", datefmt="%H:%M:%S")
    if cfg.selftest:
        selftest(); return
    if uvloop:
        uvloop.install()
    try:
        asyncio.run(amain(cfg))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()


    import time


