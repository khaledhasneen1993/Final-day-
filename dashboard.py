"""Local dashboard and persistent virtual tracking for delivered signals."""

import asyncio
import logging
import math
import sqlite3
import time
from pathlib import Path

from aiohttp import web

log = logging.getLogger(__name__)
PAGE = Path(__file__).with_name("dashboard.html")


class TradeLedger:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                symbol TEXT, side TEXT, candle INTEGER, entry REAL, stop REAL, target REAL,
                movement REAL, volume REAL, imbalance REAL, opened REAL,
                status TEXT DEFAULT 'open', exit_price REAL, closed REAL,
                PRIMARY KEY (symbol, side, candle)
            )
        """)
        self.prices = {}

    def record(self, alert):
        with self.db:
            self.db.execute("""
                INSERT OR IGNORE INTO trades
                    (symbol, side, candle, entry, stop, target, movement, volume, imbalance, opened)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (alert.symbol, alert.side, alert.candle, alert.price, alert.stop, alert.target,
                  alert.movement, alert.volume, alert.imbalance, time.time()))

    def contains(self, symbol, side, candle):
        return self.db.execute(
            "SELECT 1 FROM trades WHERE symbol=? AND side=? AND candle=?",
            (symbol, side, candle),
        ).fetchone() is not None

    def apply_prices(self, tickers, requested_at):
        with self.db:
            open_trades = self.db.execute(
                "SELECT * FROM trades WHERE status='open' AND opened <= ?", (requested_at,)
            ).fetchall()
            for trade in open_trades:
                symbol = trade["symbol"]
                price = (tickers.get(symbol) or {}).get("last")
                if not isinstance(price, (int, float)) or not math.isfinite(price) or price <= 0:
                    continue
                self.prices[symbol] = (price, time.time())
                is_long = trade["side"] == "buy"
                hit_target = price >= trade["target"] if is_long else price <= trade["target"]
                hit_stop = price <= trade["stop"] if is_long else price >= trade["stop"]
                if hit_target or hit_stop:
                    self.db.execute("""
                        UPDATE trades SET status=?, exit_price=?, closed=?
                        WHERE symbol=? AND side=? AND candle=?
                    """, ("tp" if hit_target else "sl", price, time.time(),
                          symbol, trade["side"], trade["candle"]))

    async def follow(self, market_data):
        while True:
            symbols = [row[0] for row in self.db.execute(
                "SELECT DISTINCT symbol FROM trades WHERE status='open'")]
            if symbols:
                try:
                    requested_at = time.time()
                    tickers = await market_data.fetch_tickers(symbols)
                except Exception as error:
                    log.warning("Dashboard price refresh failed: %s", type(error).__name__)
                else:
                    self.apply_prices(tickers, requested_at)
            await asyncio.sleep(5)

    def snapshot(self, page, page_size=50):
        counts = dict(self.db.execute("SELECT status, count(*) FROM trades GROUP BY status").fetchall())
        opened = self.db.execute("SELECT * FROM trades WHERE status='open' ORDER BY opened DESC")
        closed = self.db.execute("""
            SELECT * FROM trades WHERE status!='open' ORDER BY closed DESC, rowid DESC
            LIMIT ? OFFSET ?
        """, (page_size, (page - 1) * page_size))

        def format_trade(row):
            trade = dict(row)
            price, updated = self.prices.get(row["symbol"], (None, None))
            if row["status"] != "open":
                price, updated = row["exit_price"], row["closed"]
            direction = 1 if row["side"] == "buy" else -1
            trade.update(price=price, price_at=updated, change=(
                direction * (price / row["entry"] - 1) if price is not None else None
            ))
            return trade

        return {"open": [format_trade(row) for row in opened],
                "closed": [format_trade(row) for row in closed],
                "counts": counts, "page": page, "page_size": page_size}


def dashboard_app(scanner, ledger):
    async def home(request):
        return web.FileResponse(PAGE)

    async def snapshot(request):
        try:
            page = max(1, min(1_000_000, int(request.query.get("page", "1"))))
        except ValueError:
            raise web.HTTPBadRequest(text="Invalid page") from None
        return web.json_response({
            "market": scanner.snapshot(),
            "trades": ledger.snapshot(page),
            "time": time.time(),
        }, headers={"Cache-Control": "no-store"})

    app = web.Application()
    app.router.add_get("/", home)
    app.router.add_get("/api/status", snapshot)
    return app
