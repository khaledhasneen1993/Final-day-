"""Persist Demo entry intent, reconcile fills, and place exchange-side exits."""

import asyncio
import hashlib
import logging
import sqlite3
import time
from decimal import ROUND_CEILING, ROUND_DOWN, Decimal

from demo_api import APIError
from telegram_alerts import build_alert

log = logging.getLogger(__name__)
TERMINAL = {"FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED"}
MISSING = {-2011, -2013}


def rounded(value, step, mode=ROUND_DOWN):
    value, step = Decimal(str(value)), Decimal(step)
    return format((value / step).to_integral_value(rounding=mode) * step, "f")


class DemoTrader:
    def __init__(self, api, database, notional, max_positions):
        self.api = api
        self.notional = notional
        self.max_positions = max_positions
        self.queue = asyncio.Queue(maxsize=50)
        self.queued = set()
        self.db = sqlite3.connect(database)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS demo_orders (
                id TEXT PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL,
                stop TEXT NOT NULL, target TEXT NOT NULL, status TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS demo_active_symbol ON demo_orders(symbol)
                WHERE status != 'done';
        """)

    def enqueue(self, symbol, signal, history):
        if symbol in self.queued or self.queue.full():
            return False
        alert = build_alert(symbol, signal, history)
        if alert is None:
            return False
        self.queue.put_nowait((time.monotonic(), alert))
        self.queued.add(symbol)
        return True

    async def position(self, symbol):
        rows = await self.api.private("GET", "v3/positionRisk", symbol=symbol)
        return sum((Decimal(row["positionAmt"]) for row in rows), Decimal(0))

    async def submit(self, queued_at, alert):
        market = self.api.market(alert.symbol)
        symbol = market["id"]
        order_id = "fd-" + hashlib.sha256(f"{symbol}/{alert.side}/{alert.candle}".encode()).hexdigest()[:22]
        if self.db.execute("SELECT 1 FROM demo_orders WHERE id=? OR (symbol=? AND status!='done')",
                           (order_id, symbol)).fetchone():
            return
        count = self.db.execute("SELECT count(*) FROM demo_orders WHERE status!='done'").fetchone()[0]
        if count >= self.max_positions or await self.position(symbol):
            return
        if (await self.api.private("GET", "v1/openOrders", symbol=symbol)
                or await self.api.private("GET", "v1/openAlgoOrders", symbol=symbol)):
            return

        filters = market["filters"]
        lot = filters["MARKET_LOT_SIZE"]
        quantity = rounded(self.notional / Decimal(str(alert.price)), lot["stepSize"])
        amount = Decimal(quantity)
        minimum = Decimal(filters.get("MIN_NOTIONAL", {}).get("notional", "0"))
        if (amount <= 0 or not Decimal(lot["minQty"]) <= amount <= Decimal(lot["maxQty"])
                or amount * Decimal(str(alert.price)) < minimum):
            log.info("Demo signal skipped for %s: configured notional below or outside market limits", symbol)
            return
        tick = filters["PRICE_FILTER"]["tickSize"]
        direction = ROUND_DOWN if alert.side == "buy" else ROUND_CEILING
        stop = rounded(alert.stop, tick, direction)
        target = rounded(alert.target, tick, ROUND_CEILING if alert.side == "buy" else ROUND_DOWN)
        if Decimal(stop) <= 0 or Decimal(target) <= 0 or time.monotonic() - queued_at > 10:
            return
        with self.db:
            self.db.execute("INSERT INTO demo_orders VALUES (?, ?, ?, ?, ?, 'pending')",
                            (order_id, symbol, alert.side, stop, target))
        try:
            entry = await self.api.private("POST", "v1/order", symbol=symbol, side=alert.side.upper(),
                                   type="MARKET", quantity=quantity, newClientOrderId=order_id,
                                   newOrderRespType="RESULT")
        except APIError as error:
            if error.status == 400 and error.code in {-1013, -1021, -1022, -1102, -1111,
                                                      -2015, -2019, -2027, -4164}:
                self.finish(order_id)
            raise
        await self.reconcile(self.db.execute("SELECT * FROM demo_orders WHERE id=?", (order_id,)).fetchone(), entry)

    def finish(self, order_id):
        with self.db:
            self.db.execute("UPDATE demo_orders SET status='done' WHERE id=?", (order_id,))

    async def lookup(self, endpoint, **params):
        try:
            return await self.api.private("GET", endpoint, **params)
        except APIError as error:
            if error.code not in MISSING:
                raise
            return None

    async def reconcile(self, row, entry=None):
        order_id, symbol, side = row["id"], row["symbol"], row["side"]
        if entry is None:
            entry = await self.lookup("v1/order", symbol=symbol, origClientOrderId=order_id)
        if entry is None:
            log.warning("Demo order unresolved for %s (%s); continuing other symbols", symbol, order_id)
            return
        filled = Decimal(entry["executedQty"])
        position = await self.position(symbol)
        if entry["status"] not in TERMINAL:
            return
        if not position:
            if filled and row["status"] == "pending":
                # The fill can reach the order API before positionRisk reflects it.
                return
            for suffix in ("-sl", "-tp"):
                algo_id = order_id + suffix
                algo = await self.lookup("v1/algoOrder", clientAlgoId=algo_id)
                if algo and algo["algoStatus"] == "NEW":
                    await self.api.private("DELETE", "v1/algoOrder", clientAlgoId=algo_id)
                elif algo and algo["algoStatus"] in {"TRIGGERING", "TRIGGERED"}:
                    return
            self.finish(order_id)
            return
        if not filled or (position > 0) != (side == "buy") or abs(position) > filled:
            log.warning("Demo position for %s differs from bot fill; no account changes", symbol)
            return
        if row["status"] == "pending":
            with self.db:
                self.db.execute("UPDATE demo_orders SET status='observed' WHERE id=?", (order_id,))
        close_side = "SELL" if side == "buy" else "BUY"
        for suffix, kind, price in (("-sl", "STOP_MARKET", row["stop"]),
                                    ("-tp", "TAKE_PROFIT_MARKET", row["target"])):
            algo_id = order_id + suffix
            try:
                existing = await self.lookup("v1/algoOrder", clientAlgoId=algo_id)
                if existing is None:
                    await self.api.private("POST", "v1/algoOrder", algoType="CONDITIONAL",
                                           symbol=symbol, side=close_side, type=kind,
                                           quantity=format(abs(position), "f"), reduceOnly="true",
                                           triggerPrice=price, workingType="CONTRACT_PRICE", clientAlgoId=algo_id)
                elif existing["algoStatus"] != "NEW":
                    log.warning("Demo %s for %s is %s; checking the position again",
                                kind, symbol, existing["algoStatus"])
            except APIError as error:
                log.warning("Demo %s protection unconfirmed for %s: %s", kind, symbol, error)

    async def run(self):
        next_sync = next_reconcile = 0
        delay = 2
        while True:
            now = time.monotonic()
            if now >= next_sync:
                try:
                    await self.api.sync_time()
                    await self.api.check_account()
                except (APIError, ValueError) as error:
                    log.warning("Demo account check will retry in %ss: %s", delay, error)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30)
                    continue
                next_sync = time.monotonic() + 60
                delay = 2
            if now >= next_reconcile:
                for row in self.db.execute("SELECT * FROM demo_orders WHERE status!='done'").fetchall():
                    try:
                        await self.reconcile(row)
                    except APIError as error:
                        log.warning("Demo reconciliation will retry for %s: %s", row["symbol"], error)
                next_reconcile = time.monotonic() + 5
            try:
                queued_at, alert = await asyncio.wait_for(self.queue.get(), timeout=1)
            except TimeoutError:
                continue
            try:
                if time.monotonic() - queued_at <= 10:
                    await self.submit(queued_at, alert)
            except APIError as error:
                log.warning("Demo order not confirmed for %s: %s", alert.symbol, error)
                if error.code == -1021:
                    next_sync = 0
            finally:
                self.queued.discard(alert.symbol)
                self.queue.task_done()
