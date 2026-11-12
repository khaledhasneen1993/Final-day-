"""Persist Demo entry intent, reconcile fills, and place exchange-side exits."""

import asyncio
import hashlib
import logging
import sqlite3
import time
from decimal import ROUND_CEILING, ROUND_DOWN, Decimal, InvalidOperation

from demo_api import APIError
from telegram_alerts import build_alert

log = logging.getLogger(__name__)
TERMINAL = {"FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED"}
MISSING = {-2011, -2013}
STOP_FRACTION = Decimal("0.03")


def rounded(value, step, mode=ROUND_DOWN):
    value, step = Decimal(str(value)), Decimal(step)
    return format((value / step).to_integral_value(rounding=mode) * step, "f")


def nonnegative_decimal(value):
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        return None
    return result if result.is_finite() and result >= 0 else None


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

        columns = {row["name"] for row in self.db.execute("PRAGMA table_info(demo_orders)")}
        if "close_reason" not in columns:
            with self.db:
                self.db.execute("ALTER TABLE demo_orders ADD COLUMN close_reason TEXT")

    def enqueue(self, symbol, signal, history):
        if symbol in self.queued or self.queue.full():
            return False
        alert = build_alert(symbol, signal, history, require_stop=False)
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
        target = rounded(alert.target, tick, ROUND_CEILING if alert.side == "buy" else ROUND_DOWN)
        if Decimal(target) <= 0 or time.monotonic() - queued_at > 10:
            return

        tiers = await self.api.private("GET", "v1/leverageBracket", symbol=symbol)
        if isinstance(tiers, list):
            tiers = next((tier for tier in tiers if tier["symbol"] == symbol), {})
        value = amount * Decimal(str(alert.price))
        leverage = max((int(tier["initialLeverage"]) for tier in tiers.get("brackets", ())
                        if Decimal(str(tier["notionalFloor"])) <= value
                        < Decimal(str(tier["notionalCap"]))), default=0)
        if not leverage or time.monotonic() - queued_at > 10:
            return
        configured = await self.api.private("POST", "v1/leverage", symbol=symbol, leverage=leverage)
        if (int(configured["leverage"]) != leverage
                or value >= Decimal(configured["maxNotionalValue"])
                or time.monotonic() - queued_at > 10):
            return
        log.info("Demo leverage for %s set to %sx", symbol, leverage)
        with self.db:
            self.db.execute("""
                INSERT INTO demo_orders (id, symbol, side, stop, target, status)
                VALUES (?, ?, ?, '', ?, 'pending')
            """, (order_id, symbol, alert.side, target))
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

    def finish(self, order_id, reason=None):
        with self.db:
            self.db.execute("UPDATE demo_orders SET status='done', close_reason=? WHERE id=?",
                            (reason, order_id))

    async def closing_fill(self, symbol, side, entry):
        entry_time = entry.get("time") or entry.get("updateTime")
        if not entry_time:
            return None
        fills = await self.api.private("GET", "v1/userTrades", symbol=symbol,
                                       startTime=max(entry_time, int(time.time() * 1000) - 6 * 86400000),
                                       limit=100)
        return max((fill for fill in fills if fill["side"] != side.upper()
                    and fill["orderId"] != entry["orderId"] and fill["time"] >= entry_time),
                   key=lambda fill: (fill["time"], fill["id"]), default=None)

    async def finalize_position(self, row, fill):
        order_id, symbol = row["id"], row["symbol"]
        reason = "external_or_manual" if fill else "flat_unclassified"
        for suffix, label in (("-sl", "stop_loss"), ("-tp", "take_profit")):
            algo_id = order_id + suffix
            algo = await self.lookup("v1/algoOrder", clientAlgoId=algo_id)
            if not algo:
                continue
            if fill and str(algo.get("actualOrderId")) == str(fill["orderId"]):
                reason = label
            if algo["algoStatus"] == "NEW":
                await self.api.private("DELETE", "v1/algoOrder", clientAlgoId=algo_id)
            elif algo["algoStatus"] in {"TRIGGERING", "TRIGGERED"}:
                return
        if reason == "external_or_manual":
            closing_order = await self.lookup("v1/order", symbol=symbol, orderId=fill["orderId"])
            client_id = (closing_order or {}).get("clientOrderId", "")
            if client_id.startswith(("autoclose-", "adl_autoclose", "settlement_autoclose")):
                reason = "exchange_auto_close"
        self.finish(order_id, reason)
        log.info("DEMO POSITION CLOSED | %s | reason=%s", symbol, reason)

    async def lookup(self, endpoint, **params):
        try:
            return await self.api.private("GET", endpoint, **params)
        except APIError as error:
            if error.code not in MISSING:
                raise
            return None

    async def fill_price(self, symbol, entry, filled):
        average = nonnegative_decimal(entry.get("avgPrice"))
        if average:
            return average
        quote = nonnegative_decimal(entry.get("cumQuote"))
        if quote:
            return quote / filled
        order_id = entry.get("orderId")
        if not order_id:
            return None
        fills = await self.api.private("GET", "v1/userTrades", symbol=symbol, orderId=order_id)
        total_qty = total_quote = Decimal(0)
        for trade in fills:
            if str(trade.get("orderId")) != str(order_id):
                continue
            quantity = nonnegative_decimal(trade.get("qty"))
            price = nonnegative_decimal(trade.get("price"))
            if quantity and price:
                total_qty += quantity
                total_quote += quantity * price
        return total_quote / total_qty if total_qty == filled else None

    async def reconcile(self, row, entry=None):
        order_id, symbol, side = row["id"], row["symbol"], row["side"]
        needs_price = not row["stop"]
        if (not isinstance(entry, dict) or not entry.get("status")
                or nonnegative_decimal(entry.get("executedQty")) is None
                or (needs_price and not nonnegative_decimal(entry.get("avgPrice"))
                    and not nonnegative_decimal(entry.get("cumQuote")))):
            entry = await self.lookup("v1/order", symbol=symbol, origClientOrderId=order_id)
        if not isinstance(entry, dict) or not entry.get("status"):
            log.warning("Demo order unresolved for %s (%s); continuing other symbols", symbol, order_id)
            return
        filled = nonnegative_decimal(entry.get("executedQty"))
        if filled is None:
            log.warning("Demo filled quantity unconfirmed for %s (%s); will retry", symbol, order_id)
            return
        position = await self.position(symbol)
        if entry["status"] not in TERMINAL:
            return
        if not position:
            if not filled:
                self.finish(order_id)
                return
            fill = await self.closing_fill(symbol, side, entry)
            if row["status"] == "pending" and fill is None:
                # The fill can reach the order API before positionRisk reflects it.
                return
            await self.finalize_position(row, fill)
            return
        if not filled or (position > 0) != (side == "buy") or abs(position) > filled:
            log.warning("Demo position for %s differs from bot fill; no account changes", symbol)
            return
        stop = row["stop"]
        if not stop:
            entry_price = await self.fill_price(symbol, entry, filled)
            if entry_price is None:
                log.warning("Demo fill price unconfirmed for %s (%s); protection will retry", symbol, order_id)
                return
            tick = self.api.symbols[symbol]["filters"]["PRICE_FILTER"]["tickSize"]
            fraction = 1 - STOP_FRACTION if side == "buy" else 1 + STOP_FRACTION
            stop = rounded(entry_price * fraction, tick, ROUND_CEILING if side == "buy" else ROUND_DOWN)
        if row["status"] == "pending":
            with self.db:
                self.db.execute("UPDATE demo_orders SET status='observed', stop=? WHERE id=?",
                                (stop, order_id))
        close_side = "SELL" if side == "buy" else "BUY"
        for suffix, kind, price in (("-sl", "STOP_MARKET", stop),
                                    ("-tp", "TAKE_PROFIT_MARKET", row["target"])):
            algo_id = order_id + suffix
            try:
                existing = await self.lookup("v1/algoOrder", clientAlgoId=algo_id)
                if existing is None:
                    result = await self.api.private("POST", "v1/algoOrder", algoType="CONDITIONAL",
                                           symbol=symbol, side=close_side, type=kind,
                                           quantity=format(abs(position), "f"), reduceOnly="true",
                                           triggerPrice=price, workingType="CONTRACT_PRICE", clientAlgoId=algo_id)
                    if isinstance(result, dict) and result.get("algoStatus") == "NEW":
                        log.info("DEMO PROTECTION ACCEPTED | %s | %s | trigger=%s", symbol, kind, price)
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
                    except (KeyError, TypeError, ValueError, InvalidOperation) as error:
                        log.warning("Unexpected Demo order data for %s (%s); will retry",
                                    row["symbol"], type(error).__name__)
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
            except (KeyError, TypeError, ValueError, InvalidOperation) as error:
                log.warning("Unexpected Demo data for %s (%s); continuing reconciliation",
                            alert.symbol, type(error).__name__)
            finally:
                self.queued.discard(alert.symbol)
                self.queue.task_done()
