"""Qualify Telegram signals using public Binance USDT-M market data."""

import asyncio
import logging
import math
import time
from collections import deque
from urllib.parse import urlencode

import aiohttp
import ccxt

log = logging.getLogger(__name__)

CASCADE_THRESHOLD = 0.01
VOLUME_SPIKE = 3.5
ORDERBOOK_THRESHOLD = 0.35
MAX_SYMBOLS = 50
MIN_QUOTE_VOLUME = 15_000_000
UNIVERSE_REFRESH_SEC = 600

WS_URL = "wss://fstream.binance.com/market/stream"
MAX_EVENT_AGE = 10.0
BOOK_CHECK_INTERVAL = 1.0
BOOK_REQUEST_SPACING = 0.25
WORKERS = 8


class CandleGap(Exception):
    pass


def valid_rows(rows):
    cleaned = []
    for row in rows:
        if len(row) < 6:
            raise ValueError("Incomplete candle")
        t = int(row[0])
        o, h, l, c, v = map(float, row[1:6])
        if (t % 60000 or not all(math.isfinite(x) for x in (o, h, l, c, v))
                or min(o, h, l, c) <= 0 or v < 0
                or l > min(o, c) or h < max(o, c)):
            raise ValueError("Invalid candle")
        cleaned.append([t, o, h, l, c, v])
    if len(cleaned) < 15 or any(b[0] - a[0] != 60000 for a, b in zip(cleaned, cleaned[1:])):
        raise ValueError("Insufficient or gapped candle history")
    return cleaned[-30:]


class ContinuousScanner:
    def __init__(self, market_data, alerts, *, clock=time.monotonic, wall_clock=time.time):
        self.market_data = market_data
        self.alerts = alerts
        self.clock = clock
        self.wall_clock = wall_clock
        self.symbols = ()
        self.histories = {}
        self.stream_ids = {}
        self.event_times = {}
        self.received = {}
        self.last_book_check = {}
        self.books = {}
        self.refreshed_at = None
        self.queue = asyncio.Queue(maxsize=MAX_SYMBOLS)
        self.pending = set()
        self.changed = asyncio.Event()
        self.ready = asyncio.Event()
        self.connected = False
        self.book_lock = asyncio.Lock()
        self.next_book_request = 0.0
        self.book_pause_until = 0.0
        self.events = self.book_checks = self.alerts_queued = 0

    async def refresh_universe(self):
        try:
            tickers = await self.market_data.fetch_tickers()
        except Exception as error:
            log.warning("Ticker list unavailable; using fallback markets: %s", error)
            tickers = {f"{asset}/USDT:USDT": {"quoteVolume": MIN_QUOTE_VOLUME}
                       for asset in ("BTC", "ETH", "SOL")}

        candidates = []
        for symbol, ticker in tickers.items():
            try:
                market = self.market_data.market(symbol)
                volume = float(ticker.get("quoteVolume") or 0)
            except (ValueError, TypeError, KeyError, ccxt.BadSymbol):
                continue
            if (market.get("linear") and market.get("swap") and market.get("settle") == "USDT"
                    and market.get("active") is not False
                    and math.isfinite(volume) and volume >= MIN_QUOTE_VOLUME):
                candidates.append((symbol, volume, market["id"]))

        candidates.sort(key=lambda candidate: candidate[1], reverse=True)
        selected = tuple(symbol for symbol, _, _ in candidates[:MAX_SYMBOLS])
        self.stream_ids = {symbol: stream_id for symbol, _, stream_id in candidates[:MAX_SYMBOLS]}
        if selected != self.symbols:
            self.symbols = selected
            self.changed.set()
        self.refreshed_at = self.wall_clock()
        self.ready.set()
        log.info("CONTINUOUS UNIVERSE | symbols=%d | refresh=%ss", len(selected), UNIVERSE_REFRESH_SEC)

    async def universe_loop(self):
        while True:
            try:
                await self.refresh_universe()
            except Exception as error:
                log.warning("Universe refresh failed: %s", error)
                await asyncio.sleep(10)
                continue
            await asyncio.sleep(UNIVERSE_REFRESH_SEC)

    async def seed(self, symbols):
        semaphore = asyncio.Semaphore(WORKERS)

        async def one(symbol):
            async with semaphore:
                try:
                    rows = await self.market_data.fetch_ohlcv(symbol, "1m", limit=30)
                    self.histories[symbol] = deque(valid_rows(rows), maxlen=30)
                    return symbol
                except Exception as error:
                    log.warning("Candle warmup failed %s: %s", symbol, error)
                    return None

        return [s for s in await asyncio.gather(*(one(s) for s in symbols)) if s is not None]

    def schedule(self, symbol):
        if symbol not in self.pending and symbol in self.symbols:
            try:
                self.queue.put_nowait(symbol)
            except asyncio.QueueFull:
                return
            self.pending.add(symbol)

    def ingest(self, message):
        payload = message.get("data", message)
        if payload.get("e") != "kline" or payload.get("st", 1) != 1:
            return
        kline = payload["k"]
        if kline.get("i") != "1m":
            return
        stream_id = kline.get("s", payload.get("s"))
        symbol = next((s for s in self.symbols if self.stream_ids.get(s) == stream_id), None)
        if symbol is None or symbol not in self.histories:
            return
        event = int(payload["E"])
        if event <= self.event_times.get(symbol, -1):
            return
        age = self.wall_clock() - event / 1000
        if age > MAX_EVENT_AGE or age < -MAX_EVENT_AGE:
            return
        row = [int(kline["t"]), *map(float, (kline["o"], kline["h"], kline["l"], kline["c"], kline["v"]))]
        history = self.histories[symbol]
        if row[0] < history[-1][0]:
            return
        if row[0] > history[-1][0] + 60000:
            raise CandleGap(symbol)
        valid_rows([*list(history)[:-1], row] if row[0] == history[-1][0] else [*history, row])
        if row[0] == history[-1][0]:
            history[-1] = row
        else:
            history.append(row)
        self.event_times[symbol] = event
        self.received[symbol] = self.clock()
        self.events += 1
        self.schedule(symbol)

    def metrics(self, symbol):
        rows = self.histories.get(symbol, ())
        if len(rows) < 15:
            return None
        ret = (rows[-1][4] - rows[-3][4]) / rows[-3][4]
        average = sum(row[5] for row in list(rows)[-15:-3]) / 12
        spike = rows[-1][5] / average if average > 0 else 0
        return {"side": "buy" if ret > 0 else "sell", "price": rows[-1][4], "ret": ret, "spike": spike}

    def fresh(self, symbol):
        return (self.connected and symbol in self.received
                and self.clock() - self.received[symbol] <= MAX_EVENT_AGE
                and self.wall_clock() - self.event_times[symbol] / 1000 <= MAX_EVENT_AGE)

    def signal(self, symbol):
        if symbol not in self.symbols or not self.fresh(symbol):
            return None
        metrics = self.metrics(symbol)
        if metrics and abs(metrics["ret"]) > CASCADE_THRESHOLD and metrics["spike"] > VOLUME_SPIKE:
            return metrics
        return None

    def snapshot(self):
        rows = []
        for symbol in self.symbols:
            book, at = self.books.get(symbol, (None, 0))
            rows.append({
                "symbol": symbol, "metrics": self.metrics(symbol),
                "fresh": self.fresh(symbol), "event": self.event_times.get(symbol),
                "imbalance": book if self.fresh(symbol) and self.clock() - at <= MAX_EVENT_AGE else None,
            })
        return {"connected": self.connected, "events": self.events, "book_checks": self.book_checks,
                "queued": self.alerts_queued, "refresh_at": self.refreshed_at, "symbols": rows}

    async def evaluate(self, symbol):
        signal = self.signal(symbol)
        now = self.clock()
        if (signal is None or now < self.book_pause_until
                or now - self.last_book_check.get(symbol, -math.inf) < BOOK_CHECK_INTERVAL):
            return
        self.last_book_check[symbol] = now
        # Bound REST pressure even when many symbols trigger simultaneously.
        async with self.book_lock:
            await asyncio.sleep(max(0.0, self.next_book_request - self.clock()))
            if self.clock() < self.book_pause_until or self.signal(symbol) is None:
                return
            self.next_book_request = self.clock() + BOOK_REQUEST_SPACING
        try:
            self.book_checks += 1
            book = await self.market_data.fetch_order_book(symbol, limit=20)
            bids = sum(float(x[1]) for x in book["bids"][:20])
            asks = sum(float(x[1]) for x in book["asks"][:20])
            if not all(math.isfinite(v) and v >= 0 for v in (bids, asks)):
                return
            imbalance = (bids - asks) / (bids + asks) if bids + asks else 0
            self.books[symbol] = (imbalance, self.clock())
        except Exception as error:
            self.book_pause_until = self.clock() + 5
            log.warning("Order book check failed %s (backoff 5s): %s", symbol, error)
            return
        signal = self.signal(symbol)
        if signal is None:
            return
        if not ((signal["side"] == "buy" and imbalance > ORDERBOOK_THRESHOLD)
                or (signal["side"] == "sell" and imbalance < -ORDERBOOK_THRESHOLD)):
            return
        if self.alerts.enqueue(symbol, {**signal, "imbalance": imbalance}, list(self.histories[symbol])):
            self.alerts_queued += 1
            log.info("SIGNAL QUEUED %s | move=%.3f%% volume=%.2fx imbalance=%.3f",
                     symbol, signal["ret"] * 100, signal["spike"], imbalance)

    async def worker(self):
        while True:
            symbol = await self.queue.get()
            try:
                await self.evaluate(symbol)
            except Exception as error:
                log.warning("Continuous check failed %s: %s", symbol, error)
            finally:
                self.pending.discard(symbol)
                self.queue.task_done()

    async def read_stream(self, socket):
        async for message in socket:
            if message.type == aiohttp.WSMsgType.TEXT:
                payload = message.json()
                if "code" in payload:
                    raise ConnectionError(str(payload))
                self.ingest(payload)
            elif message.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE,
                                  aiohttp.WSMsgType.CLOSED):
                break
        raise ConnectionError("Candle stream closed")

    async def stream_loop(self):
        await self.ready.wait()
        delay = 1
        timeout = aiohttp.ClientTimeout(total=None, connect=20)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            while True:
                self.changed.clear()
                self.connected = False
                self.received.clear()
                self.event_times.clear()
                self.books.clear()
                symbols = tuple(self.symbols)
                try:
                    if not symbols:
                        await asyncio.sleep(5)
                        continue
                    warmed = await self.seed(symbols)
                    if not warmed:
                        raise ConnectionError("No usable candle history")
                    if self.changed.is_set():
                        continue
                    streams = "/".join(self.stream_ids[s].lower() + "@kline_1m" for s in warmed)
                    url = WS_URL + "?" + urlencode({"streams": streams})
                    async with session.ws_connect(url, heartbeat=20, receive_timeout=30) as socket:
                        self.connected = True
                        log.info("CONTINUOUS WATCH CONNECTED | symbols=%d | signals checked on updates", len(warmed))
                        delay = 1
                        reader = asyncio.create_task(self.read_stream(socket))
                        change = asyncio.create_task(self.changed.wait())
                        reseed = asyncio.create_task(asyncio.sleep(30 if len(warmed) < len(symbols) else 23 * 3600))
                        try:
                            done, _ = await asyncio.wait((reader, change, reseed), return_when=asyncio.FIRST_COMPLETED)
                            if reader in done:
                                await reader
                        finally:
                            self.connected = False
                            reader.cancel()
                            change.cancel()
                            reseed.cancel()
                            await asyncio.gather(reader, change, reseed, return_exceptions=True)
                except Exception as error:
                    self.connected = False
                    log.warning("Candle stream reconnect in %ss: %s", delay, error)
                    await asyncio.sleep(delay)
                    delay = min(30, delay * 2)

    async def status_loop(self):
        while True:
            await asyncio.sleep(30)
            log.info("CONTINUOUS STATUS | connected=%s symbols=%d events=%d book_checks=%d alerts_queued=%d",
                            self.connected, len(self.symbols), self.events, self.book_checks, self.alerts_queued)

    async def run(self):
        tasks = [asyncio.create_task(self.universe_loop()), asyncio.create_task(self.stream_loop()),
                 asyncio.create_task(self.status_loop())]
        tasks.extend(asyncio.create_task(self.worker()) for _ in range(WORKERS))
        try:
            await asyncio.gather(*tasks)
        finally:
            self.connected = False
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
