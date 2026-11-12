"""Continuous candle monitoring; entry remains in the paper-only account."""

import asyncio
import math
import time
from collections import deque
from urllib.parse import urlencode

import aiohttp

import binance_cascade_hunter_dynamic_v2 as default_config

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
    def __init__(self, bot, *, config=default_config, clock=time.monotonic, wall_clock=time.time):
        self.bot = bot
        self.config = config
        self.clock = clock
        self.wall_clock = wall_clock
        self.symbols = ()
        self.histories = {}
        self.stream_ids = {}
        self.event_times = {}
        self.received = {}
        self.last_book_check = {}
        self.queue = asyncio.Queue(maxsize=self.config.MAX_SYMBOLS_TO_SCAN)
        self.pending = set()
        self.changed = asyncio.Event()
        self.ready = asyncio.Event()
        self.connected = False
        self.book_lock = asyncio.Lock()
        self.next_book_request = 0.0
        self.book_pause_until = 0.0
        self.events = self.book_checks = self.entries = 0

    async def refresh_universe(self):
        symbols = await self.bot.get_top_symbols()
        accepted = []
        for symbol in symbols:
            try:
                market = self.bot.exchange.market_data.market(symbol)
                if not (market.get("linear") and market.get("swap")
                        and market.get("settle") == "USDT" and market.get("active") is not False):
                    continue
                accepted.append(symbol)
                self.stream_ids[symbol] = market.get("id") or symbol.split(":")[0].replace("/", "")
            except Exception as error:
                self.config.log.warning("Skipping market %s: %s", symbol, error)
        selected = tuple(accepted[:self.config.MAX_SYMBOLS_TO_SCAN])
        if selected != self.symbols:
            self.symbols = selected
            self.changed.set()
        self.ready.set()
        self.config.log.info("CONTINUOUS UNIVERSE | symbols=%d | refresh=%ss", len(selected), self.config.SCAN_INTERVAL)

    async def universe_loop(self):
        while True:
            try:
                await self.refresh_universe()
            except Exception as error:
                self.config.log.warning("Universe refresh failed: %s", error)
                await asyncio.sleep(10)
                continue
            await asyncio.sleep(self.config.SCAN_INTERVAL)

    async def seed(self, symbols):
        semaphore = asyncio.Semaphore(WORKERS)

        async def one(symbol):
            async with semaphore:
                try:
                    rows = await self.bot.exchange.fetch_ohlcv(symbol, "1m", limit=30)
                    self.histories[symbol] = deque(valid_rows(rows), maxlen=30)
                    return symbol
                except Exception as error:
                    self.config.log.warning("Candle warmup failed %s: %s", symbol, error)
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

    def signal(self, symbol):
        if (not self.connected or symbol not in self.symbols or symbol not in self.received
                or self.clock() - self.received[symbol] > MAX_EVENT_AGE
                or self.wall_clock() - self.event_times[symbol] / 1000 > MAX_EVENT_AGE):
            return None
        rows = list(self.histories.get(symbol, ()))
        if len(rows) < 15:
            return None
        ret = (rows[-1][4] - rows[-3][4]) / rows[-3][4]
        average = sum(r[5] for r in rows[-15:-3]) / 12
        spike = rows[-1][5] / average if average > 0 else 0
        if abs(ret) <= self.config.CASCADE_THRESHOLD or spike <= self.config.VOLUME_SPIKE:
            return None
        return {"side": "buy" if ret > 0 else "sell", "price": rows[-1][4], "ret": ret, "spike": spike}

    async def evaluate(self, symbol):
        if symbol in self.bot.positions or len(self.bot.positions) >= self.config.MAX_CONCURRENT_POSITIONS:
            return
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
            book = await self.bot.exchange.fetch_order_book(symbol, limit=20)
            bids = sum(float(x[1]) for x in book["bids"][:20])
            asks = sum(float(x[1]) for x in book["asks"][:20])
            if not all(math.isfinite(v) and v >= 0 for v in (bids, asks)):
                return
            imbalance = (bids - asks) / (bids + asks) if bids + asks else 0
        except Exception as error:
            self.book_pause_until = self.clock() + 5
            self.config.log.warning("Order book check failed %s (backoff 5s): %s", symbol, error)
            return
        async with self.bot.lock:
            signal = self.signal(symbol)
            if (signal is None or symbol in self.bot.positions
                    or len(self.bot.positions) >= self.config.MAX_CONCURRENT_POSITIONS):
                return
            if not ((signal["side"] == "buy" and imbalance > 0.35)
                    or (signal["side"] == "sell" and imbalance < -0.35)):
                return
            # First eligible completed check gets the slot; do not wait for all symbols.
            if await self.bot.enter(symbol, signal["side"], signal["price"]):
                if hasattr(self.bot, "record_entry_signal"):
                    self.bot.record_entry_signal(symbol, {**signal, "imbalance": imbalance})
                self.entries += 1
                self.config.log.info("CONTINUOUS ENTRY %s | move=%.3f%% volume=%.2fx imbalance=%.3f",
                                symbol, signal["ret"] * 100, signal["spike"], imbalance)

    async def worker(self):
        while True:
            symbol = await self.queue.get()
            try:
                await self.evaluate(symbol)
            except Exception as error:
                self.config.log.warning("Continuous check failed %s: %s", symbol, error)
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
                        self.config.log.info("CONTINUOUS WATCH CONNECTED | symbols=%d | entries checked on updates", len(warmed))
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
                    self.config.log.warning("Candle stream reconnect in %ss: %s", delay, error)
                    await asyncio.sleep(delay)
                    delay = min(30, delay * 2)

    async def status_loop(self):
        while True:
            await asyncio.sleep(30)
            self.config.log.info("CONTINUOUS STATUS | connected=%s symbols=%d events=%d book_checks=%d entries=%d",
                            self.connected, len(self.symbols), self.events, self.book_checks, self.entries)

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
