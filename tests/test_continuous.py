import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import aiohttp

from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter as DynamicPaperHunter
from continuous_scanner import CandleGap, ContinuousScanner
from fixtures import DynamicPublicData

NOW = 1_800_000_000_000
BTC = "BTC/USDT:USDT"
ETH = "ETH/USDT:USDT"
SOL = "SOL/USDT:USDT"


class StreamData(DynamicPublicData):
    async def fetch_ohlcv(self, symbol, timeframe, limit):
        return [[NOW - (29 - i) * 60000, 100, 100, 100, 100, 1] for i in range(30)]


def event(symbol="BTCUSDT", close=102, volume=10, stamp=NOW, offset=1):
    return {"e": "kline", "E": stamp + offset, "s": symbol, "st": 1,
            "k": {"s": symbol, "i": "1m", "t": stamp, "o": "100",
                  "h": str(max(100, close)), "l": str(min(100, close)),
                  "c": str(close), "v": str(volume)}}


class ContinuousTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "state.json"
        self.data = StreamData()
        self.bot = DynamicPaperHunter(self.path, fee_rate=0, market_data=self.data)
        self.mono = 100.0
        self.wall = NOW / 1000
        self.scanner = ContinuousScanner(self.bot, clock=lambda: self.mono, wall_clock=lambda: self.wall)
        await self.scanner.refresh_universe()
        await self.scanner.seed(self.scanner.symbols)
        self.scanner.connected = True
        self.data.price = 102

    async def test_first_signal_enters_without_waiting_for_other_symbol(self):
        release = asyncio.Event()
        entered = asyncio.Event()
        original_book = self.data.fetch_order_book
        original_enter = self.bot.enter

        async def order_book(symbol, limit):
            if symbol == ETH:
                await release.wait()
            return await original_book(symbol, limit)

        async def enter(symbol, side, price):
            result = await original_enter(symbol, side, price)
            if symbol == BTC and result:
                entered.set()
            return result

        self.data.fetch_order_book = order_book
        self.bot.enter = enter
        self.scanner.ingest(event("ETHUSDT"))
        self.scanner.ingest(event("BTCUSDT"))
        with patch("continuous_scanner.BOOK_REQUEST_SPACING", 0):
            workers = [asyncio.create_task(self.scanner.worker()) for _ in range(2)]
            try:
                await asyncio.wait_for(entered.wait(), 1)
                self.assertIn(BTC, self.bot.positions)
                self.assertNotIn(ETH, self.bot.positions)
                release.set()
                await asyncio.wait_for(self.scanner.queue.join(), 1)
            finally:
                release.set()
                for task in workers:
                    task.cancel()
                await asyncio.gather(*workers, return_exceptions=True)

    async def test_concurrent_signals_open_three_positions(self):
        for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
            self.scanner.ingest(event(symbol))
        with patch("continuous_scanner.BOOK_REQUEST_SPACING", 0):
            await asyncio.gather(*(self.scanner.evaluate(s) for s in (BTC, ETH, SOL)))
        self.assertEqual(len(self.bot.positions), 3)
        self.assertEqual(len(self.bot.exchange.trades), 3)
        self.assertGreaterEqual(self.bot.exchange.free, 0)

    async def test_duplicate_updates_coalesce_and_do_not_duplicate_entry(self):
        for i in range(1, 30):
            self.scanner.ingest(event(offset=i))
        self.assertEqual(self.scanner.queue.qsize(), 1)
        await asyncio.gather(self.scanner.evaluate(BTC), self.scanner.evaluate(BTC))
        self.assertEqual(len(self.bot.exchange.trades), 1)

    async def test_stale_and_disconnected_data_cannot_enter(self):
        self.scanner.ingest(event())
        self.mono += 11
        await self.scanner.evaluate(BTC)
        self.assertFalse(self.bot.positions)
        self.mono = 100
        self.scanner.connected = False
        await self.scanner.evaluate(BTC)
        self.assertFalse(self.bot.positions)
        self.assertEqual(self.scanner.book_checks, 0)

    async def test_signal_rechecked_after_slow_orderbook_request(self):
        self.scanner.ingest(event())
        original = self.data.fetch_order_book

        async def changed_book(symbol, limit):
            self.scanner.ingest(event(close=100, offset=2))
            return await original(symbol, limit)

        self.data.fetch_order_book = changed_book
        await self.scanner.evaluate(BTC)
        self.assertFalse(self.bot.positions)

    async def test_orderbook_filter_remains_required(self):
        self.scanner.ingest(event())

        async def balanced(symbol, limit):
            return {"bids": [[102, 1]], "asks": [[102, 1]]}

        self.data.fetch_order_book = balanced
        await self.scanner.evaluate(BTC)
        self.assertFalse(self.bot.positions)

    async def test_old_events_are_ignored_and_candle_gap_requires_resync(self):
        self.scanner.ingest(event(offset=5))
        self.scanner.ingest(event(close=110, offset=4))
        self.assertEqual(self.scanner.histories[BTC][-1][4], 102)
        self.wall += 120
        with self.assertRaises(CandleGap):
            self.scanner.ingest(event(stamp=NOW + 120000))

    async def test_refresh_updates_symbols_and_preserves_open_position(self):
        self.scanner.ingest(event())
        await self.scanner.evaluate(BTC)
        self.data.tickers = {ETH: {"quoteVolume": 40_000_000}}
        await self.scanner.refresh_universe()
        self.assertEqual(self.scanner.symbols, (ETH,))
        self.assertIn(BTC, self.bot.positions)
        self.assertIsNone(self.scanner.signal(BTC))
        self.data.price = 105
        await self.bot.manage_exits()
        self.assertNotIn(BTC, self.bot.positions)

    async def test_failed_book_check_backs_off_without_entry(self):
        self.scanner.ingest(event())

        async def unavailable(symbol, limit):
            raise ConnectionError("offline")

        self.data.fetch_order_book = unavailable
        await self.scanner.evaluate(BTC)
        self.assertEqual(self.scanner.book_pause_until, 105)
        self.assertFalse(self.bot.positions)

    async def test_socket_reconnect_reseeds_and_closes_cleanly(self):
        opened_twice = asyncio.Event()
        tracker = {"connections": 0, "seeds": 0, "closed": False}
        original_seed = self.scanner.seed

        async def seed(symbols):
            tracker["seeds"] += 1
            return await original_seed(symbols)

        class Socket:
            async def __aenter__(socket):
                tracker["connections"] += 1
                socket.number = tracker["connections"]
                if socket.number == 2:
                    opened_twice.set()
                return socket

            async def __aexit__(socket, *args):
                return False

            def __aiter__(socket):
                async def messages():
                    if socket.number == 1:
                        raise ConnectionError("simulated disconnect")
                    await asyncio.Future()
                    yield None
                return messages()

        class Session:
            async def __aenter__(session):
                return session

            async def __aexit__(session, *args):
                tracker["closed"] = True

            def ws_connect(session, url, **kwargs):
                self.assertIn("/market/stream?streams=", url)
                return Socket()

        self.scanner.seed = seed
        with patch("continuous_scanner.aiohttp.ClientSession", return_value=Session()):
            task = asyncio.create_task(self.scanner.stream_loop())
            try:
                await asyncio.wait_for(opened_twice.wait(), 3)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertGreaterEqual(tracker["seeds"], 2)
        self.assertTrue(tracker["closed"])
        self.assertFalse(self.scanner.connected)


if __name__ == "__main__":
    unittest.main()
