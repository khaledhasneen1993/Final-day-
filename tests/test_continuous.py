import asyncio
import unittest
from unittest.mock import patch

from continuous_scanner import CandleGap, ContinuousScanner
from telegram_alerts import TelegramAlerts

NOW = 1_800_000_000_000
BTC = "BTC/USDT:USDT"
ETH = "ETH/USDT:USDT"
SOL = "SOL/USDT:USDT"


class StreamData:
    def __init__(self):
        self.tickers = {symbol: {"quoteVolume": 40_000_000} for symbol in (BTC, ETH, SOL)}

    async def fetch_tickers(self):
        return self.tickers

    def market(self, symbol):
        return {"linear": True, "swap": True, "settle": "USDT", "active": True,
                "id": symbol.split(":")[0].replace("/", "")}

    async def fetch_order_book(self, symbol, limit):
        return {"bids": [[100, 9]], "asks": [[100, 1]]}

    async def fetch_ohlcv(self, symbol, timeframe, limit):
        return [[NOW - (29 - i) * 60000, 100, 100, 100, 100, 1] for i in range(30)]


def event(symbol="BTCUSDT", close=102, volume=10, stamp=NOW, offset=1):
    return {"e": "kline", "E": stamp + offset, "s": symbol, "st": 1,
            "k": {"s": symbol, "i": "1m", "t": stamp, "o": "100",
                  "h": str(max(100, close)), "l": str(min(100, close)),
                  "c": str(close), "v": str(volume)}}


class ContinuousTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.data = StreamData()
        self.alerts = TelegramAlerts("fake-token", "123")
        self.mono = 100.0
        self.wall = NOW / 1000
        self.scanner = ContinuousScanner(
            self.data, self.alerts, clock=lambda: self.mono, wall_clock=lambda: self.wall,
        )
        await self.scanner.refresh_universe()
        await self.scanner.seed(self.scanner.symbols)
        self.scanner.connected = True

    async def test_ranked_watchlist_caps_at_fifty_eligible_markets(self):
        self.data.tickers = {
            f"TOKEN{i}/USDT:USDT": {"quoteVolume": 15_000_000 + i}
            for i in range(55)
        }
        self.data.tickers[BTC] = {"quoteVolume": 14_999_999}
        await self.scanner.refresh_universe()
        self.assertEqual(len(self.scanner.symbols), 50)
        self.assertEqual(self.scanner.symbols[0], "TOKEN54/USDT:USDT")
        self.assertEqual(self.scanner.symbols[-1], "TOKEN5/USDT:USDT")
        self.assertNotIn(BTC, self.scanner.symbols)

    async def test_qualified_signal_is_queued_once_per_candle(self):
        for offset in range(1, 10):
            self.scanner.ingest(event(close=101.1, offset=offset))
        self.assertEqual(self.scanner.queue.qsize(), 1)
        await self.scanner.evaluate(BTC)
        self.mono += 1
        await self.scanner.evaluate(BTC)

        self.assertEqual(self.alerts.queue.qsize(), 1)
        self.assertEqual(self.alerts.queue.get_nowait().splitlines(), [
            "العملة: BTC/USDT", "الاتجاه: Long", "السعر الحالي: 101.1",
            "وقف الخسارة: 100", "الهدف: 102.111",
        ])

    async def test_stale_and_disconnected_data_cannot_alert(self):
        self.scanner.ingest(event())
        self.mono += 11
        await self.scanner.evaluate(BTC)
        self.assertTrue(self.alerts.queue.empty())
        self.mono = 100
        self.scanner.connected = False
        await self.scanner.evaluate(BTC)
        self.assertTrue(self.alerts.queue.empty())
        self.assertEqual(self.scanner.book_checks, 0)

    async def test_signal_rechecked_after_slow_orderbook_request(self):
        self.scanner.ingest(event())
        original = self.data.fetch_order_book

        async def changed_book(symbol, limit):
            self.scanner.ingest(event(close=100, offset=2))
            return await original(symbol, limit)

        self.data.fetch_order_book = changed_book
        await self.scanner.evaluate(BTC)
        self.assertTrue(self.alerts.queue.empty())

    async def test_orderbook_filter_remains_required(self):
        self.scanner.ingest(event())

        async def balanced(symbol, limit):
            return {"bids": [[102, 1]], "asks": [[102, 1]]}

        self.data.fetch_order_book = balanced
        await self.scanner.evaluate(BTC)
        self.assertTrue(self.alerts.queue.empty())

    async def test_old_events_are_ignored_and_candle_gap_requires_resync(self):
        self.scanner.ingest(event(offset=5))
        self.scanner.ingest(event(close=110, offset=4))
        self.assertEqual(self.scanner.histories[BTC][-1][4], 102)
        self.wall += 120
        with self.assertRaises(CandleGap):
            self.scanner.ingest(event(stamp=NOW + 120000))

    async def test_refresh_updates_symbols_and_ignores_removed_markets(self):
        self.scanner.ingest(event())
        self.data.tickers = {ETH: {"quoteVolume": 40_000_000}}
        await self.scanner.refresh_universe()
        self.assertEqual(self.scanner.symbols, (ETH,))
        self.assertIsNone(self.scanner.signal(BTC))

    async def test_failed_book_check_backs_off_without_alert(self):
        self.scanner.ingest(event())

        async def unavailable(symbol, limit):
            raise ConnectionError("offline")

        self.data.fetch_order_book = unavailable
        await self.scanner.evaluate(BTC)
        self.assertEqual(self.scanner.book_pause_until, 105)
        self.assertTrue(self.alerts.queue.empty())

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
