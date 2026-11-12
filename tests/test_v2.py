import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import binance_cascade_hunter_dynamic_v2 as v2
from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter
from continuous_scanner import ContinuousScanner
from test_continuous import NOW, StreamData, event


class V2Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "v2.json"
        self.data = StreamData()
        self.data.tickers["DOGE/USDT:USDT"] = {"quoteVolume": 25_000_000}
        self.bot = V2PaperHunter(self.path, fee_rate=0, market_data=self.data)

    async def test_ten_percent_wallet_margin_on_each_of_three_entries(self):
        for symbol in ("BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"):
            self.assertTrue(await self.bot.enter(symbol, "buy", 100))
            self.assertAlmostEqual(self.bot.positions[symbol]["amount"], 5)
            self.assertAlmostEqual(self.bot.exchange.positions[symbol]["margin"], 10)
        self.assertAlmostEqual(self.bot.exchange.used, 30)
        self.assertAlmostEqual(self.bot.exchange.free, 70)
        self.assertFalse(await self.bot.enter("DOGE/USDT:USDT", "buy", 100))
        self.assertEqual(len(self.bot.positions), 3)

    async def test_long_stop_is_fifteen_percent_price_and_tp_unchanged(self):
        await self.bot.enter("BTC/USDT:USDT", "buy", 100)
        position = self.bot.positions["BTC/USDT:USDT"]
        self.assertAlmostEqual(position["sl"], 85)
        self.assertAlmostEqual(position["tp"], 101.8)
        self.data.price = 99
        await self.bot.manage_exits()
        self.assertIn("BTC/USDT:USDT", self.bot.positions)
        self.data.price = 85
        await self.bot.manage_exits()
        self.assertFalse(self.bot.positions)
        self.assertAlmostEqual(self.bot.exchange.balance, 25)

    async def test_short_stop_is_fifteen_percent_price(self):
        await self.bot.enter("ETH/USDT:USDT", "sell", 100)
        position = self.bot.positions["ETH/USDT:USDT"]
        self.assertAlmostEqual(position["sl"], 115)
        self.assertAlmostEqual(position["tp"], 98.2)
        self.data.price = 110
        await self.bot.manage_exits()
        self.assertTrue(self.bot.positions)
        self.data.price = 116
        await self.bot.manage_exits()
        self.assertFalse(self.bot.positions)
        self.assertAlmostEqual(self.bot.exchange.balance, 20)

    async def test_incompatible_state_rejected_without_modifying_file(self):
        self.bot.save()
        state = json.loads(self.path.read_text())
        state["settings"]["MAX_POSITION_PCT"] = 0.85
        self.path.write_text(json.dumps(state))
        before = self.path.read_bytes()
        with self.assertRaises(ValueError):
            V2PaperHunter(self.path, fee_rate=0, market_data=self.data)
        self.assertEqual(self.path.read_bytes(), before)

    async def test_saved_v2_restores_positions_and_settings(self):
        await self.bot.enter("SOL/USDT:USDT", "buy", 100)
        restored = V2PaperHunter(self.path, fee_rate=0, market_data=self.data)
        self.assertEqual(restored.positions, self.bot.positions)
        self.assertEqual(restored.exchange.positions, self.bot.exchange.positions)
        settings = json.loads(self.path.read_text())["settings"]
        self.assertEqual(settings["position_basis"], "wallet_total")
        self.assertEqual(settings["stop_basis"], "signal_price")
        self.assertEqual(settings["MAX_CONCURRENT_POSITIONS"], 3)

    async def test_continuous_scanner_uses_v2_limit_of_three(self):
        scanner = ContinuousScanner(self.bot, config=v2, clock=lambda: 100,
                                    wall_clock=lambda: NOW / 1000)
        await scanner.refresh_universe()
        await scanner.seed(scanner.symbols)
        scanner.connected = True
        self.data.price = 102
        for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT"):
            scanner.ingest(event(symbol))
        with patch("continuous_scanner.BOOK_REQUEST_SPACING", 0):
            await asyncio.gather(*(scanner.evaluate(s) for s in scanner.symbols))
        self.assertEqual(len(self.bot.positions), 3)
        self.assertEqual(len(self.bot.exchange.trades), 3)

    async def test_wallet_based_sizing_includes_fees_and_precision(self):
        bot = V2PaperHunter(self.path, fee_rate=0.0005, market_data=self.data)
        await bot.enter("BTC/USDT:USDT", "buy", 100)
        self.assertAlmostEqual(bot.exchange.balance, 99.75)
        await bot.enter("ETH/USDT:USDT", "buy", 100)
        amount = bot.positions["ETH/USDT:USDT"]["amount"]
        self.assertAlmostEqual(amount, 4.987)
        self.assertGreater(bot.exchange.free, 0)


if __name__ == "__main__":
    unittest.main()
