import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock

from aiohttp.test_utils import TestClient, TestServer

from dashboard import TradeLedger, dashboard_app
from telegram_alerts import Alert, TelegramAlerts


class DashboardTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_delivered_alerts_are_tracked_and_survive_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trades.sqlite3"
            ledger = TradeLedger(path)
            alerts = TelegramAlerts("fake-token", "123")
            alerts.ledger = ledger
            alerts.send = AsyncMock(side_effect=[False, True])
            rejected = Alert("ETH/USDT:USDT", "buy", 60, 100, 95, 101, .02, 4, .6)
            delivered = Alert("BTC/USDT:USDT", "sell", 60, 100, 105, 99, -.02, 4, -.6)
            alerts.queue.put_nowait(rejected)
            alerts.queue.put_nowait(delivered)
            task = asyncio.create_task(alerts.run())
            try:
                await asyncio.wait_for(alerts.queue.join(), 2)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

            self.assertEqual(ledger.snapshot(1)["counts"], {"open": 1})
            self.assertTrue(ledger.contains(delivered.symbol, delivered.side, delivered.candle))
            self.assertFalse(ledger.contains(rejected.symbol, rejected.side, rejected.candle))
            ledger.apply_prices({delivered.symbol: {"last": 98}}, requested_at=0)
            self.assertEqual(ledger.snapshot(1)["counts"], {"open": 1})
            ledger.apply_prices({delivered.symbol: {"last": 98}}, requested_at=time.time())
            ledger.db.close()

            reopened = TradeLedger(path)
            try:
                scanner = Mock()
                scanner.snapshot.return_value = {"connected": True, "symbols": []}
                async with TestClient(TestServer(dashboard_app(scanner, reopened))) as client:
                    async with client.get("/") as response:
                        self.assertEqual(response.status, 200)
                        self.assertIn("لوحة الإشارات", await response.text())
                    async with client.get("/api/status") as response:
                        result = await response.json()
                        self.assertEqual(result["trades"]["counts"], {"tp": 1})
                        self.assertEqual(result["trades"]["closed"][0]["exit_price"], 98)
                        self.assertAlmostEqual(result["trades"]["closed"][0]["change"], .02)
            finally:
                reopened.db.close()
