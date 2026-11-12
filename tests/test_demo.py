import base64
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import parse_qsl, quote, urlsplit
from decimal import Decimal

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from demo_api import APIError, DemoAPI
from demo_trader import DemoTrader
from telegram_alerts import Alert


class DemoTests(unittest.IsolatedAsyncioTestCase):
    async def test_signed_request_is_demo_only_and_uses_rsa(self):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        session = MagicMock()
        response = session.request.return_value.__aenter__.return_value
        response.status = 200
        response.json = AsyncMock(return_value={"dualSidePosition": False})
        await DemoAPI(session, "demo-key", key).private("GET", "v1/positionSide/dual")
        (_, url), kwargs = session.request.call_args
        parsed = urlsplit(str(url))
        self.assertEqual(parsed.netloc, "demo-fapi.binance.com")
        self.assertFalse(kwargs["allow_redirects"])
        self.assertEqual(kwargs["headers"], {"X-MBX-APIKEY": "demo-key"})
        payload, _ = parsed.query.rsplit("&signature=", 1)
        signature = dict(parse_qsl(parsed.query))["signature"]
        self.assertEqual(url.raw_query_string, f"{payload}&signature={quote(signature, safe='')}")
        key.public_key().verify(base64.b64decode(signature), payload.encode(),
                                padding.PKCS1v15(), hashes.SHA256())

    async def test_unknown_entry_is_never_resent_and_reconciles_after_restart(self):
        api = MagicMock()
        api.market.return_value = {"id": "BTCUSDT", "filters": {
            "MARKET_LOT_SIZE": {"stepSize": "0.001", "minQty": "0.001", "maxQty": "100"},
            "PRICE_FILTER": {"tickSize": "0.10"},
        }}
        api.symbols = {"BTCUSDT": api.market.return_value}
        alert = Alert("BTC/USDT:USDT", "buy", 60000, 100, 97.25, 101, .02, 4, .6)
        api.private = AsyncMock(side_effect=[
            [], [], [],
            {"brackets": [
                {"notionalFloor": 0, "notionalCap": 10, "initialLeverage": 125},
                {"notionalFloor": 10, "notionalCap": 50, "initialLeverage": 50},
            ]},
            {"leverage": 50, "maxNotionalValue": "50"},
            APIError(503, None),
        ])
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/demo.sqlite3"
            trader = DemoTrader(api, path, Decimal(20), 1)
            with self.assertRaises(APIError):
                await trader.submit(time.monotonic(), alert)
            api.private.assert_any_await("POST", "v1/leverage", symbol="BTCUSDT", leverage=50)
            trader.db.close()

            trader = DemoTrader(api, path, Decimal(20), 1)
            self.addCleanup(trader.db.close)
            api.private.reset_mock()
            await trader.submit(time.monotonic(), alert)
            api.private.assert_not_awaited()
            row = trader.db.execute("SELECT * FROM demo_orders").fetchone()

            api.private.side_effect = [{"status": "FILLED", "executedQty": "0.200", "avgPrice": "100.70"}, []]
            await trader.reconcile(row)
            self.assertEqual(trader.db.execute("SELECT status FROM demo_orders").fetchone()[0], "pending")

            api.private.side_effect = [
                {"status": "FILLED", "executedQty": "0.200", "avgPrice": "100.70"},
                [{"positionAmt": "0.200"}],
                APIError(400, -2013), {}, APIError(400, -2013), {},
            ]
            await trader.reconcile(row)
            posts = [call.kwargs for call in api.private.await_args_list
                     if call.args == ("POST", "v1/algoOrder")]
            self.assertEqual([post["type"] for post in posts], ["STOP_MARKET", "TAKE_PROFIT_MARKET"])
            self.assertTrue(all(post["reduceOnly"] == "true" for post in posts))
            self.assertEqual([Decimal(post["triggerPrice"]) for post in posts],
                             [Decimal("97.70"), Decimal("101.00")])


    async def test_manual_close_cancels_own_protection_and_releases_slot(self):
        api = MagicMock()
        trader = DemoTrader(api, ":memory:", Decimal(20), 1)
        self.addCleanup(trader.db.close)
        with trader.db:
            trader.db.execute("""
                INSERT INTO demo_orders (id, symbol, side, stop, target, status)
                VALUES ('fd-test', 'BTCUSDT', 'buy', '97', '101', 'observed')
            """)
        api.private = AsyncMock(side_effect=[
            [],
            [{"side": "SELL", "orderId": 100, "time": 2000, "id": 5}],
            {"algoStatus": "NEW", "actualOrderId": ""}, {},
            {"algoStatus": "NEW", "actualOrderId": ""}, {},
            {"clientOrderId": "web-close"},
        ])
        row = trader.db.execute("SELECT * FROM demo_orders").fetchone()
        with self.assertLogs("demo_trader", level="INFO") as logs:
            await trader.reconcile(row, {"status": "FILLED", "executedQty": ".2",
                                         "time": 1000, "orderId": 99})
        self.assertIn("reason=external_or_manual", logs.output[0])
        self.assertEqual(tuple(trader.db.execute(
            "SELECT status, close_reason FROM demo_orders").fetchone()), ("done", "external_or_manual"))
        api.private.assert_any_await("DELETE", "v1/algoOrder", clientAlgoId="fd-test-sl")
        api.private.assert_any_await("DELETE", "v1/algoOrder", clientAlgoId="fd-test-tp")
        self.assertFalse(any(call.args[0] == "POST" for call in api.private.await_args_list))

    async def test_market_reply_without_average_recovers_exact_fills_and_protects(self):
        api = MagicMock()
        api.symbols = {"ZECUSDT": {"filters": {"PRICE_FILTER": {"tickSize": "0.01"}}}}
        trader = DemoTrader(api, ":memory:", Decimal(20), 1)
        self.addCleanup(trader.db.close)
        with trader.db:
            trader.db.execute("""
                INSERT INTO demo_orders (id, symbol, side, stop, target, status)
                VALUES ('fd-zec', 'ZECUSDT', 'buy', '', '110', 'pending')
            """)
        entry = {"orderId": 42, "status": "FILLED", "executedQty": "0.2"}
        api.private = AsyncMock(side_effect=[
            entry, [{"positionAmt": "0.2"}],
            [{"orderId": 42, "qty": "0.05", "price": "99"},
             {"orderId": 42, "qty": "0.15", "price": "103"}],
            APIError(400, -2013), {"algoStatus": "NEW"},
            APIError(400, -2013), {"algoStatus": "NEW"},
        ])
        row = trader.db.execute("SELECT * FROM demo_orders").fetchone()
        with self.assertLogs("demo_trader", level="INFO") as logs:
            await trader.reconcile(row, entry)
        self.assertEqual(tuple(trader.db.execute(
            "SELECT status, stop FROM demo_orders").fetchone()), ("observed", "98.94"))
        api.private.assert_any_await("GET", "v1/order", symbol="ZECUSDT",
                                     origClientOrderId="fd-zec")
        api.private.assert_any_await("GET", "v1/userTrades", symbol="ZECUSDT", orderId=42)
        self.assertTrue(all("DEMO PROTECTION ACCEPTED" in line for line in logs.output))
        self.assertFalse(any(call.args == ("POST", "v1/order") for call in api.private.await_args_list))
