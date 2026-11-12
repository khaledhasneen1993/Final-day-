import base64
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import parse_qsl, urlsplit
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
        parsed = urlsplit(url)
        self.assertEqual(parsed.netloc, "demo-fapi.binance.com")
        self.assertFalse(kwargs["allow_redirects"])
        self.assertEqual(kwargs["headers"], {"X-MBX-APIKEY": "demo-key"})
        payload, _ = parsed.query.rsplit("&signature=", 1)
        signature = dict(parse_qsl(parsed.query))["signature"]
        key.public_key().verify(base64.b64decode(signature), payload.encode(),
                                padding.PKCS1v15(), hashes.SHA256())

    async def test_unknown_entry_is_never_resent_and_reconciles_after_restart(self):
        api = MagicMock()
        api.market.return_value = {"id": "BTCUSDT", "filters": {
            "MARKET_LOT_SIZE": {"stepSize": "0.001", "minQty": "0.001", "maxQty": "100"},
            "PRICE_FILTER": {"tickSize": "0.10"},
        }}
        alert = Alert("BTC/USDT:USDT", "buy", 60000, 100, 97.25, 101, .02, 4, .6)
        api.private = AsyncMock(side_effect=[[], [], [], APIError(503, None)])
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/demo.sqlite3"
            trader = DemoTrader(api, path, Decimal(20), 1)
            with self.assertRaises(APIError):
                await trader.submit(time.monotonic(), alert)
            trader.db.close()

            trader = DemoTrader(api, path, Decimal(20), 1)
            self.addCleanup(trader.db.close)
            api.private.reset_mock()
            await trader.submit(time.monotonic(), alert)
            api.private.assert_not_awaited()
            row = trader.db.execute("SELECT * FROM demo_orders").fetchone()

            api.private.side_effect = [{"status": "FILLED", "executedQty": "0.200"}, []]
            await trader.reconcile(row)
            self.assertEqual(trader.db.execute("SELECT status FROM demo_orders").fetchone()[0], "pending")

            api.private.side_effect = [
                {"status": "FILLED", "executedQty": "0.200"},
                [{"positionAmt": "0.200"}],
                APIError(400, -2013), {}, APIError(400, -2013), {},
            ]
            await trader.reconcile(row)
            posts = [call.kwargs for call in api.private.await_args_list
                     if call.args == ("POST", "v1/algoOrder")]
            self.assertEqual([post["type"] for post in posts], ["STOP_MARKET", "TAKE_PROFIT_MARKET"])
            self.assertTrue(all(post["reduceOnly"] == "true" for post in posts))
            self.assertEqual([Decimal(post["triggerPrice"]) for post in posts],
                             [Decimal("97.20"), Decimal("101.00")])
