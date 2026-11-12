"""Public market data and RSA-signed orders restricted to Binance USD-M Demo."""

import base64
import time
from urllib.parse import urlencode

import aiohttp
import ccxt
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from yarl import URL

REST_URL = "https://demo-fapi.binance.com"
WS_URL = "wss://demo-fstream.binance.com/market/stream"


class APIError(Exception):
    def __init__(self, status, code):
        super().__init__(f"Demo API HTTP {status}, code {code}")
        self.status = status
        self.code = code


class DemoAPI:
    def __init__(self, session, api_key, private_key):
        if not isinstance(private_key, rsa.RSAPrivateKey):
            raise ValueError("An RSA private key is required")
        self.session = session
        self.api_key = api_key
        self.private_key = private_key
        self.offset = 0
        self.retry_at = 0
        self.markets = {}
        self.symbols = {}

    async def request(self, method, path, params=None, *, signed=False):
        if time.monotonic() < self.retry_at:
            raise APIError(429, None)
        query = dict(params or {})
        headers = {}
        if signed:
            query.update(timestamp=int(time.time() * 1000) + self.offset, recvWindow=5000)
            signature = self.private_key.sign(urlencode(query).encode("ascii"),
                                              padding.PKCS1v15(), hashes.SHA256())
            query["signature"] = base64.b64encode(signature).decode("ascii")
            headers["X-MBX-APIKEY"] = self.api_key
        # Preserve the percent-encoded RSA signature, including encoded slashes.
        url = URL(REST_URL + path + "?" + urlencode(query), encoded=True)
        try:
            async with self.session.request(method, url, headers=headers,
                                            allow_redirects=False) as response:
                if response.status in (418, 429):
                    fallback = 120 if response.status == 418 else 10
                    try:
                        wait = int(response.headers.get("Retry-After", fallback))
                    except ValueError:
                        wait = fallback
                    self.retry_at = time.monotonic() + max(1, wait)
                try:
                    data = await response.json(content_type=None)
                except ValueError:
                    data = None
                code = data.get("code") if isinstance(data, dict) else None
                if response.status != 200 or isinstance(code, int) and code < 0 or data is None:
                    raise APIError(response.status, code)
                return data
        except (aiohttp.ClientError, TimeoutError):
            # Signed request URLs must never appear in exception logs.
            raise APIError(0, None) from None

    async def sync_time(self):
        before = int(time.time() * 1000)
        data = await self.request("GET", "/fapi/v1/time")
        self.offset = data["serverTime"] - (before + int(time.time() * 1000)) // 2

    async def load_markets(self):
        data = await self.request("GET", "/fapi/v1/exchangeInfo")
        self.markets = {
            f'{row["baseAsset"]}/USDT:USDT': {
                "id": row["symbol"], "linear": True, "swap": True, "settle": "USDT",
                "active": row["status"] == "TRADING",
                "filters": {f["filterType"]: f for f in row["filters"]},
            }
            for row in data["symbols"]
            if row["quoteAsset"] == row["marginAsset"] == "USDT"
            and row["contractType"] == "PERPETUAL"
        }

        self.symbols = {market["id"]: market for market in self.markets.values()}

    def market(self, symbol):
        try:
            return self.markets[symbol]
        except KeyError:
            raise ccxt.BadSymbol(symbol) from None

    async def fetch_tickers(self):
        data = await self.request("GET", "/fapi/v1/ticker/24hr")
        by_id = {row["symbol"]: row for row in data}
        return {
            symbol: {"quoteVolume": by_id[market["id"]]["quoteVolume"]}
            for symbol, market in self.markets.items() if market["id"] in by_id
        }

    async def fetch_ohlcv(self, symbol, timeframe, limit):
        return await self.request("GET", "/fapi/v1/klines",
                                  {"symbol": self.market(symbol)["id"], "interval": timeframe, "limit": limit})

    async def fetch_order_book(self, symbol, limit):
        return await self.request("GET", "/fapi/v1/depth", {"symbol": self.market(symbol)["id"], "limit": limit})

    async def private(self, method, endpoint, **params):
        return await self.request(method, "/fapi/" + endpoint, params, signed=True)

    async def check_account(self):
        mode = await self.private("GET", "v1/positionSide/dual")
        if mode["dualSidePosition"] not in (False, "false"):
            raise ValueError("Binance Demo Futures must be in One-way mode")
        await self.private("GET", "v3/positionRisk")
