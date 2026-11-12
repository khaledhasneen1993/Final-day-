import asyncio

class PublicData:
    """No trading, balance or leverage endpoints exist on this fake client."""

    def __init__(self):
        self.price = 100.0
        self.closed = False

    async def load_markets(self):
        return {}

    async def fetch_ticker(self, symbol):
        await asyncio.sleep(0)
        return {"last": self.price}

    async def fetch_order_book(self, symbol, limit):
        return {"bids": [[100, 9]], "asks": [[100, 1]]}

    async def fetch_ohlcv(self, symbol, timeframe, limit):
        rows = [[i, 100, 100, 100, 100, 1] for i in range(limit)]
        rows[-1] = [limit, 100, 102, 100, 102, 10]
        return rows

    def amount_to_precision(self, symbol, amount):
        return str(int(amount * 1000) / 1000)

    def market(self, symbol):
        return {"linear": True, "swap": True, "settle": "USDT",
                "limits": {"amount": {"min": 0.001}, "cost": {"min": 5}}}

    async def close(self):
        self.closed = True

class DynamicPublicData(PublicData):
    def __init__(self):
        super().__init__()
        self.tickers = {s: {"quoteVolume": 20_000_000 + i * 1_000_000}
                        for i, s in enumerate(("BTC/USDT:USDT", "ETH/USDT:USDT",
                                               "SOL/USDT:USDT"))}

    async def fetch_tickers(self):
        return self.tickers

