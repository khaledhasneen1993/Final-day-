"""Continuous paper runtime for the approved V2 strategy."""

import asyncio
from pathlib import Path

import ccxt.async_support as ccxt

import binance_cascade_hunter_dynamic_v2 as dynamic
from paper_account import PaperState, PaperExchange
from continuous_scanner import ContinuousScanner


class DynamicPaperExchange(PaperExchange):
    async def fetch_tickers(self):
        return await self.market_data.fetch_tickers()


class DynamicPaperHunter(dynamic.CascadeHunter, PaperState):
    def __init__(self, state_path="paper_state_dynamic_v2.json", fee_rate=0.0005,
                 reset=False, market_data=None, strategy_config=dynamic):
        self.strategy = strategy_config
        # Only public market data is used; all fills go through PaperExchange.
        public = market_data if market_data is not None else ccxt.binanceusdm({
            "enableRateLimit": True, "options": {"defaultType": "future"},
        })
        self.exchange = DynamicPaperExchange(public, fee_rate, leverage=self.strategy.LEVERAGE)
        self.positions = {}
        self.last_trade = {}
        self.lock = asyncio.Lock()
        self.state_path = Path(state_path)
        if self.state_path.exists() and not reset:
            self.restore()
        self.scanner = ContinuousScanner(self, config=self.strategy)

    def settings(self):
        return {"mode": "dynamic_scan", **{name: getattr(self.strategy, name) for name in (
            "LEVERAGE", "MAX_POSITION_PCT", "CASCADE_THRESHOLD", "VOLUME_SPIKE",
            "TAKE_PROFIT_PCT", "STOP_LOSS_PCT", "COOLDOWN_SEC", "SCAN_INTERVAL",
            "MAX_SYMBOLS_TO_SCAN", "MAX_CONCURRENT_POSITIONS", "MIN_QUOTE_VOLUME",
        )}}

    async def enter(self, symbol, side, price):
        # ContinuousScanner owns the entry lock; do not acquire it again here.
        result = await self.strategy.CascadeHunter.enter(self, symbol, side, float(price))
        self.save()
        return result

    def record_entry_signal(self, symbol, signal):
        for trade in reversed(self.exchange.trades):
            if trade["symbol"] == symbol and not trade["reduceOnly"]:
                trade["entry_signal"] = dict(signal)
                trade["entry_reason"] = "CASCADE_MOVE_VOLUME_ORDERBOOK"
                position = self.positions[symbol]
                trade["take_profit_price"] = position["tp"]
                trade["stop_loss_price"] = position["sl"]
                self.save()
                break

    def record_exit_reason(self, symbol, reason, trigger_price):
        for trade in reversed(self.exchange.trades):
            if trade["symbol"] == symbol and trade["reduceOnly"]:
                trade["exit_reason"] = reason
                trade["trigger_price"] = float(trigger_price)
                break

    async def manage_exits(self):
        await self.strategy.CascadeHunter.manage_exits(self)
        self.save()

    async def scan_loop(self):
        await self.scanner.run()

    async def start(self):
        tasks = []
        try:
            await self.exchange.load_markets()
            self.save()
            dynamic.log.info("CONTINUOUS PAPER ONLY | initial=100 USDT | wallet=%.4f | state=%s",
                             self.exchange.balance, self.state_path)
            tasks = [asyncio.create_task(self.scan_loop()),
                     asyncio.create_task(self.exit_watcher()),
                     asyncio.create_task(self.report())]
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            try:
                self.save()
            finally:
                await self.exchange.close()

