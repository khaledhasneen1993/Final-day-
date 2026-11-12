"""Approved V2 account; optional local runner without dashboard publishing."""

import argparse
import asyncio
import math

import binance_cascade_hunter_dynamic_v2 as v2
from paper_runtime import DynamicPaperHunter


class V2PaperHunter(DynamicPaperHunter):
    def __init__(self, state_path="paper_state_dynamic_v2.json", fee_rate=0.0005,
                 reset=False, market_data=None):
        super().__init__(state_path, fee_rate, reset, market_data, strategy_config=v2)

    def settings(self):
        return {**super().settings(), "mode": "dynamic_v2_continuous",
                "position_basis": "wallet_total", "stop_basis": "signal_price"}

    async def size_position(self, symbol):
        return await v2.CascadeHunter.size_position(self, symbol)

    async def get_top_symbols(self):
        return await v2.CascadeHunter.get_top_symbols(self)

    async def get_orderbook_imbalance(self, symbol, depth=20):
        return await v2.CascadeHunter.get_orderbook_imbalance(self, symbol, depth)

    async def detect_cascade(self, symbol):
        return await v2.CascadeHunter.detect_cascade(self, symbol)

    async def set_leverage(self, symbol):
        return await v2.CascadeHunter.set_leverage(self, symbol)

    async def exit_watcher(self):
        return await v2.CascadeHunter.exit_watcher(self)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default="paper_state_dynamic_v2.json")
    parser.add_argument("--reset", action="store_true", help="Start a fresh virtual $100 V2 account")
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    args = parser.parse_args()
    if not math.isfinite(args.fee_rate) or not 0 <= args.fee_rate < 0.01:
        parser.error("--fee-rate must be finite and in [0, 0.01)")
    bot = V2PaperHunter(args.state, args.fee_rate, args.reset)
    v2.log.info("V2 PAPER | margin=10%% wallet per trade | leverage=50x | SL=15%% PRICE | max_positions=3")
    v2.log.warning("V2 paper account does not model liquidation, funding or slippage")
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        v2.log.info("V2 paper account saved; stopped")


if __name__ == "__main__":
    main()
