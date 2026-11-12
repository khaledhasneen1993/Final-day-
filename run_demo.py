"""Run Final Day against Binance USD-M Demo with an RSA key."""

import argparse
import asyncio
import logging
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path

import aiohttp
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from continuous_scanner import ContinuousScanner
from demo_api import APIError, DemoAPI, WS_URL
from demo_trader import DemoTrader


async def run(args, api_key, key):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
        api = DemoAPI(session, api_key, key)
        if args.check:
            await api.load_markets()
            await api.sync_time()
            await api.check_account()
            logging.info("Demo RSA check passed | markets=%d | no orders submitted", len(api.markets))
            return

        delay = 2
        while True:
            try:
                await api.load_markets()
                break
            except APIError as error:
                logging.warning("Demo market setup will retry in %ss: %s", delay, error)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
        trader = DemoTrader(api, args.database, args.notional, args.max_positions)
        try:
            scanner = ContinuousScanner(api, trader, ws_url=WS_URL)
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(scanner.run())
                tasks.create_task(trader.run())
        finally:
            trader.db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="Read-only Demo account check")
    mode.add_argument("--trade-demo", action="store_true", help="Enable Demo order submission")
    parser.add_argument("--notional", help="Required for trading: target USDT position value before fees")
    parser.add_argument("--max-positions", type=int, default=1)
    parser.add_argument("--database", default="final-day-demo.sqlite3")
    args = parser.parse_args()
    if args.trade_demo:
        try:
            args.notional = Decimal(args.notional or "")
        except InvalidOperation:
            parser.error("--notional must be a positive USDT amount")
        if not args.notional.is_finite() or args.notional <= 0 or args.max_positions < 1:
            parser.error("Provide positive notional and maximum positions")

    api_key = os.getenv("BINANCE_DEMO_API_KEY", "").strip()
    key_file = os.getenv("BINANCE_DEMO_RSA_KEY_FILE", "").strip()
    if not api_key or not key_file:
        parser.error("Set BINANCE_DEMO_API_KEY and BINANCE_DEMO_RSA_KEY_FILE")
    try:
        key = load_pem_private_key(Path(key_file).read_bytes(), password=None)
    except (OSError, ValueError, TypeError):
        parser.error("Unable to read the unencrypted RSA PEM private key")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
    try:
        asyncio.run(run(args, api_key, key))
    except (APIError, ValueError) as error:
        parser.exit(1, f"{error}\n")
    except KeyboardInterrupt:
        logging.info("Demo runner stopped")


if __name__ == "__main__":
    main()
