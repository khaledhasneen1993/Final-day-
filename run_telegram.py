"""Monitor Binance USDT-M markets and publish qualified signals to Telegram."""

import argparse
import asyncio
import logging

import ccxt.async_support as ccxt

from continuous_scanner import ContinuousScanner
from telegram_alerts import TelegramAlerts


async def run(alerts):
    market_data = ccxt.binanceusdm({"enableRateLimit": True})
    try:
        await market_data.load_markets()
        scanner = ContinuousScanner(market_data, alerts)
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(alerts.run())
            tasks.create_task(scanner.run())
    finally:
        await market_data.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
    alerts = TelegramAlerts.from_environment()
    if alerts is None:
        parser.error("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
    try:
        asyncio.run(run(alerts))
    except KeyboardInterrupt:
        logging.info("Telegram signal bot stopped")


if __name__ == "__main__":
    main()
