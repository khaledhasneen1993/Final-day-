"""Monitor Binance USDT-M markets and publish qualified signals to Telegram."""

import argparse
import asyncio
import logging

from aiohttp import web

import ccxt.async_support as ccxt

from continuous_scanner import ContinuousScanner
from dashboard import TradeLedger, dashboard_app
from telegram_alerts import TelegramAlerts


async def run(alerts, port, database):
    ledger = TradeLedger(database)
    alerts.ledger = ledger
    market_data = ccxt.binanceusdm({"enableRateLimit": True})
    scanner = ContinuousScanner(market_data, alerts)
    runner = web.AppRunner(dashboard_app(scanner, ledger))
    try:
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", port).start()
        logging.info("Dashboard available at http://127.0.0.1:%s", port)
        await market_data.load_markets()
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(alerts.run())
            tasks.create_task(scanner.run())
            tasks.create_task(ledger.follow(market_data))
    finally:
        await runner.cleanup()
        await market_data.close()
        ledger.db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--database", default="final-day.sqlite3")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
    alerts = TelegramAlerts.from_environment()
    if alerts is None:
        parser.error("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
    try:
        asyncio.run(run(alerts, args.port, args.database))
    except KeyboardInterrupt:
        logging.info("Telegram signal bot stopped")


if __name__ == "__main__":
    main()
