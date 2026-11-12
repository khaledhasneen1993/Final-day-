"""Run approved V2 paper trading and send snapshots to its private dashboard."""
import argparse
import asyncio
import copy
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import aiohttp
from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter

log = logging.getLogger("CASCADE")


def read_config(path):
    config = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    url = urlparse(config.get("url", ""))
    if (url.scheme != "https" or not url.hostname or not url.hostname.endswith(".chatgpt.site")
            or url.path != "/api/live" or url.username or url.password or url.query
            or url.fragment or url.port not in (None, 443)):
        raise ValueError("Dashboard config must point to an HTTPS Sites /api/live endpoint")
    if not isinstance(config.get("token"), str) or not config["token"].strip():
        raise ValueError("Dashboard config is missing its service token")
    return config


class Publisher:
    def __init__(self, bot, config, session_id):
        self.bot, self.config, self.session_id = bot, config, session_id

    def snapshot(self, status="running"):
        bot = self.bot
        # No await while copying the ledger; fills cannot interleave in this event loop.
        return copy.deepcopy({
            "session_id": self.session_id, "status": status,
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "initial_balance_usdt": 100.0, "settings": bot.settings(),
            "fee_rate": bot.exchange.fee_rate,
            "balance_usdt": bot.exchange.balance, "equity_usdt": bot.exchange.equity,
            "free_usdt": bot.exchange.free, "used_usdt": bot.exchange.used,
            "account_positions": bot.exchange.positions,
            "strategy_positions": bot.positions, "prices": bot.exchange.prices,
            "trades": bot.exchange.trades,
            "scanner": {"connected": bot.scanner.connected,
                        "symbols": len(bot.scanner.symbols), "events": bot.scanner.events,
                        "entries": bot.scanner.entries},
        })

    async def upload(self, session, status="running"):
        async with session.post(
            self.config["url"], json=self.snapshot(status), allow_redirects=False,
            headers={"OAI-Sites-Authorization": "Bearer " + self.config["token"]},
        ) as response:
            if response.status != 200:
                raise RuntimeError(f"Dashboard HTTP {response.status}")
            result = await response.json()
            if result.get("ok") is not True:
                raise RuntimeError("Dashboard did not acknowledge snapshot")

    async def loop(self):
        delay = 10
        online = False
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
            while True:
                try:
                    await self.upload(session)
                    if not online:
                        log.info("DASHBOARD CONNECTED | session=%s", self.session_id)
                    online = True
                    delay = 10
                except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError, ValueError):
                    # Never print credentials, response bodies or headers.
                    log.warning("DASHBOARD UPDATE FAILED | trading continues; retrying")
                    online = False
                    delay = min(30, delay * 2)
                await asyncio.sleep(delay)

    async def stopped(self):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                await self.upload(session, "stopped")
        except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError, ValueError):
            log.warning("Final dashboard update unavailable; local state remains saved")




class CloudPublisher:
    """Telemetry-only publisher for the combined private dashboard."""
    URL = "https://coamowlsyuqbxngijxsu.supabase.co/functions/v1/cascade-dashboard"

    def __init__(self, bot, session_id, token):
        self.bot, self.session_id, self.token = bot, session_id, token

    def snapshot(self, status="running"):
        bot = self.bot
        return copy.deepcopy({
            "session_id": self.session_id, "status": status,
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "initial_balance_usdt": 100.0, "settings": bot.settings(),
            "fee_rate": bot.exchange.fee_rate,
            "balance_usdt": bot.exchange.balance, "equity_usdt": bot.exchange.equity,
            "free_usdt": bot.exchange.free, "used_usdt": bot.exchange.used,
            "account_positions": bot.exchange.positions,
            "strategy_positions": bot.positions, "prices": bot.exchange.prices,
            "trades": bot.exchange.trades,
            "scanner": {"connected": bot.scanner.connected,
                        "symbols": len(bot.scanner.symbols), "events": bot.scanner.events,
                        "entries": bot.scanner.entries},
        })

    async def upload(self, session, status="running"):
        async with session.post(
            self.URL, json=self.snapshot(status),
            headers={"x-bot-id": "final", "x-bot-token": self.token},
        ) as response:
            if response.status != 200 or (await response.json()).get("ok") is not True:
                raise RuntimeError("Combined dashboard rejected snapshot")

    async def loop(self):
        delay = 10
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
            while True:
                try:
                    await self.upload(session)
                    delay = 10
                except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError, ValueError):
                    log.warning("COMBINED DASHBOARD UPDATE FAILED | trading continues")
                    delay = min(30, delay * 2)
                await asyncio.sleep(delay)

    async def stopped(self):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
                await self.upload(session, "stopped")
        except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError, ValueError):
            log.warning("Combined dashboard final update unavailable; local state remains saved")


async def run(bot, publisher=None, cloud_publisher=None):
    tasks = [asyncio.create_task(bot.start())]
    if publisher is not None:
        tasks.append(asyncio.create_task(publisher.loop()))
    if cloud_publisher is not None:
        tasks.append(asyncio.create_task(cloud_publisher.loop()))
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if publisher is not None:
            await publisher.stopped()
        if cloud_publisher is not None:
            await cloud_publisher.stopped()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="dashboard-config.json")
    parser.add_argument("--legacy-dashboard", action="store_true", help="Also publish to the old private Sites dashboard")
    parser.add_argument("--resume", metavar="STATE_FILE", help="Resume a prior V2 session instead of creating one")
    args = parser.parse_args()
    config = None
    if args.legacy_dashboard:
        try:
            config = read_config(args.config)
        except (OSError, ValueError, TypeError):
            parser.error("Legacy dashboard config is missing or invalid")
    if args.resume:
        state_path = Path(args.resume)
        if not state_path.is_file():
            parser.error("Resume state does not exist; refusing to start a new account")
        session_id = state_path.stem
        if not session_id.replace("_", "").replace("-", "").isalnum() or len(session_id) > 100:
            parser.error("Use a state filename containing only letters, numbers, underscores and hyphens")
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        session_id = "paper_state_live_" + stamp + "_" + uuid4().hex[:8]
        state_path = Path(session_id + ".json")
    bot = V2PaperHunter(state_path=state_path)
    publisher = Publisher(bot, config, session_id) if config else None
    cloud_token = os.environ.get("CASCADE_DASHBOARD_TOKEN", "").strip()
    cloud_publisher = CloudPublisher(bot, session_id, cloud_token) if cloud_token else None
    if publisher is None and cloud_publisher is None:
        parser.error("Set CASCADE_DASHBOARD_TOKEN, or use --legacy-dashboard with a valid config")
    if cloud_publisher:
        log.info("COMBINED DASHBOARD TELEMETRY ENABLED | session=%s", session_id)
    log.info("LIVE V2 PAPER | state=%s | margin=10%% | leverage=50x | price SL=15%% | max=3", state_path)
    log.warning("Paper model excludes liquidation, funding and slippage")
    try:
        asyncio.run(run(bot, publisher, cloud_publisher))
    except KeyboardInterrupt:
        log.info("Paper session stopped and local state saved")


if __name__ == "__main__":
    main()
