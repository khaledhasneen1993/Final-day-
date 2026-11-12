"""Deliver qualified scanner signals to an optional Telegram chat."""

import asyncio
import logging
import os
from dataclasses import dataclass

import aiohttp

log = logging.getLogger(__name__)


def stop_level(candles, price, is_long):
    """Use the latest confirmed swing on the protective side of the signal."""
    index = 3 if is_long else 2
    levels = [row[index] for row in candles]
    is_protective = (lambda level: level < price) if is_long else (lambda level: level > price)
    extreme = min if is_long else max
    is_swing = (lambda level, neighbors: level < min(neighbors)) if is_long else (
        lambda level, neighbors: level > max(neighbors)
    )
    for i in range(len(levels) - 3, 1, -1):
        level = levels[i]
        neighbors = levels[i - 2:i] + levels[i + 1:i + 3]
        if is_protective(level) and is_swing(level, neighbors):
            return level
    return extreme((level for level in levels if is_protective(level)), default=None)


@dataclass(frozen=True)
class Alert:
    symbol: str
    side: str
    candle: int
    price: float
    stop: float
    target: float
    movement: float | None
    volume: float | None
    imbalance: float | None

    def message(self):
        return (
            f"العملة: {self.symbol.split(':', 1)[0]}\n"
            f"الاتجاه: {'Long' if self.side == 'buy' else 'Short'}\n"
            f"السعر الحالي: {self.price:.12g}\n"
            f"وقف الخسارة: {self.stop:.12g}\n"
            f"الهدف: {self.target:.12g}"
        )


def build_alert(symbol, signal, history):
    side, price = signal["side"], signal["price"]
    is_long = side == "buy"
    stop = stop_level(history[:-1], price, is_long)
    if stop is None:
        return None
    return Alert(symbol, side, history[-1][0], price, stop,
                 price * (1.01 if is_long else 0.99), signal.get("ret"),
                 signal.get("spike"), signal.get("imbalance"))


class TelegramAlerts:
    def __init__(self, token, chat_id):
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.chat_id = chat_id
        self.queue = asyncio.Queue(maxsize=100)
        self.last_candle = {}
        self.ledger = None

    @classmethod
    def from_environment(cls):
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if token and chat_id:
            return cls(token, chat_id)
        return None

    def enqueue(self, symbol, signal, history):
        side, price = signal["side"], signal["price"]
        key = (symbol, side)
        candle = history[-1][0]
        if candle <= self.last_candle.get(key, -1):
            return
        if self.ledger is not None and self.ledger.contains(symbol, side, candle):
            return
        alert = build_alert(symbol, signal, history)
        if alert is None:
            return
        try:
            self.queue.put_nowait(alert)
        except asyncio.QueueFull:
            log.warning("Telegram alert queue full for %s", symbol)
            return
        self.last_candle[key] = candle
        return True

    async def send(self, session, message):
        for attempt in range(3):
            delay = 2 ** attempt
            try:
                async with session.post(
                    self.url, json={"chat_id": self.chat_id, "text": message},
                    allow_redirects=False,
                ) as response:
                    if response.status == 200 and (await response.json()).get("ok") is True:
                        return True
                    if response.status == 429:
                        payload = await response.json()
                        delay = max(delay, int(payload.get("parameters", {}).get("retry_after", delay)))
                    elif response.status < 500:
                        log.warning("Telegram rejected the alert (HTTP %s)", response.status)
                        return
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError):
                # Telegram puts the token in the URL, so avoid logging exception text.
                pass
            if attempt < 2:
                await asyncio.sleep(delay)
        log.warning("Telegram alert delivery failed after three attempts")

    async def run(self):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            while True:
                alert = await self.queue.get()
                try:
                    if await self.send(session, alert.message()) and self.ledger is not None:
                        self.ledger.record(alert)
                finally:
                    self.queue.task_done()
