import unittest
from unittest.mock import AsyncMock, MagicMock

from telegram_alerts import TelegramAlerts


class TelegramAlertsTests(unittest.IsolatedAsyncioTestCase):
    async def test_short_alert_uses_recent_resistance_and_posts_expected_payload(self):
        alerts = TelegramAlerts("fake-token", "123")
        history = [
            [i, 100, high, 99, 100, 1]
            for i, high in enumerate([102, 103, 110, 103, 102, 104, 107, 104, 102, 120])
        ]
        alerts.enqueue("BTC/USDT:USDT", {"side": "sell", "price": 100}, history)
        response = MagicMock(status=200)
        response.json = AsyncMock(return_value={"ok": True})
        session = MagicMock()
        session.post.return_value.__aenter__.return_value = response

        await alerts.send(session, alerts.queue.get_nowait().message())

        session.post.assert_called_once_with(
            "https://api.telegram.org/botfake-token/sendMessage",
            json={"chat_id": "123", "text": (
                "العملة: BTC/USDT\nالاتجاه: Short\nالسعر الحالي: 100\n"
                "وقف الخسارة: 107\nالهدف: 99"
            )},
            allow_redirects=False,
        )
