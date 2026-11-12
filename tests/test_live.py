import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from binance_cascade_hunter_dynamic_v2_paper import V2PaperHunter
from run_paper_live import Publisher, read_config, run
from test_continuous import StreamData, NOW, event
from continuous_scanner import ContinuousScanner


class LiveTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'state.json'
        self.data=StreamData()
        self.bot=V2PaperHunter(self.path,fee_rate=0,market_data=self.data)
        self.publisher=Publisher(self.bot,{'url':'https://example.chatgpt.site/api/live','token':'test-only'},'test-session')

    async def test_entry_and_exit_diagnostics_survive_restore(self):
        scanner=ContinuousScanner(self.bot,clock=lambda:100,wall_clock=lambda:NOW/1000)
        await scanner.refresh_universe();await scanner.seed(scanner.symbols)
        scanner.connected=True;self.data.price=102
        scanner.ingest(event())
        await scanner.evaluate('BTC/USDT:USDT')
        fill=self.bot.exchange.trades[0]
        self.assertEqual(fill['entry_reason'],'CASCADE_MOVE_VOLUME_ORDERBOOK')
        self.assertAlmostEqual(fill['entry_signal']['imbalance'],.8)
        self.assertAlmostEqual(fill['stop_loss_price'],86.7)
        self.data.price=105
        await self.bot.manage_exits()
        closed=self.bot.exchange.trades[-1]
        self.assertEqual(closed['exit_reason'],'TP')
        self.assertEqual(closed['trigger_price'],105)
        restored=V2PaperHunter(self.path,fee_rate=0,market_data=self.data)
        self.assertEqual(restored.exchange.trades,self.bot.exchange.trades)

    async def test_short_sl_reason_and_trigger(self):
        await self.bot.enter('ETH/USDT:USDT','sell',100)
        self.data.price=116
        await self.bot.manage_exits()
        self.assertEqual(self.bot.exchange.trades[-1]['exit_reason'],'SL')
        self.assertEqual(self.bot.exchange.trades[-1]['trigger_price'],116)

    async def test_snapshot_is_copy_and_never_contains_token(self):
        await self.bot.enter('BTC/USDT:USDT','buy',100)
        snapshot=self.publisher.snapshot()
        self.bot.exchange.trades[0]['price']=999
        self.assertEqual(snapshot['trades'][0]['price'],100)
        self.assertNotIn('test-only',json.dumps(snapshot))
        self.assertEqual(snapshot['settings']['MAX_POSITION_PCT'],.1)

    async def test_upload_uses_private_header_and_rejects_redirects(self):
        captured={}
        class Response:
            status=200
            async def __aenter__(self):return self
            async def __aexit__(self,*args):pass
            async def json(self):return {'ok':True}
        class Session:
            def post(self,url,**kwargs):captured.update(kwargs);return Response()
        await self.publisher.upload(Session())
        self.assertFalse(captured['allow_redirects'])
        self.assertEqual(captured['headers']['OAI-Sites-Authorization'],'Bearer test-only')
        self.assertEqual(captured['json']['session_id'],'test-session')

    async def test_cancellation_saves_closes_and_marks_stopped(self):
        async def offline_stream(scanner): await asyncio.Future()
        states=[]
        async def loop(): await asyncio.Future()
        async def stopped():states.append('stopped')
        self.publisher.loop=loop;self.publisher.stopped=stopped
        with patch('continuous_scanner.ContinuousScanner.stream_loop',offline_stream):
            task=asyncio.create_task(run(self.bot,self.publisher))
            for _ in range(20):await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(self.path.exists());self.assertTrue(self.data.closed)
        self.assertEqual(states,['stopped'])

    async def test_config_validation(self):
        p=Path(self.temp.name)/'config.json'
        for url in ['http://example.chatgpt.site/api/live','https://evil.example/api/live','https://example.chatgpt.site/api/live?token=bad']:
            p.write_text(json.dumps({'url':url,'token':'x'}))
            with self.assertRaises(ValueError):read_config(p)
        p.write_text(json.dumps(self.publisher.config))
        self.assertEqual(read_config(p),self.publisher.config)
