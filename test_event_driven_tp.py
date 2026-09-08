import os, shutil, asyncio, importlib.util, time
from dataclasses import dataclass
from decimal import Decimal

DATA='/tmp/prejump_v2012_event_tp'
shutil.rmtree(DATA, ignore_errors=True); os.makedirs(DATA, exist_ok=True)
os.environ['DATA_DIR']=DATA
os.environ['LIVE_MASTER_ENABLE']='1'
os.environ['TAKE_PROFIT_USDC']='0.60'
os.environ['EVENT_DRIVEN_LIVE_TP']='1'
os.environ['LIVE_TP_EVENT_MIN_INTERVAL_MS']='0'
os.environ['LIVE_TP_MIN_HOLD_MS']='0'
os.environ['TELEGRAM_BOT_TOKEN']=''
os.environ['TELEGRAM_CHAT_ID']=''

spec=importlib.util.spec_from_file_location('bot', os.path.join(os.path.dirname(__file__),'main.py'))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db()
assert bot.VERSION.startswith('20.12-')
assert bot.EVENT_DRIVEN_LIVE_TP
V=bot.STRATEGIES_BY_SYMBOL['BTC'][0]
bot.state_set(f"mode:{V['name']}", 'LIVE')
bot.LIVE_MASTER_ENABLE=True; bot.live_client_ready=True; bot.sdk_post_order_with_allowance_recovery=None

@dataclass(frozen=True)
class Signed:
    token_id:str; price:str; size:str; side:str; post_only:bool=False; order_type:str='LIMIT'
class Accepted:
    ok=True; code=''; message=''; status='matched'; order_id='x'; trade_ids=('t',)
    def __init__(self, side, size, price):
        size=Decimal(str(size)); price=Decimal(str(price))
        if side=='BUY': self.making_amount=str(size*price); self.taking_amount=str(size)
        else: self.making_amount=str(size); self.taking_amount=str(size*price)
class Client:
    def __init__(self): self.posts=[]
    async def create_limit_order(self, **kw):
        return Signed(str(kw['token_id']),str(kw['price']),str(kw['size']),str(kw['side']),False)
    async def post_order(self, order):
        self.posts.append((order.side, float(order.price), float(order.size)))
        return Accepted(order.side, order.size, order.price)

async def run():
    bot.live_client=Client()
    cid='evt-tp'; asset='DN'; now=bot.now_ms()
    bot.markets[cid]={'condition_id':cid,'symbol':'BTC','up_asset':'UP','down_asset':asset,'start_ts':time.time()-30,'end_ts':time.time()+270}
    bot.books[asset]={'bids':{0.54:100.0},'asks':{0.55:100.0},'received_ms':now,'source':'ws','tick_size':0.01}
    buy=await bot.execute_live_fak(cid,V,asset,'Down','ENTRY','BUY',5.0)
    assert buy['ok'] and abs(buy['filled']-5)<1e-9, buy
    assert asset in bot.live_tp_watch_assets
    # One profitable BID event should trigger TP without waiting for 750ms timer fallback.
    bot.books[asset]={'bids':{0.88:100.0},'asks':{0.89:100.0},'received_ms':bot.now_ms(),'source':'ws','tick_size':0.01}
    trigger=bot.now_ms()
    bot.notify_live_tp_book_event(asset, trigger)
    for _ in range(50):
        await asyncio.sleep(0.01)
        if bot.position_totals(cid,V['name'])['remaining'] < 1e-8:
            break
    pos=bot.position_totals(cid,V['name'])
    assert pos['remaining'] < 1e-8, pos
    assert any(x[0]=='SELL' for x in bot.live_client.posts), bot.live_client.posts
    line=bot._tp_latency_line(cid,V['name'])
    assert 'path=book_event' in line, line
    assert 'event→submit' in line, line
    print('PRE-JUMP v20.12 event-driven LIVE TP regression: OK |', line)

asyncio.run(run())
