import os, shutil, asyncio, importlib.util, time
from dataclasses import dataclass
from decimal import Decimal
DATA='/tmp/v2013_live_exec'; shutil.rmtree(DATA, ignore_errors=True); os.makedirs(DATA)
os.environ['DATA_DIR']=DATA; os.environ['LIVE_MASTER_ENABLE']='1'; os.environ['TAKE_PROFIT_USDC']='0.90'
os.environ['LIVE_TP_MIN_HOLD_MS']='0'; os.environ['LIVE_TP_EVENT_MIN_INTERVAL_MS']='0'
os.environ['TELEGRAM_BOT_TOKEN']=''; os.environ['TELEGRAM_CHAT_ID']=''
spec=importlib.util.spec_from_file_location('bot', os.path.join(os.path.dirname(__file__),'main.py'))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db()
V=bot.STRATEGIES_BY_SYMBOL['BTC'][0]; bot.state_set(f"mode:{V['name']}",'LIVE')
bot.LIVE_MASTER_ENABLE=True; bot.live_client_ready=True; bot.sdk_post_order_with_allowance_recovery=None
@dataclass(frozen=True)
class Signed:
    token_id:str; price:str; size:str; side:str; post_only:bool=False; order_type:str='LIMIT'
class Accepted:
    ok=True; code=''; message=''; status='matched'; order_id='x'; trade_ids=('t',)
    def __init__(self,side,size,price):
        size=Decimal(str(size)); price=Decimal(str(price))
        if side=='BUY': self.making_amount=str(size*price); self.taking_amount=str(size)
        else: self.making_amount=str(size); self.taking_amount=str(size*price)
class Client:
    def __init__(self): self.posts=[]
    async def create_limit_order(self, **kw):
        return Signed(str(kw['token_id']),str(kw['price']),str(kw['size']),str(kw['side']),False)
    async def post_order(self, order):
        self.posts.append((order.side,float(order.price),float(order.size)))
        return Accepted(order.side,order.size,order.price)
bot.live_client=Client()

async def run():
    cid='live'; asset='UP'; bot.markets[cid]={'condition_id':cid,'symbol':'BTC','up_asset':asset,'down_asset':'DN','start_ts':time.time()-20,'end_ts':time.time()+280}
    # Signal ask .56 => hard cap .61. Current ask .60 is allowed and FAK submits at .61.
    bot.books[asset]={'bids':{.59:100},'asks':{.60:100},'received_ms':bot.now_ms(),'source':'ws','tick_size':.01}
    r=await bot.execute_live_fak(cid,V,asset,'Up','ENTRY','BUY',5.0,reference_price=.56,force_rest=False)
    assert r['ok'] and abs(r['filled']-5)<1e-9, r
    assert bot.live_client.posts[-1][0]=='BUY' and abs(bot.live_client.posts[-1][1]-.61)<1e-9, bot.live_client.posts
    # Event-driven TP at .90 target should sell immediately from a strong bid.
    bot.books[asset]={'bids':{.90:100},'asks':{.91:100},'received_ms':bot.now_ms(),'source':'ws','tick_size':.01}
    bot.notify_live_tp_book_event(asset,bot.now_ms())
    for _ in range(100):
        await asyncio.sleep(.01)
        if bot.position_totals(cid,V['name'])['remaining']<1e-8: break
    pos=bot.position_totals(cid,V['name'])
    assert pos['remaining']<1e-8, pos
    assert any(x[0]=='SELL' for x in bot.live_client.posts), bot.live_client.posts
    assert 'path=book_event' in bot._tp_latency_line(cid,V['name'])
asyncio.run(run())
print('v20.14 LIVE cap + event TP regression: OK')
