import os, shutil, asyncio, importlib.util, time
from dataclasses import dataclass

DATA='/tmp/v2013_prelead_safe'
shutil.rmtree(DATA, ignore_errors=True); os.makedirs(DATA, exist_ok=True)
os.environ['DATA_DIR']=DATA
os.environ['LIVE_MASTER_ENABLE']='0'
os.environ['PRELEAD_PAPER_SIM_DELAY_MS']='0'
os.environ['TAKE_PROFIT_USDC']='0.90'
os.environ['TELEGRAM_BOT_TOKEN']=''; os.environ['TELEGRAM_CHAT_ID']=''

spec=importlib.util.spec_from_file_location('bot', os.path.join(os.path.dirname(__file__),'main.py'))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db()
assert bot.VERSION.startswith('20.13-')
assert abs(bot.take_profit_usdc()-0.90)<1e-12
assert bot.PRELEAD_EVENT_DRIVEN_ENTRY is False
assert bot.PRELEAD_LIVE_NO_MATCH_RETRIES == 0
assert abs(bot.PRELEAD_SAFE_PROJECTED_SCORE-0.55)<1e-12
assert abs(bot.PRELEAD_SAFE_PRICE_MAX-0.56)<1e-12
V=bot.STRATEGIES_BY_SYMBOL['BTC'][0]
assert V['code']=='PLS' and 'PRE-LEAD-SAFE' in V['short']

calls=[]
async def fake_execute(*args, **kwargs):
    calls.append((args, kwargs)); return True
async def fake_refresh(asset):
    return 0.54, 0.55, 10
bot.execute_order=fake_execute
bot._refresh_entry_book_if_needed=fake_refresh

now=bot.now_ms(); cid='pass'
market={'condition_id':cid,'symbol':'BTC','up_asset':'UP','down_asset':'DN','start_ts':time.time()-20,'end_ts':time.time()+280}
# 0.37 now, 0.18 ~300ms ago => projected 0.56.
prior={'sample_ms':now-300,'ext_score':0.18,'up_votes':2,'down_votes':0,'fresh_venues':2,
       'binance':{'fresh':True,'score':0.30},'bybit':{'fresh':True,'score':0.30},'coinbase':None}
cur={'sample_ms':now,'ext_score':0.37,'up_votes':2,'down_votes':0,'fresh_venues':2,
     'binance':{'fresh':True,'score':0.35},'bybit':{'fresh':True,'score':0.35},'coinbase':None}
bot.lead_feature_history['BTC'].append(prior); bot.lead_feature_history['BTC'].append(cur)
bot.lead_pm_history[cid]['UP'].append((now-1000,0.54)); bot.lead_pm_history[cid]['UP'].append((now,0.55))

async def tests():
    r=await bot.evaluate_prejump_variant(market,V,20.0,cur)
    assert r and len(calls)==1, (r,calls)
    kw=calls[0][1]
    assert kw['reference_price']==0.55 and kw['evaluation_path']=='prelead_100ms'

    # projected below 0.55 -> no signal
    cid2='projfail'; m2=dict(market,condition_id=cid2)
    n=bot.now_ms(); p={'sample_ms':n-300,**{k:v for k,v in prior.items() if k!='sample_ms'}}; p['ext_score']=0.30
    c={'sample_ms':n,**{k:v for k,v in cur.items() if k!='sample_ms'}}; c['ext_score']=0.37
    bot.lead_feature_history['BTC'].clear(); bot.lead_feature_history['BTC'].extend([p,c])
    bot.lead_pm_history[cid2]['UP'].extend([(n-1000,0.54),(n,0.55)])
    r=await bot.evaluate_prejump_variant(m2,V,20.0,c)
    assert not r and len(calls)==1

    # current score already >=0.40 is no longer a LEAD signal
    cid3='crossed'; m3=dict(market,condition_id=cid3); n=bot.now_ms()
    p={'sample_ms':n-300,**{k:v for k,v in prior.items() if k!='sample_ms'}}; p['ext_score']=0.20
    c={'sample_ms':n,**{k:v for k,v in cur.items() if k!='sample_ms'}}; c['ext_score']=0.41
    bot.lead_feature_history['BTC'].clear(); bot.lead_feature_history['BTC'].extend([p,c])
    bot.lead_pm_history[cid3]['UP'].extend([(n-1000,0.54),(n,0.55)])
    r=await bot.evaluate_prejump_variant(m3,V,20.0,c)
    assert not r and len(calls)==1

asyncio.run(tests())

# One-time strategy-change migration forces explicit re-arming for flat tokens.
bot.state_set('trading_enabled','1'); bot.state_set(f"mode:{V['name']}",'LIVE')
assert bot.apply_v2013_safety_migration()
assert not bot.trading_enabled() and bot.strategy_mode(V['name'])=='OFF'
assert not bot.apply_v2013_safety_migration()

# TP runtime supports the planned 0.90 value and bounded Telegram range.
kind,val=bot._telegram_tp_value('0.90'); assert kind=='VALUE' and abs(val-.90)<1e-12
try:
    bot._telegram_tp_value('9.00'); raise AssertionError('TP range should reject 9')
except ValueError: pass

print('v20.13 PRE_LEAD_SAFE signal/migration/TP regression: OK')
