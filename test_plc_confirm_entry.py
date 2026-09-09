import os, shutil, asyncio, importlib.util, time
DATA='/tmp/prejump_v2014_plc'
shutil.rmtree(DATA, ignore_errors=True); os.makedirs(DATA)
os.environ['DATA_DIR']=DATA
os.environ['PRELEAD_CONFIRM_MS']='0'
os.environ['PAPER_ENTRY_SIM_DELAY_MS']='0'
os.environ['TELEGRAM_BOT_TOKEN']=''; os.environ['TELEGRAM_CHAT_ID']=''
spec=importlib.util.spec_from_file_location('bot', os.path.join(os.path.dirname(__file__),'main.py'))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db(); bot.apply_v2014_safety_migration()
V=[v for v in bot.STRATEGIES_BY_SYMBOL['BTC'] if v['code']=='PLC'][0]

async def run():
    cid='plc-confirm'; asset='UP'; now=bot.now_ms()
    market={'condition_id':cid,'symbol':'BTC','up_asset':asset,'down_asset':'DN','start_ts':time.time()-20,'end_ts':time.time()+280}
    bot.markets[cid]=market
    bot.books[asset]={'bids':{.54:100},'asks':{.55:100},'received_ms':bot.now_ms(),'source':'test','tick_size':.01}
    # Previous PM point lets the 1s confirmation momentum resolve to +0.01.
    bot.lead_pm_history[cid][asset].append((now-1000,.54))
    feature={
        'sample_ms':bot.now_ms(),'ext_score':.49,'fresh_venues':2,'up_votes':2,'down_votes':0,
        'binance':{'fresh':True,'score':.5},'bybit':{'fresh':True,'score':.5},
    }
    bot.lead_feature_history['BTC'].append(feature)
    st=bot.get_variant_state(cid,V); st['gate_decided']=True; st['gate_asset']=asset
    diag={'score_now':.50}
    ok=await bot._confirm_plc_candidate(market,V,asset,'Up',.54,.01,feature,diag,bot.now_ms(),'PAPER')
    assert ok
    # Confirmation schedules the normal 250ms live-like PAPER entry (0ms in this regression env).
    if bot.candidate_exec_tasks:
        await asyncio.gather(*list(bot.candidate_exec_tasks))
    pos=bot.position_totals(cid,V['name']); assert abs(pos['remaining']-5)<1e-8, pos
    with bot.db() as conn:
        row=conn.execute('SELECT status,reason FROM prelead_confirm_events WHERE condition_id=? ORDER BY id DESC LIMIT 1',(cid,)).fetchone()
        gate=conn.execute('SELECT passed,reason FROM gate_decisions WHERE condition_id=? AND variant=? ORDER BY decision_ms DESC LIMIT 1',(cid,V['name'])).fetchone()
    assert row['status']=='PASSED' and row['reason']=='ok', dict(row)
    assert gate['passed']==1 and gate['reason']=='PRELEAD_CONFIRM_PERSIST_OK', dict(gate)

asyncio.run(run())
print('v20.14 PLC confirmation -> PAPER execution regression: OK')
