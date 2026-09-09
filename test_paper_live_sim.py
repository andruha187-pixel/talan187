import os, shutil, asyncio, importlib.util, time
DATA='/tmp/prejump_v2014_paper_sim'
shutil.rmtree(DATA, ignore_errors=True); os.makedirs(DATA)
os.environ['DATA_DIR']=DATA
os.environ['PAPER_ENTRY_SIM_DELAY_MS']='0'
os.environ['PAPER_TP_SIM_MIN_HOLD_MS']='0'
os.environ['PAPER_TP_SIM_DELAY_MS']='20'
os.environ['TAKE_PROFIT_USDC']='1.05'
os.environ['TELEGRAM_BOT_TOKEN']=''; os.environ['TELEGRAM_CHAT_ID']=''
spec=importlib.util.spec_from_file_location('bot', os.path.join(os.path.dirname(__file__),'main.py'))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db(); bot.apply_v2014_safety_migration()
V=bot.STRATEGIES_BY_SYMBOL['BTC'][0]

async def run():
    cid='paper-live-sim'; asset='UP'
    market={'condition_id':cid,'symbol':'BTC','up_asset':asset,'down_asset':'DN','start_ts':time.time()-20,'end_ts':time.time()+280}
    bot.markets[cid]=market
    bot.books[asset]={'bids':{.53:100},'asks':{.54:100},'received_ms':bot.now_ms(),'source':'test','tick_size':.01}
    ok=await bot.execute_paper_live_sim_entry(cid,V,asset,'Up',.54)
    assert ok, 'entry should fill'
    pos=bot.position_totals(cid,V['name']); assert abs(pos['remaining']-5)<1e-8, pos

    # Trigger TP at .80, then remove that price during the simulated 20ms SELL hold.
    bot.books[asset]={'bids':{.80:100},'asks':{.81:100},'received_ms':bot.now_ms(),'source':'test','tick_size':.01}
    armed=await bot.maybe_take_profit(market,V,30)
    assert armed
    bot.books[asset]={'bids':{.70:100},'asks':{.71:100},'received_ms':bot.now_ms(),'source':'test','tick_size':.01}
    await asyncio.sleep(.05)
    pos=bot.position_totals(cid,V['name']); assert abs(pos['remaining']-5)<1e-8, pos
    with bot.db() as conn:
        exits=conn.execute('SELECT COUNT(*) n FROM paper_exits WHERE condition_id=?',(cid,)).fetchone()['n']
    assert exits==0, exits

    # A later real touch that survives the delay may fill.
    bot.books[asset]={'bids':{.80:100},'asks':{.81:100},'received_ms':bot.now_ms(),'source':'test','tick_size':.01}
    armed=await bot.maybe_take_profit(market,V,31); assert armed
    await asyncio.sleep(.05)
    pos=bot.position_totals(cid,V['name']); assert pos['remaining']<1e-8, pos
    with bot.db() as conn:
        pnl=conn.execute('SELECT pnl FROM market_results WHERE condition_id=? AND variant=?',(cid,V['name'])).fetchone()['pnl']
    assert pnl > 1.05, pnl

asyncio.run(run())
print('v20.14 PAPER live-like ENTRY/TP NO_MATCH regression: OK')
