import os, shutil, importlib.util

DATA='/tmp/prejump_v2014_candidates'
shutil.rmtree(DATA, ignore_errors=True); os.makedirs(DATA)
os.environ['DATA_DIR']=DATA
os.environ['TELEGRAM_BOT_TOKEN']=''; os.environ['TELEGRAM_CHAT_ID']=''
spec=importlib.util.spec_from_file_location('bot', os.path.join(os.path.dirname(__file__),'main.py'))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db(); bot.apply_v2014_safety_migration()

assert bot.VERSION.startswith('20.14-'), bot.VERSION
assert len(bot.STRATEGIES)==21, len(bot.STRATEGIES)
assert [v['code'] for v in bot.STRATEGIES_BY_SYMBOL['BTC']]==['PJM03','PJS','PLC']
assert abs(bot.take_profit_usdc()-1.05)<1e-12
assert abs(bot.TAKE_PROFIT_STEP_USDC-0.05)<1e-12
assert not bot.trading_enabled()
assert all(bot.strategy_mode(v['name'])=='PAPER' for v in bot.STRATEGIES)

# Frozen token SAFE overlays.
def D(score): return {'score':score}
assert bot._pj_safe_token_filter('BTC',D(.44),.54,.53,.02)[0]
assert not bot._pj_safe_token_filter('BTC',D(.42),.54,.53,.02)[0]
assert bot._pj_safe_token_filter('XRP',D(.41),.60,.59,.02)[0]
assert not bot._pj_safe_token_filter('XRP',D(.409),.60,.59,.02)[0]
assert bot._pj_safe_token_filter('BNB',D(.40),.60,.59,.01)[0]
assert not bot._pj_safe_token_filter('BNB',D(.40),.60,.59,.009)[0]
assert bot._pj_safe_token_filter('SOL',D(.40),.60,.59,.02)[0]
assert not bot._pj_safe_token_filter('SOL',D(.40),.60,.59,.021)[0]
assert bot._pj_safe_token_filter('DOGE',D(.40),.60,.58,.02)[0]
assert not bot._pj_safe_token_filter('DOGE',D(.40),.60,.57,.02)[0]
assert bot._pj_safe_token_filter('ETH',D(.455),.60,.59,.02)[0]
assert not bot._pj_safe_token_filter('ETH',D(.454),.60,.59,.02)[0]
assert bot._pj_safe_token_filter('HYPE',D(.40),.60,.59,.04)[0]
assert abs(bot.PJM03_PM_MOM_MAX-.03)<1e-12

# PLC 125ms confirmation filter: candidate score may fade at most .01, but not below base min.
ok,why,floor=bot._plc_confirm_filter(.50,.49,.55,2,2)
assert ok and why=='ok' and abs(floor-.49)<1e-12
ok,why,floor=bot._plc_confirm_filter(.50,.489,.55,2,2)
assert not ok and why=='confirm_score_faded'
assert bot.PRELEAD_CONFIRM_MS==125

# One LIVE branch per token; PAPER branches can coexist.
pjm,pjs,plc=bot.STRATEGIES_BY_SYMBOL['BTC']
ok,msg=bot._set_mode_direct(pjs,'LIVE'); assert ok, msg
ok,msg=bot._set_mode_direct(pjm,'LIVE'); assert not ok and 'already LIVE' in msg, msg
ok,msg=bot._set_mode_direct(pjs,'PAPER'); assert ok, msg
ok,msg=bot._set_mode_direct(pjm,'LIVE'); assert ok, msg
ok,msg=bot._set_mode_direct(plc,'PAPER'); assert ok, msg

# Tick regression: never trust a book tick finer than 0.01 fallback.
bot.books['TICK']={'tick_size':.001}
assert bot._asset_tick_size('TICK') == bot.Decimal('0.01')
assert bot._normalize_live_limit_price('TICK',.537,'BUY') == bot.Decimal('0.53')

print('v20.14 candidates/safety/tick regression: OK')
