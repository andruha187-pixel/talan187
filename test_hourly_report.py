import os, shutil, importlib.util, zipfile, time
DATA='/tmp/prejump_v2014_report'
shutil.rmtree(DATA, ignore_errors=True); os.makedirs(DATA)
os.environ['DATA_DIR']=DATA
os.environ['TELEGRAM_BOT_TOKEN']=''; os.environ['TELEGRAM_CHAT_ID']=''
spec=importlib.util.spec_from_file_location('bot', os.path.join(os.path.dirname(__file__),'main.py'))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db(); bot.apply_v2014_safety_migration()
end=(int(time.time())//3600)*3600*1000; start=end-3600*1000
path,summary=bot.build_hourly_report(start,end)
assert path.exists(), path
with zipfile.ZipFile(path) as z:
    names=set(z.namelist())
required={'summary.txt','results_PJM03.csv','results_PJS.csv','results_PLC.csv','gate_decisions.csv','accepted_signals.csv','prelead_confirm_checks.csv','paper_trades.csv','paper_exits.csv','live_orders.csv'}
assert required <= names, required-names
assert 'PJM03:' in summary and 'PJS:' in summary and 'PLC:' in summary
print('v20.14 hourly ZIP report regression: OK')
