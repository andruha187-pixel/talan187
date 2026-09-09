# PRE-JUMP v20.14 — PJM03 / PJS / PLC

Version: `20.14-multi7-pjm03-pjs-plc-paper-live`

This build forward-tests three independent candidates on each configured 5-minute crypto token:

- `PJM03` — first PRE-JUMP >= 0.40, token-specific SAFE overlay, plus PM momentum <= +0.03.
- `PJS` — same first PRE-JUMP and token-specific SAFE overlay, without the new +0.03 filter.
- `PLC` — first PRE_LEAD_SAFE candidate, then a 125 ms confirmation before execution.

Default size is **5 shares** and whole-position NET TP is **+$1.05**. Fresh databases start with every branch in PAPER, global entries OFF, and no real order can be sent until LIVE is explicitly enabled and confirmed.

## PAPER live-like model

PJM03/PJS accepted signals wait 250 ms before the simulated FAK. PLC waits 125 ms for confirmation and then pays the same 250 ms simulated execution delay. The full requested size must still be visible no worse than the accepted ask +0.05; otherwise the attempt is NO FILL.

PAPER TP waits at least 2 seconds after BUY. When the NET target first becomes executable it freezes the actual sell limit, waits 250 ms, and only fills if the whole remaining size can still sell at that limit or better. NO_MATCH does not create PnL.

## LIVE safety and execution

LIVE keeps the v20.12/v20.13 prewarm/presign, signed LIMIT -> FAK path, deterministic NO_MATCH handling, fail-closed ambiguous submission handling, and FAST EVENT TP. v20.14 also hard-blocks two strategies from being LIVE on the same token at once.

The LIVE price normalizer never uses a tick finer than `LIVE_PRICE_TICK_FALLBACK=0.01`, preventing the ETH-style SDK rejection caused by three-decimal prices when the market accepts only 0.01 ticks.

The BTC post-BUY TP propagation fix is retained: LIVE TP waits 2 seconds by default; an explicit not-enough-balance/allowance rejection is classified as `REJECTED_BALANCE_ALLOWANCE` rather than ambiguous, and later TP cycles may retry safely. The startup migration also repairs old poisoned TP balance-rejection records.

Start with `LIVE_MASTER_ENABLE=0`. Verify `WALLET` reports READY before deliberately enabling LIVE.

## Telegram essentials

`START` / `STOP` control new entries globally.

Examples:

```text
MODE BTC PJM03 PAPER
MODE BTC PJM03 LIVE
CONFIRM LIVE BTC PJM03
MODE BTC PJS PAPER
MODE BTC PLC PAPER
SIZE BTC 5
SIZE ETH PLC 5
TP 1.05
TP OFF
TP ENV
MODES
SIZES
POSITIONS
STATISTICS
TRADES
WALLET
```

`SIZE BTC 5` sets all three BTC candidates to 5 shares. `SIZE BTC PLC 5` changes only PLC. PAPER candidates may run simultaneously; only one candidate per token may be LIVE.

## Persistent data

Mount persistent storage at `/var/data`.

- SQLite: `/var/data/prejump_v20_14_candidates.db`
- hourly report directory: `/var/data/prejump_v20_14_reports`

Hourly Telegram ZIPs contain separate `results_PJM03.csv`, `results_PJS.csv`, and `results_PLC.csv`, plus gates, accepted signals, PLC confirmation checks, PAPER trades/exits, LIVE orders, and `summary.txt`.

## Coolify

Use the included Dockerfile. Application port is `8080`; health endpoint is `/health`. Copy `.env.example` variables into Coolify Environment and keep the private key out of GitHub.

## Regression checks

From the package directory:

```bash
python -m py_compile main.py
python test_v2014_candidates.py
python test_paper_live_sim.py
python test_plc_confirm_entry.py
python test_hourly_report.py
python test_tick_alignment.py
python test_live_exec_tp.py
python test_tp_balance_sync.py
python test_prewarm.py
python test_presign_prewarm.py
```
