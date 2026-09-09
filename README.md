# v20.13 PRE_LEAD_SAFE LIVE

Real-money-capable PRE_LEAD_SAFE build based on v20.12 low-latency execution and event-driven TP.

## What changed

Only one entry strategy is active per token: `PRE_LEAD_SAFE`.

Validated signal family kept from the paper lab:

- directional external score `0.34 <= score < 0.40`;
- score rises at least `+0.015` over approximately 300 ms;
- 300 ms projected score `>= 0.55`;
- at least 2 fresh same-direction venues;
- Polymarket signal ask `0.52..0.56`;
- 1 s PM ask momentum `-0.01..+0.05`;
- elapsed `1..160 s`.

The dedicated lab simulated Polymarket execution by waiting ~250 ms before an entry. **LIVE does not add another 250 ms sleep.** It submits the first FAK immediately after the validated early signal; the purpose of PRE_LEAD_SAFE is to reach Polymarket before the move while its own taker-order handling occurs.

The signal itself remains on the same 100 ms evaluation cadence used by the forward test. The old `EVENT_DRIVEN_LIVE_ENTRY` environment variable is intentionally ignored in v20.13 so the signal population does not silently change.

## Entry execution

- default 5 shares;
- hard execution cap = signal ask + `LIVE_ENTRY_MAX_SLIPPAGE` (default +0.05), never above the PRE_LEAD execution band max 0.66;
- presign prewarm is retained, so the real signal should normally avoid the old ~250 ms cold build/sign cost;
- `PRELEAD_LIVE_NO_MATCH_RETRIES=0` by default: one actual FAK attempt, closest to the paper live-sim methodology;
- ambiguous submissions remain fail-closed.

## Take profit

Default ENV target is **+$0.90 NET for the whole remaining position**.

LIVE TP remains event-driven from v20.12: PM BID updates immediately recalculate executable whole-position NET PnL and submit SELL FAK when the configured target is available. The 0.75 s loop is fallback only.

Telegram controls:

- `TAKE PROFIT` — show current TP;
- `TP 0.90` — exact value;
- `TP 1.00` — example alternative;
- `➖ TP` / `➕ TP` — change by $0.10;
- `TP OFF` / `TP ENV`.

If `/var/data` already contains state from v20.12, the persisted TP can override the `.env` default. **After deployment, press TAKE PROFIT or send `TP 0.90` explicitly.**

`LIVE_TP_MIN_HOLD_MS=2000` is retained because a newly bought outcome-token balance/allowance can take time to become sellable. This can delay a TP during the first 2 seconds after a fill, but avoids the balance/allowance rejection already observed in LIVE.

## One-time v20.13 safety migration

Because the entry strategy changed from PRE_JUMP to PRE_LEAD_SAFE, the first v20.13 boot intentionally:

- sets global new entries to STOPPED;
- sets every flat token mode to OFF;
- preserves already-open v20.12 positions so their TP/settlement can continue to be tracked;
- does not touch wallet credentials.

This prevents an old armed LIVE mode from automatically trading a new signal family.

## Recommended first deployment

1. Deploy and wait for `WALLET` to report READY.
2. Check `TAKE PROFIT`; send `TP 0.90` if that is the test target.
3. Check `MODES`.
4. Arm only the token(s) you want, for example `MODE BTC LIVE`, then `CONFIRM LIVE BTC` within 60 s.
5. Keep `SIZE ... 5` for the first real-money forward sample.
6. Press `START` only after modes and TP are correct.

No filter or TP value guarantees profit. The latest PRE_LEAD_SAFE sample was promising, but earlier forward periods were much weaker, so treat the first live run as a small-size validation rather than scaling size from the paper PnL.

## Persistent compatibility

The internal strategy key remains `BTC_PRE_JUMP`, `ETH_PRE_JUMP`, etc. on purpose. This lets v20.13 read existing v20.12 positions from the same SQLite DB while the user-visible strategy is `PRE-LEAD-SAFE`.
