# MULTI7 SAFE67 A/B/C/E — PAPER/LIVE + NET TP 0.60

Trading build of the uploaded MULTI7 A/B/C/E PAPER bot.

Version:

```text
19.2-multi7-abce-paper-live-telegram-tp-retry
```

## Tokens and strategies

Default tokens:

```text
BTC
XRP
BNB
SOL
ETH
DOGE
HYPE
```

Four strategies per token = **28 independent strategy accounts**:

```text
A
B
C
E
```

Each has its own mode:

```text
PAPER
LIVE
OFF
```

Fresh database defaults every strategy to `PAPER`.
Global trading starts `OFF`; use `START`.

## Strategy logic preserved

### A — SAFE67 BASE

```text
FIRST V2 eligible:
price 0.55..0.75
momentum 0.03..0.30

ENTRY:
price 0.67..0.75
momentum 0.05..0.10
default 5 shares

No DCA
No stop-loss
```

### B — SAFE67 old reversal DCA

```text
ENTRY:
price 0.67..0.75
momentum 0.05..0.10
default 5 shares

DCA arm:
held-side ask <= 0.50
elapsed <= 120 sec
NO BUY on arm tick

Later:
momentum >= +0.05
ask <= 0.60
default +5 shares
one DCA
```

B intentionally keeps no `0.30` floor and no `+0.15` rebound cap.

### C — tighter entry + safer reversal DCA

```text
ENTRY:
price 0.67..0.70
momentum 0.05..0.10
default 5 shares

DCA arm:
ask <= 0.50
elapsed <= 120 sec
NO BUY on arm tick

Later:
ask 0.30..0.60
momentum +0.05..+0.15
default +5 shares
one DCA
```

### E — cross-token consensus

```text
target entry:
price 0.67..0.75
momentum 0.05..0.10

confirmation:
>= 2 DISTINCT OTHER tokens
with A/BASE SAFE67 PASS
same direction
previous 10 sec

default ENTRY 5 shares
No DCA
```

The target token does not count itself. One other token counts once.

A signals are still evaluated as consensus sources even when A's trading mode is
`OFF`; `OFF` blocks order execution, not signal/gate recording.

No strategy switches sides and there is no stop-loss.

## Telegram-configurable NET take-profit

Hosting default:

```text
TAKE_PROFIT_USDC=0.60
```

This remains **+$0.60 NET for the whole remaining position**, not per share.

The threshold calculation is unchanged and includes:

```text
entry gross cost
+ entry commission
- prior exit net
- projected current sell net
including projected exit commission
```

The important change in this version is that **you do not need a redeploy to
change TP anymore**.

In Telegram press:

```text
🎯 TAKE PROFIT
```

or send:

```text
TP
```

The bot shows both the current active TP and the hosting ENV/default.

Change it immediately:

```text
TP 0.30
TP 0.60
TP 1.00
```

Decimal comma is also accepted:

```text
TP 0,60
```

Disable TP:

```text
TP OFF
```

Return to the value from `TAKE_PROFIT_USDC` in Render/Coolify:

```text
TP ENV
```

`TAKE PROFIT 0.60` is also accepted.

The Telegram value is stored in the existing SQLite `state` table and is loaded
again after a bot restart. It therefore survives restarts as long as `/var/data`
is persistent.

`TAKE_PROFIT_USDC` in the hosting environment is now the **default/fallback**.
After a Telegram value has been saved, changing the hosting variable alone does
not overwrite that saved value. Use `TP ENV` to deliberately return to the
hosting value.

A Telegram TP change applies immediately to all open positions whose TP has
not already been latched, and to all future positions. For example, if an open
position has about `+$0.70 NET`, changing `TP 0.90` to `TP 0.60` makes it
eligible on the next monitoring cycle without a restart.

For B/C after a DCA, the configured amount is still the target for the
**whole remaining position**, including all buys and fees.

If a LIVE take-profit has already partially filled, liquidation is intentionally
latched. `TP OFF` or a later TP change does **not** cancel that already-started
real liquidation; the bot continues toward flat. This preserves the previous
LIVE partial-fill safety behavior.


## LIVE TAKE-PROFIT: retry after deterministic FAK no-match

This version fixes one narrow LIVE execution case without changing A/B/C/E
signals, entry/DCA rules, TP threshold logic, or true-ambiguity protection.

If Polymarket rejects a TAKE_PROFIT SELL with the deterministic message:

```text
no orders found to match with FAK order
```

the bot now records:

```text
status = REJECTED_NO_MATCH
filled_shares = 0
```

and does **not** mark the action AMBIGUOUS.

On each later strategy cycle (~3 seconds), the existing TP monitor recalculates
the current executable whole-position NET PnL:

- if NET PnL is still >= the active TP, it submits a fresh SELL FAK;
- if NET PnL has fallen below TP, it waits and does not sell below the target;
- if NET PnL becomes valid again later, retries resume.

This zero-fill no-match case is different from a timeout, 503, connection error,
or any other exception where submission/fill status may be unknown. Those remain
`AMBIGUOUS` and fail-closed exactly as before, with no automatic duplicate retry.

A genuine partial LIVE TP fill keeps the existing latch behavior: liquidation
continues toward flat even if the TP setting later changes or is turned OFF.

Only the first no-match for a position sends a Telegram explanation to avoid
spamming the chat every ~3 seconds. Retries remain visible in runtime logs.

### PAPER TP

PAPER requires enough visible bid depth to sell the entire remaining position.
It does not record a partial PAPER take-profit merely to hit the threshold.

### LIVE TP

LIVE checks the same bot-tracked NET target, freshness-checks the book and uses
the protected real-order path:

```text
signed LIMIT -> FAK SELL
```

A genuine partial LIVE TP fill is recorded. Once a real TP has partially filled,
TP becomes latched and the bot continues trying to flatten the bot-tracked
remainder on later cycles.

An ambiguous submission remains fail-closed; it is not blindly duplicated.

`STOP` blocks new ENTRY/DCA actions, but TP monitoring continues for already
open bot-tracked PAPER/LIVE positions.

## LIVE safety

Master gate:

```text
LIVE_MASTER_ENABLE=0
```

First deploy with `0`. In Telegram use:

```text
WALLET
```

Verify:

```text
SDK: READY
Wallet: expected address
Collateral: expected balance
LIVE master: OFF
```

Then set:

```text
LIVE_MASTER_ENABLE=1
```

and redeploy.

Every individual strategy still needs a second 60-second Telegram confirmation:

```text
MODE BTC B LIVE
CONFIRM LIVE BTC B
```

Examples:

```text
MODE ETH C LIVE
CONFIRM LIVE ETH C

MODE SOL E LIVE
CONFIRM LIVE SOL E
```

Switch back:

```text
MODE BTC B PAPER
MODE BTC B OFF
```

Mode crossing PAPER <-> LIVE is blocked while that strategy holds an open
position in the other execution mode.

## Multiple LIVE strategies on one token

Default:

```text
ALLOW_MULTI_LIVE_PER_TOKEN=0
```

This prevents, for example, BTC A and BTC B from both being LIVE at the same
time. The strategies can share a signal and would otherwise send independent
real orders.

If you deliberately want several A/B/C/E strategies LIVE on the same token:

```text
ALLOW_MULTI_LIVE_PER_TOKEN=1
```

then redeploy.

Different tokens can be LIVE at the same time.

## Sizes

Whole token:

```text
SIZE BTC 5 5
```

sets:

```text
A ENTRY = 5
E ENTRY = 5
B ENTRY = 5, DCA = 5
C ENTRY = 5, DCA = 5
```

Per strategy:

```text
SIZE BTC A 5
SIZE BTC B 5 5
SIZE BTC C 5 5
SIZE BTC E 5
```

Use the other token names in the same way.

Sizes cannot be changed while that strategy has an open bot-tracked position.

## Telegram controls

```text
START
STOP
MODES
SIZES
TAKE PROFIT
BALANCE
POSITIONS
STATISTICS
TRADES
WALLET
EMERGENCY STOP

TP 0.60
TP OFF
TP ENV
```

## LIVE execution

The real-order wrapper is the same protected pattern used in the earlier
PAPER/LIVE bot:

```text
fresh book check
signed LIMIT order
converted to FAK
actual accepted fill amount persisted
```

If the response after submission is ambiguous, that market/action is marked
fail-closed and the bot does not automatically submit a possible duplicate.

LIVE settlement PnL is bot-tracked from accepted fill amounts. Winning LIVE
shares that remain to market settlement are **not auto-redeemed** by this bot.

## Hosting variables

Minimum first-deploy block:

```text
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

PORT=8080
DATA_DIR=/var/data

POLYMARKET_PRIVATE_KEY=
POLYMARKET_WALLET_ADDRESS=

LIVE_MASTER_ENABLE=0
ALLOW_MULTI_LIVE_PER_TOKEN=0

TAKE_PROFIT_USDC=0.60

PAPER_START_BALANCE=500
ENTRY_ORDER_SIZE=5
DCA_ORDER_SIZE=5
```

Never put the real `POLYMARKET_PRIVATE_KEY` in GitHub. Keep it only in the
hosting Environment/Secret store.

Optional:

```text
POLYMARKET_RELAYER_API_KEY=
POLYMARKET_RELAYER_API_KEY_ADDRESS=
```

Leave them blank unless your wallet setup specifically uses them.

The complete template is in `.env.example`.

## Coolify

A `Dockerfile` is included.

Expose:

```text
8080
```

Persistent storage mount:

```text
/var/data
```

Health endpoint:

```text
/health
```

Database:

```text
/var/data/safe67_multi7_abce_paper_live_tp60.db
```

## Render

Build:

```text
pip install -r requirements.txt
```

Start:

```text
python main.py
```

Persistent disk should also be mounted at:

```text
/var/data
```

## Reports

The hourly ZIP reporter is deliberately disabled in this LIVE trading build.
Persistent SQLite still stores PAPER trades, LIVE orders, exits, signals,
consensus decisions, trajectories and results.

## Regression

Run both:

```text
python test_multi7_abce_live_tp60.py
python test_telegram_takeprofit.py
```

Expected:

```text
MULTI7 A/B/C/E PAPER/LIVE + NET TP60 regression: OK
MULTI7 Telegram TAKE-PROFIT runtime control regression: OK
```

The original regression still verifies:

- 7 tokens × A/B/C/E = 28 strategies;
- the existing A/B/C/E entry and DCA settings;
- B can still take the old deep rebound DCA;
- C rejects DCA below 0.30 and rebound momentum above +0.15;
- E still requires two other-token A/BASE confirmations;
- PAPER and fake-SDK LIVE TP still use the same NET accounting and FAK path;
- multiple LIVE strategies on the same token remain blocked by default.

The new Telegram-TP regression additionally verifies:

- `TP 0.60`, `TP 0,75`, `TAKE PROFIT 0.90`, `TP OFF`, and `TP ENV`;
- Telegram TP persists in SQLite across a simulated process restart;
- changing TP applies to an already-open PAPER position without restart;
- a partially-filled LIVE TP remains latched and continues toward flat after `TP OFF`;
- the original `$0.60` hosting default remains available through `TP ENV`.

See `strategy_parity_check.txt` for byte-exact checks of the unchanged strategy,
market, fee, PAPER execution and LIVE FAK functions.

## Additional retry regression

Run:

```text
python test_tp_sell_retry.py
```

Expected:

```text
LIVE TP deterministic FAK no-match retry regression: OK
NO MATCH -> retry while TP valid: OK
NO MATCH -> pause retry when TP invalid: OK
True ambiguous transport error remains fail-closed: OK
```
