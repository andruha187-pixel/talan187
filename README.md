# MULTI7 PRE-JUMP — PAPER + LIVE

Отдельный торговый бот для BTC/XRP/BNB/SOL/ETH/DOGE/HYPE на логике PRE_JUMP.
Исследовательский 4-way PAPER-бот не нужен этому сервису и может работать отдельно.

## Стратегия по умолчанию

- Polymarket 5m Up/Down.
- Вход один раз на рынок.
- PM ask: `0.52..0.66`.
- Внешний directional score: `>= 0.40` по умолчанию.
- Минимум 2 свежих same-side venue votes.
- PM ask momentum за ~1 сек: `-0.01..+0.05`.
- Время входа: `1..160` секунд после старта 5m рынка.
- ENTRY: 5 shares по умолчанию.
- DCA: нет.
- Stop-loss: нет.
- Take-profit: `+$0.60 NET` на всю оставшуюся позицию по умолчанию.

Score можно менять из Telegram в реальном времени. Жёсткий минимум в коде — `0.40`; ниже бот не разрешит поставить.

## Telegram

Клавиатура:

```text
START                 STOP
MODES                 SIZES
BALANCE               POSITIONS
STATISTICS            TRADES
TAKE PROFIT            WALLET
-SCORE      SCORE      +SCORE
EMERGENCY STOP
```

### SCORE

```text
SCORE 0.42
SCORE +0.01
SCORE -0.01
SCORE ENV
```

Кнопки `+ SCORE` / `- SCORE` меняют порог на 0.01. `SCORE ENV` возвращает значение `PREJUMP_SCORE` из Environment.
Изменение применяется только к будущим входам; открытая позиция не меняется.

### Размер

```text
SIZE BTC 5
SIZE ETH 3
SIZE ALL 5
```

Размер нельзя менять для токена, пока у него есть открытая bot-tracked позиция.

### Режимы PAPER / LIVE / OFF

```text
MODE BTC PAPER
MODE BTC OFF
MODE BTC LIVE
CONFIRM LIVE BTC
```

Для LIVE требуется второе подтверждение в течение 60 секунд.

## LIVE safety

Реальный BUY/SELL невозможен, пока одновременно не выполнены все условия:

1. `LIVE_MASTER_ENABLE=1` в Environment;
2. wallet SDK и credentials готовы;
3. токен переведён через `MODE <TOKEN> LIVE`;
4. затем отправлено `CONFIRM LIVE <TOKEN>` в течение 60 секунд;
5. глобальный `START` включён;
6. текущий PRE_JUMP сигнал проходит все фильтры.

LIVE execution сохраняет защищённый wrapper предыдущего торгового бота:

- fresh Polymarket book перед execution;
- signed LIMIT order, преобразованный в FAK;
- FAK не оставляет намеренно незаполненный остаток висеть в стакане;
- при неоднозначной ошибке submission используется fail-closed: повтор возможного дублирующего реального ордера автоматически не отправляется;
- TP продолжает контролироваться после STOP;
- EMERGENCY STOP блокирует новые входы и очищает ожидающие LIVE confirmations, но не делает аварийный market-sell открытых позиций.

Winning LIVE shares, оставшиеся до settlement, бот автоматически не redeem-ит.

## TAKE PROFIT

```text
TP 0.60
TP 0.90
TP OFF
TP ENV
```

TP — NET цель для всей оставшейся позиции после учёта entry fee и projected exit fee.

## Первый запуск в Coolify

1. Загрузить файлы репозитория.
2. Persistent storage: `/var/data`.
3. Port: `8080`; health endpoint: `/health`.
4. Добавить Telegram variables и wallet secrets только через Environment/Secrets.
5. Первый deploy оставить с `LIVE_MASTER_ENABLE=0`.
6. Нажать `WALLET` и проверить `SDK: READY` и collateral.
7. Для реальной торговли поставить `LIVE_MASTER_ENABLE=1` и redeploy.
8. Перед `START` перевести нужные токены в LIVE: `MODE BTC LIVE` -> `CONFIRM LIVE BTC`.

По умолчанию все токены после новой базы находятся в PAPER и глобальные ENTRY выключены.

## Важное про ключ

`POLYMARKET_PRIVATE_KEY` не класть в GitHub, Telegram, скриншоты или сообщения. Только Environment/Secret store на сервере.

## Database

```text
/var/data/prejump_multi7_paper_live.db
```

В ней сохраняются PAPER/LIVE fills, сигналы, score threshold на момент сигнала, позиции, TP exits и settlement statistics.

## Tests

```text
python test_prejump_live.py
```

Ожидается:

```text
PRE-JUMP PAPER/LIVE regression: OK
```

## LIVE FAK NO_MATCH handling (v20.1)

PRE-JUMP can move faster than the Polymarket book snapshot. A deterministic
`no orders found to match with FAK order` response is now stored as
`REJECTED_NO_MATCH`, **not** `AMBIGUOUS`. It represents a zero-fill killed FAK.

For LIVE ENTRY the bot now:

1. forces a fresh REST book immediately before submission;
2. uses the accepted signal ask plus `LIVE_ENTRY_MAX_SLIPPAGE` as a hard BUY cap;
3. on deterministic NO_MATCH only, waits `LIVE_ENTRY_RETRY_DELAY_MS`;
4. rechecks PRE-JUMP direction/score/venue votes and performs at most
   `LIVE_ENTRY_NO_MATCH_RETRIES` additional attempt(s);
5. never retries timeouts, transport failures or unknown submission errors. Those
   remain `AMBIGUOUS` and fail-closed.

Defaults:

```text
LIVE_ENTRY_MAX_SLIPPAGE=0.01
LIVE_ENTRY_NO_MATCH_RETRIES=1
LIVE_ENTRY_RETRY_DELAY_MS=150
LIVE_ENTRY_FORCE_REST_BOOK=1
```

The slippage cap is also bounded by `PREJUMP_PRICE_MAX`, so an accepted signal
at 0.66 cannot be chased above 0.66.



## v20.2 tick-safe LIVE price

LIVE limit prices are aligned to the Polymarket outcome token tick before signing.
The bot preserves `tick_size` from fresh order-book snapshots when available and
uses `LIVE_PRICE_TICK_FALLBACK=0.01` otherwise. BUY limits are rounded DOWN to
the tick so normalization never exceeds the configured slippage cap.

Errors raised while building/signing the order, before any `post_order` call, are
recorded as `REJECTED_LOCAL` and are not treated as ambiguous submissions.
Unknown failures after submission remain fail-closed.


## v20.3 LIVE TP balance propagation

A freshly matched LIVE BUY can briefly be visible in bot/CLOB execution history before
the acquired outcome-token balance is available to a subsequent SELL. To avoid a
false fail-closed TP in that short window:

- the first LIVE TP SELL is delayed by `LIVE_TP_MIN_HOLD_MS` after the newest BUY;
- explicit `not enough balance / allowance` TAKE_PROFIT rejections are stored as
  `REJECTED_BALANCE_ALLOWANCE`, not `AMBIGUOUS`;
- no unknown fill is assumed for that deterministic rejection;
- the tracked position remains open and TP retries on later cycles with
  `LIVE_TP_BALANCE_RETRY_DELAY_MS` backoff;
- unknown timeouts/transport failures after submission still remain AMBIGUOUS and
  fail-closed.

Defaults:

```text
LIVE_TP_MIN_HOLD_MS=2000
LIVE_TP_BALANCE_RETRY_DELAY_MS=1000
```

At startup v20.3 also repairs old v20.2 `AMBIGUOUS` TP rows whose stored error
explicitly says `not enough balance`, so an already affected open position can
resume TP attempts after redeploy.
