import os
import time
import asyncio
import tempfile
import importlib.util
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

os.environ["DATA_DIR"] = tempfile.mkdtemp(prefix="tp_sell_retry_")
os.environ["TELEGRAM_BOT_TOKEN"] = ""
os.environ["TELEGRAM_CHAT_ID"] = ""
os.environ["SYMBOLS"] = "BTC,XRP,BNB,SOL,ETH,DOGE,HYPE"
os.environ["PAPER_START_BALANCE"] = "500"
os.environ["TAKE_PROFIT_USDC"] = "0.60"
os.environ["LIVE_MASTER_ENABLE"] = "0"
os.environ["ALLOW_MULTI_LIVE_PER_TOKEN"] = "0"
os.environ["POLYMARKET_PRIVATE_KEY"] = ""

here = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("bot", here / "main.py")
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()

assert bot.TAKE_PROFIT_USDC == 0.60
assert bot.take_profit_usdc() == 0.60

class RequestRejectedError(Exception):
    pass

class TransportError(Exception):
    pass

NO_MATCH = (
    "no orders found to match with FAK order. "
    "FAK orders are partially filled or killed if no match is found."
)

assert bot.is_definite_fak_no_match_error(RequestRejectedError(NO_MATCH))
assert not bot.is_definite_fak_no_match_error(TransportError("503 No exit node"))

@dataclass(frozen=True)
class FakeSigned:
    token_id: str
    price: str
    size: str
    side: str
    post_only: bool = False
    order_type: str = "GTC"

@dataclass
class FakeResponse:
    ok: bool
    making_amount: Decimal
    taking_amount: Decimal
    status: str = "matched"
    order_id: str = "fake-order"
    trade_ids: tuple = ("fake-trade",)
    code: str = ""
    message: str = ""

class RetryClient:
    def __init__(self, sell_failures=1, ambiguous=False):
        self.sell_calls = 0
        self.sell_failures = sell_failures
        self.ambiguous = ambiguous

    async def create_limit_order(self, **kwargs):
        return FakeSigned(
            token_id=str(kwargs["token_id"]),
            price=str(kwargs["price"]),
            size=str(kwargs["size"]),
            side=str(kwargs["side"]).upper(),
            post_only=bool(kwargs.get("post_only", False)),
        )

    async def post_order(self, order):
        size = Decimal(order.size)
        price = Decimal(order.price)
        assert order.order_type == "FAK"
        if order.side == "BUY":
            return FakeResponse(True, size * price, size, order_id="buy")

        self.sell_calls += 1
        if self.sell_calls <= self.sell_failures:
            if self.ambiguous:
                raise TransportError("503 No exit node")
            raise RequestRejectedError(NO_MATCH)
        return FakeResponse(
            True, size, size * price,
            order_id=f"sell-{self.sell_calls}",
            trade_ids=(f"sell-trade-{self.sell_calls}",),
        )

slot = (int(time.time()) // 300) * 300
counter = 0

def make_market(symbol, tag):
    global counter
    counter += 1
    m = {
        "condition_id": f"cid-{symbol}-{tag}-{counter}",
        "symbol": symbol,
        "question": f"{symbol} Up or Down test",
        "slug": f"{bot.ASSET_CONFIG[symbol]['prefix']}-{slot}",
        "start_ts": slot,
        "end_ts": slot + 300,
        "up_asset": f"{symbol}-UP-{tag}-{counter}",
        "down_asset": f"{symbol}-DN-{tag}-{counter}",
    }
    bot.markets[m["condition_id"]] = m
    bot.persist_market(m)
    return m

def set_book(asset, bid, ask, size=100.0):
    bot.books[asset] = {
        "bids": {float(bid): float(size)},
        "asks": {float(ask): float(size)},
        "received_ms": bot.now_ms(),
        "source": "test",
    }

def seed_up(m, ask=.68, mom=.07):
    ms = bot.now_ms()
    ref = ask - mom
    mid = ref + mom / 2
    set_book(m["up_asset"], ask-.01, ask)
    set_book(m["down_asset"], max(.01, 1-ask-.01), max(.01, 1-ask))
    h = bot.price_history[m["condition_id"]][m["up_asset"]]
    h.clear()
    h.extend([(ms-6000, ref), (ms-3000, mid), (ms, ask)])
    hd = bot.price_history[m["condition_id"]][m["down_asset"]]
    hd.clear()
    hd.extend([(ms-6000, .45), (ms-3000, .40), (ms, .35)])

def open_live_a(symbol, tag, client):
    bot.live_client = client
    bot.live_client_ready = True
    bot.LIVE_MASTER_ENABLE = True
    bot.sdk_post_order_with_allowance_recovery = None
    m = make_market(symbol, tag)
    a = bot.STRATEGIES_BY_SYMBOL[symbol][0]
    bot.state_set(f"mode:{a['name']}", "LIVE")
    seed_up(m, .68, .07)
    asyncio.run(bot.evaluate_variant(m, a, 30.0))
    pos = bot.position_totals(m["condition_id"], a["name"])
    assert pos["execution_mode"] == "LIVE"
    assert pos["remaining"] > 0
    return m, a

# ------------------------------------------------------------------
# 1) Exact screenshot case: SELL TP FAK gets deterministic NO MATCH.
#    It must NOT become AMBIGUOUS, and next cycle retries if TP is still true.
# ------------------------------------------------------------------
client = RetryClient(sell_failures=1, ambiguous=False)
m, a = open_live_a("SOL", "retry-success", client)
set_book(m["up_asset"], .83, .84, 100)
mark = bot.projected_full_exit(m["condition_id"], a["name"])
assert mark and mark["total_pnl"] >= .60

first = asyncio.run(bot.maybe_take_profit(m, a, 60.0))
assert first is False
assert client.sell_calls == 1
assert not bot.live_action_ambiguous(m["condition_id"], a["name"], "SELL", "TAKE_PROFIT")
with bot.db() as conn:
    row = conn.execute(
        "SELECT * FROM live_orders WHERE condition_id=? AND variant=? AND action='SELL' ORDER BY id DESC LIMIT 1",
        (m["condition_id"], a["name"]),
    ).fetchone()
assert row["status"] == "REJECTED_NO_MATCH"
assert float(row["filled_shares"]) == 0.0

# Conditions still valid -> retry on next strategy cycle, then fill.
set_book(m["up_asset"], .83, .84, 100)
second = asyncio.run(bot.maybe_take_profit(m, a, 63.0))
assert second is True
assert client.sell_calls == 2
assert bot.position_totals(m["condition_id"], a["name"])["remaining"] <= 1e-9

# ------------------------------------------------------------------
# 2) NO MATCH but TP ceases to be valid: do NOT keep selling below target.
# ------------------------------------------------------------------
client2 = RetryClient(sell_failures=99, ambiguous=False)
m2, a2 = open_live_a("ETH", "retry-condition", client2)
set_book(m2["up_asset"], .83, .84, 100)
assert asyncio.run(bot.maybe_take_profit(m2, a2, 60.0)) is False
assert client2.sell_calls == 1

# Price falls; current executable NET PnL is below +0.60 -> no retry.
set_book(m2["up_asset"], .75, .76, 100)
mark2 = bot.projected_full_exit(m2["condition_id"], a2["name"])
assert mark2 and mark2["total_pnl"] < .60
assert asyncio.run(bot.maybe_take_profit(m2, a2, 63.0)) is False
assert client2.sell_calls == 1

# Price returns above target -> retry becomes eligible again.
set_book(m2["up_asset"], .83, .84, 100)
assert asyncio.run(bot.maybe_take_profit(m2, a2, 66.0)) is False
assert client2.sell_calls == 2

# ------------------------------------------------------------------
# 3) Real ambiguity (e.g. transport 503) remains fail-closed exactly as before.
# ------------------------------------------------------------------
client3 = RetryClient(sell_failures=99, ambiguous=True)
m3, a3 = open_live_a("XRP", "true-ambiguous", client3)
set_book(m3["up_asset"], .83, .84, 100)
assert asyncio.run(bot.maybe_take_profit(m3, a3, 60.0)) is False
assert client3.sell_calls == 1
assert bot.live_action_ambiguous(m3["condition_id"], a3["name"], "SELL", "TAKE_PROFIT")

# Still above target, but fail-closed prevents a possible duplicate after true ambiguity.
set_book(m3["up_asset"], .84, .85, 100)
assert asyncio.run(bot.maybe_take_profit(m3, a3, 63.0)) is False
assert client3.sell_calls == 1

print("LIVE TP deterministic FAK no-match retry regression: OK")
print("NO MATCH -> retry while TP valid: OK")
print("NO MATCH -> pause retry when TP invalid: OK")
print("True ambiguous transport error remains fail-closed: OK")
