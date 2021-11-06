import pytest
import stexs.io.persistence as iop
from stexs.domain import model
from stexs.domain.order import (
    Order,
)
from stexs.services import orderbook

def wrap_service_match_one(buys, sells, bid=None, ask=None, ref=None):
    return orderbook.match_one(buys, sells, reference_price=ref)

def _attempt_test_trade(orders, reference_price=None, expected_trades=1):
    buys = []
    sells = []

    buy_prices = []
    sell_prices = []

    for order in orders:
        if order.side == "BUY":
            buys.append(order)
            if order.price != float("inf"):
                buy_prices.append(order.price)
        elif order.side == "SELL":
            sells.append(order)
            if order.price != float("-inf"):
                sell_prices.append(order.price)

    bid = None
    if len(buy_prices) > 0:
        bid = max(buy_prices)

    ask = None
    if len(sell_prices) > 0:
        ask = min(sell_prices)

    trades = wrap_service_match_one(buys, sells, ref=reference_price, ask=ask, bid=bid)
    assert len(trades) == expected_trades
    if expected_trades == 1:
        return trades[0]

def _assert_trade(trade, excess, buy_id, sell_ids, price):
    assert trade.excess == excess
    assert trade.buy_txid == buy_id
    assert trade.sell_txids == sell_ids
    assert trade.closed == False
    assert trade.avg_price == price

# T7 11.2.2.1 1
def test_market_order_meets_book_with_market_order_on_other_side():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=float("inf"), volume=6000, ts=901),
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=float("-inf"), volume=6000, ts=902),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='1', sell_ids=['2'], price=200.0)

# T7 12.2.2.1 2
def test_order_meets_book_with_limit_order_on_other_side():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=200, volume=6000, ts=901),
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=200, volume=6000, ts=902),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='1', sell_ids=['2'], price=200.0)

# T7 12.2.2.1 3
def test_market_order_meets_order_book_with_limit_order_on_other_side():
    orders = [
        Order(txid="1", csid="1", side="SELL", symbol="STI.", price=200, volume=6000, ts=901),
        Order(txid="2", csid="1", side="BUY", symbol="STI.", price=float("inf"), volume=6000, ts=902),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='2', sell_ids=['1'], price=200.0)

# T7 12.2.2.1 4
def test_market_order_meets_order_book_with_market_and_limit_order_on_buy_side_under_ref():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=float("inf"), volume=6000, ts=901),
        Order(txid="2", csid="1", side="BUY", symbol="STI.", price=195, volume=1000, ts=902),
        Order(txid="3", csid="1", side="SELL", symbol="STI.", price=float("-inf"), volume=6000, ts=903),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='1', sell_ids=['3'], price=200.0)

# T7 12.2.2.1 5
def test_market_order_meets_order_book_with_market_and_limit_order_on_buy_side_over_ref():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=float("inf"), volume=6000, ts=901),
        Order(txid="2", csid="1", side="BUY", symbol="STI.", price=202, volume=1000, ts=902),
        Order(txid="3", csid="1", side="SELL", symbol="STI.", price=float("-inf"), volume=6000, ts=903),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='1', sell_ids=['3'], price=202.0)

# T7 12.2.2.1 6
def test_market_order_meets_order_book_with_market_and_limit_order_on_sell_side_under_ref():
    orders = [
        Order(txid="1", csid="1", side="SELL", symbol="STI.", price=float("-inf"), volume=6000, ts=901),
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=202, volume=1000, ts=902),
        Order(txid="3", csid="1", side="BUY", symbol="STI.", price=float("inf"), volume=6000, ts=903),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='3', sell_ids=['1'], price=200.0)

# T7 12.2.2.1 7
def test_market_order_meets_order_book_with_market_and_limit_order_on_sell_side_over_ref():
    orders = [
        Order(txid="1", csid="1", side="SELL", symbol="STI.", price=float("-inf"), volume=6000, ts=901),
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=202, volume=1000, ts=902),
        Order(txid="3", csid="1", side="BUY", symbol="STI.", price=float("inf"), volume=6000, ts=903),
    ]
    trade = _attempt_test_trade(orders, reference_price=202)
    _assert_trade(trade, excess=0, buy_id='3', sell_ids=['1'], price=202.0)

# T7 12.2.2.1 8
def test_market_order_meets_empty_order_book():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=float("inf"), volume=6000, ts=1001),
    ]
    trade = _attempt_test_trade(orders, reference_price=202, expected_trades=0)

# T7 12.2.2.1 9
def test_limit_order_meets_order_book_with_market_order_on_other_side_ask_under_ref():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=float("inf"), volume=6000, ts=901),
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=195, volume=6000, ts=902),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='1', sell_ids=['2'], price=200)

# T7 12.2.2.1 10
def test_limit_order_meets_order_book_with_market_order_on_other_side_ask_over_ref():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=float("inf"), volume=6000, ts=901),
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=203, volume=6000, ts=902),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='1', sell_ids=['2'], price=203)

# T7 12.2.2.1 11
def test_limit_order_meets_order_book_with_market_order_on_other_side_bid_over_ref():
    orders = [
        Order(txid="1", csid="1", side="SELL", symbol="STI.", price=float("-inf"), volume=6000, ts=901),
        Order(txid="2", csid="1", side="BUY", symbol="STI.", price=203, volume=6000, ts=902),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='2', sell_ids=['1'], price=200)

# T7 12.2.2.1 12
def test_limit_order_meets_order_book_with_market_order_on_other_side_bid_under_ref():
    orders = [
        Order(txid="1", csid="1", side="SELL", symbol="STI.", price=float("-inf"), volume=6000, ts=901),
        Order(txid="2", csid="1", side="BUY", symbol="STI.", price=199, volume=6000, ts=902),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='2', sell_ids=['1'], price=199)

# T7 12.2.2.1 13
def test_t7_13_limit_ask_meets_limit_bid():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=199, volume=6000, ts=933),
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=198, volume=6000, ts=934),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='1', sell_ids=['2'], price=199)

# T7 12.2.2.1 14
def test_t7_14_limit_bid_meets_limit_ask():
    orders = [
        Order(txid="1", csid="1", side="SELL", symbol="STI.", price=199, volume=6000, ts=933),
        Order(txid="2", csid="1", side="BUY", symbol="STI.", price=200, volume=6000, ts=934),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='2', sell_ids=['1'], price=199)

# T7 12.2.2.1 15
def test_t7_15_book_best_bid_under_lowest_ask():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=199, volume=6000, ts=933),
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=200, volume=6000, ts=1001),
    ]
    trade = _attempt_test_trade(orders, reference_price=200, expected_trades=0)

# T7 12.2.2.1 16
def test_book_market_bid_matched_to_ask_under_ref():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=float("inf"), volume=6000, ts=901),
        Order(txid="2", csid="1", side="BUY", symbol="STI.", price=196, volume=1000, ts=902),
        Order(txid="3", csid="1", side="SELL", symbol="STI.", price=195, volume=6000, ts=903),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='1', sell_ids=['3'], price=200)

# T7 12.2.2.1 17
def test_book_market_bid_matched_to_ask_under_ref_at_highest_nonmarket_bid():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=float("inf"), volume=6000, ts=901),
        Order(txid="2", csid="1", side="BUY", symbol="STI.", price=202, volume=1000, ts=902),
        Order(txid="3", csid="1", side="SELL", symbol="STI.", price=199, volume=6000, ts=903),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='1', sell_ids=['3'], price=202)

# T7 12.2.2.1 18
def test_book_market_bid_matched_to_ask_over_ref_at_highest_nonmarket_bid():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=float("inf"), volume=6000, ts=901),
        Order(txid="2", csid="1", side="BUY", symbol="STI.", price=202, volume=1000, ts=902),
        Order(txid="3", csid="1", side="SELL", symbol="STI.", price=203, volume=6000, ts=903),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='1', sell_ids=['3'], price=203)

# T7 12.2.2.1 19
def test_bid_limit_meets_market_and_limit_ask_exec_highest_bid():
    orders = [
        Order(txid="1", csid="1", side="SELL", symbol="STI.", price=float("-inf"), volume=6000, ts=901),
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=202, volume=1000, ts=902),
        Order(txid="3", csid="1", side="BUY", symbol="STI.", price=203, volume=6000, ts=903),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='3', sell_ids=['1'], price=200)

# T7 12.2.2.1 20
def test_bid_limit_meets_market_and_limit_ask_exec_reference():
    orders = [
        Order(txid="1", csid="1", side="SELL", symbol="STI.", price=float("-inf"), volume=6000, ts=901),
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=202, volume=1000, ts=902),
        Order(txid="3", csid="1", side="BUY", symbol="STI.", price=200, volume=6000, ts=903),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='3', sell_ids=['1'], price=200)

# T7 12.2.2.1 21
def test_bid_limit_meets_market_and_limit_ask_exec_lowest_ask():
    orders = [
        Order(txid="1", csid="1", side="SELL", symbol="STI.", price=float("-inf"), volume=6000, ts=901),
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=199, volume=1000, ts=902),
        Order(txid="3", csid="1", side="BUY", symbol="STI.", price=203, volume=6000, ts=903),
    ]
    trade = _attempt_test_trade(orders, reference_price=200)
    _assert_trade(trade, excess=0, buy_id='3', sell_ids=['1'], price=199)

# T7 12.2.2.1 22
def test_t7_22_bid_limit_meets_empty_book():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=200, volume=6000, ts=1001),
    ]
    trade = _attempt_test_trade(orders, reference_price=200, expected_trades=0)

