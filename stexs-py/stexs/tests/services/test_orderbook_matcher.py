import pytest
import stexs.io.persistence as iop
from stexs.domain import model
from stexs.domain.order import (
    Order,
)
from stexs.services import orderbook

def wrap_service_match_one(buys, sells, bid=None, ask=None, ref=None):
    return orderbook.match_one(buys, sells, highest_bid=bid, lowest_ask=ask, reference_price=ref)

# T7 11.2.2.1 1
def test_order_meets_book_with_market_order_on_other_side():
    pass

# T7 12.2.2.1 2
def test_order_meets_book_with_limit_order_on_other_side():
    buys = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=200, volume=6000, ts=901),
    ]
    sells = [
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=200, volume=6000, ts=902),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 1
    assert trades[0].excess == 0
    assert trades[0].buy_txid == "1"
    assert trades[0].sell_txids == ["2"]
    assert trades[0].closed == False
    assert trades[0].avg_price == 200.0

# T7 12.2.2.1 3
def test_market_order_meets_order_book_with_limit_order_on_other_side():
    pass

# T7 12.2.2.1 4
def test_market_order_meets_order_book_with_market_and_limit_order_on_buy_side_under_ref():
    pass

# T7 12.2.2.1 5
def test_market_order_meets_order_book_with_market_and_limit_order_on_buy_side_over_ref():
    pass

# T7 12.2.2.1 6
def test_market_order_meets_order_book_with_market_and_limit_order_on_sell_side_under_ref():
    pass

# T7 12.2.2.1 7
def test_market_order_meets_order_book_with_market_and_limit_order_on_sell_side_over_ref():
    pass

# T7 12.2.2.1 8
def test_market_order_meets_empty_order_book():
    pass

# T7 12.2.2.1 9
def test_limit_order_meets_order_book_with_market_order_on_other_side_ask_under_ref():
    pass

# T7 12.2.2.1 10
def test_limit_order_meets_order_book_with_market_order_on_other_side_ask_over_ref():
    pass

# T7 12.2.2.1 11
def test_limit_order_meets_order_book_with_market_order_on_other_side_bid_over_ref():
    pass

# T7 12.2.2.1 12
def test_limit_order_meets_order_book_with_market_order_on_other_side_bid_under_ref():
    pass

# T7 12.2.2.1 13
def test_t7_13_limit_ask_meets_limit_bid():
    buys = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=199, volume=6000, ts=933),
    ]
    sells = [
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=198, volume=6000, ts=934),
    ]
    trades = wrap_service_match_one(buys, sells, bid=199, ask=198, ref=None)
    assert len(trades) == 1
    assert trades[0].excess == 0
    assert trades[0].buy_txid == "1"
    assert trades[0].sell_txids == ["2"]
    assert trades[0].closed == False
    assert trades[0].avg_price == 199.0

# T7 12.2.2.1 14
def test_t7_14_limit_bid_meets_limit_ask():
    buys = [
        Order(txid="2", csid="1", side="BUY", symbol="STI.", price=200, volume=6000, ts=934),
    ]
    sells = [
        Order(txid="1", csid="1", side="SELL", symbol="STI.", price=199, volume=6000, ts=933),
    ]
    trades = wrap_service_match_one(buys, sells, bid=200, ask=199, ref=None)
    assert len(trades) == 1
    assert trades[0].excess == 0
    assert trades[0].buy_txid == "2"
    assert trades[0].sell_txids == ["1"]
    assert trades[0].closed == False
    assert trades[0].avg_price == 199.0

# T7 12.2.2.1 15
def test_t7_15_book_best_bid_under_lowest_ask():
    buys = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=199, volume=6000, ts=933),
    ]
    sells = [
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=200, volume=6000, ts=1001),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 0

# T7 12.2.2.1 16
def test_book_market_bid_matched_to_ask_under_ref():
    pass

# T7 12.2.2.1 17
def test_book_market_bid_matched_to_ask_under_ref_at_highest_nonmarket_bid():
    pass

# T7 12.2.2.1 18
def test_book_market_bid_matched_to_ask_over_ref_at_highest_nonmarket_bid():
    pass

# T7 12.2.2.1 19
def test_bid_limit_meets_market_and_limit_ask_exec_highest_bid():
    pass

# T7 12.2.2.1 20
def test_bid_limit_meets_market_and_limit_ask_exec_reference():
    pass

# T7 12.2.2.1 21
def test_bid_limit_meets_market_and_limit_ask_exec_lowest_ask():
    pass

# T7 12.2.2.1 22
def test_t7_22_bid_limit_meets_empty_book():
    buys = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=200, volume=6000, ts=1001),
    ]
    sells = [
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 0
