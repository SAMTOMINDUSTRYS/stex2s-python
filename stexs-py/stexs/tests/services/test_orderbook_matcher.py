import pytest
import stexs.io.persistence as iop
from stexs.domain import model
from stexs.domain.order import (
    Order,
)
from stexs.services import orderbook, matcher

TEST_UOW = iop.order.MatcherMemoryUoW

def wrap_service_match_one(uow=None):
    return matcher.match_orderbook("STI.", uow=uow)

def _attempt_test_trade(orders, reference_price=None, expected_trades=1, uow=None):
    if not uow:
        uow = TEST_UOW()
        uow.orders.clear()

    with uow:
        for order in orders:
            uow.orders.add_book("STI.", reference_price=reference_price)
            if order.price == float("inf") or order.price == float("-inf"):
                order.price = None
            uow.orders.add(order)
        print(uow.orders.get_book("STI."))
        trades = wrap_service_match_one(uow=uow)
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
    print(trade)
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

###############################################################################

from stexs.services import orderbook
TEST_ORDER_UOW = iop.order.OrderMemoryUoW

def wrap_service_execute_trade(trade):
    return orderbook.execute_trade(trade, uow=TEST_ORDER_UOW())

def test_execute_trade():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
        Order(txid="2/2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=150, ts=1),
    ]
    with TEST_ORDER_UOW() as order_uow:
        order_uow.orders.clear()
        for order in orders:
            order_uow.orders.add(order)
        order_uow.commit()

        trade = _attempt_test_trade(orders, reference_price=1.0)
        confirmed_buys, confirmed_sells = wrap_service_execute_trade(trade)

        # Trade is closed
        assert trade.closed == True

        # Orders are closed
        for buy in confirmed_buys:
            assert order_uow.orders.get(buy.txid).closed == True
        for sell_i, sell in enumerate(confirmed_sells):
            sell = order_uow.orders.get(sell.txid)
            assert sell.closed == True
            if sell_i == len(confirmed_sells)-1:
                assert sell.volume == 100

                # Check for remainder sell
                remaining_sell = order_uow.orders.get_sell_book_for_symbol(sell.symbol)[0]
                assert remaining_sell is not None
                assert remaining_sell.txid == "2/3"
                assert remaining_sell.volume == 50

    with TEST_UOW() as uow:
        assert uow.orders.get("2/3") is not None

def test_full_split():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=150, ts=1),
    ]

    with TEST_UOW() as uow, TEST_ORDER_UOW() as order_uow:
        uow.orders.clear()
        trade = _attempt_test_trade(orders, reference_price=1.0, uow=uow)
        _assert_trade(trade, excess=50, buy_id='1', sell_ids=['2'], price=1)

        orders = [Order(txid="3", csid="1", side="BUY", symbol="STI.", price=1.0, volume=50, ts=2)]
        trade = _attempt_test_trade(orders, reference_price=1.0, uow=uow)
        _assert_trade(trade, excess=0, buy_id='3', sell_ids=['2/1'], price=1)

