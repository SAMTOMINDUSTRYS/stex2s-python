import pytest
import stexs.io.persistence as iop
from stexs.domain import model
from stexs.domain.order import (
    Order,
)
from stexs.services import orderbook

# TODO Need to turn this into some sort of fixture that nukes the memory between tests
TEST_ORDER_UOW = iop.order.OrderMemoryUoW

def wrap_service_add_orders(orders):
    with TEST_ORDER_UOW() as uow:
        uow.orders.clear()
    for order in orders:
        orderbook.add_order(order, uow=TEST_ORDER_UOW())

def wrap_service_close_txids(txids):
    orderbook.close_txids(txids, uow=TEST_ORDER_UOW())

def wrap_service_match_one(buys, sells):
    return orderbook.match_one(buys, sells)

def wrap_service_execute_trade(trade):
    return orderbook.execute_trade(trade, uow=TEST_ORDER_UOW())

# Test add_order internal service (not the underlying persistence, or external JSON service)
def test_add_order():
    expected_order = Order(
        txid="1",
        csid="1",
        side="BUY",
        symbol="STI.",
        price=1.0,
        volume=100,
        ts=1,
    )
    wrap_service_add_orders([expected_order])

    with TEST_ORDER_UOW() as uow:
        actual_order = uow.orders.get(expected_order.txid)

    assert actual_order is not None
    assert expected_order == actual_order


def test_close_order():
    test_add_order()
    wrap_service_close_txids(["1"])

    with TEST_ORDER_UOW() as uow:
        actual_order = uow.orders.get("1")
    assert actual_order.closed is True


# TODO This needs to be done for each UoW as the sorting is done in the Repo...
def test_buybook_ordered_by_maxprice_mintime():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
        Order(txid="2", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=2),
        Order(txid="3", csid="1", side="BUY", symbol="STI.", price=2.0, volume=100, ts=3),
    ]
    wrap_service_add_orders(orders)

    with TEST_ORDER_UOW() as uow:
        buybook = uow.orders.get_buy_book_for_symbol("STI.")

    assert buybook == [orders[2], orders[0], orders[1]]


# TODO This needs to be done for each UoW as the sorting is done in the Repo...
def test_sellbook_ordered_by_minprice_mintime():
    orders = [
        Order(txid="1", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1),
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=2),
        Order(txid="3", csid="1", side="SELL", symbol="STI.", price=0.5, volume=100, ts=3),
    ]
    wrap_service_add_orders(orders)

    with TEST_ORDER_UOW() as uow:
        sellbook = uow.orders.get_sell_book_for_symbol("STI.")

    assert sellbook == [orders[2], orders[0], orders[1]]


def test_match_order_with_empty_buy_has_no_trades():
    orders = [
        Order(txid="1", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    trades = wrap_service_match_one([], orders)
    assert len(trades) == 0


def test_match_order_with_empty_sell_has_no_trades():
    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    trades = wrap_service_match_one(orders, [])
    assert len(trades) == 0


def test_match_buy_with_perfect_sell():
    buys = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    sells = [
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 1
    assert trades[0].excess == 0
    assert trades[0].buy_txid == "1"
    assert trades[0].sell_txids == ["2"]
    assert trades[0].closed == False


def test_match_buy_with_excess_sell():
    buys = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    sells = [
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=1000, ts=1),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 1
    assert trades[0].excess == 900
    assert trades[0].buy_txid == "1"
    assert trades[0].sell_txids == ["2"]


def test_match_buy_with_multiple_sell():
    buys = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=1000, ts=1),
    ]
    sells = [
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=0.5, volume=500, ts=1),
        Order(txid="3", csid="1", side="SELL", symbol="STI.", price=1.0, volume=250, ts=1),
        Order(txid="4", csid="1", side="SELL", symbol="STI.", price=1.0, volume=300, ts=1),
        Order(txid="5", csid="1", side="SELL", symbol="STI.", price=1.0, volume=1000, ts=1),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 1
    assert trades[0].excess == 50
    assert trades[0].buy_txid == "1"
    assert trades[0].sell_txids == ["2", "3", "4"]
    assert trades[0].avg_price == 0.75
    assert trades[0].total_price == (0.5*500) + (1*500)


def test_match_buy_with_overpriced_sell():
    buys = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    sells = [
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=2.0, volume=500, ts=1),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 0


def test_match_buy_with_insufficient_sell():
    buys = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    sells = [
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=50, ts=1),
        Order(txid="3", csid="1", side="SELL", symbol="STI.", price=1.0, volume=10, ts=1),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 0


def test_match_aborts_on_closed_buy():
    buys = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1, closed=True),
    ]
    sells = [
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 0


def test_match_skips_on_closed_sell():
    buys = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    sells = [
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1, closed=True),
        Order(txid="3", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 1
    assert trades[0].sell_txids == ["3"]


# Test the match_orderbook process returns a Trade matching expectations
# Not necessarily testing the matcher itself
def test_match_orderbook():

    orders = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=0.5, volume=200, ts=1),
        Order(txid="4", csid="1", side="BUY", symbol="STI.", price=1.0, volume=200, ts=1),
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1),
        Order(txid="3", csid="1", side="SELL", symbol="STI.", price=0.5, volume=100, ts=1),
    ]
    expected_trade = model.Trade.propose_trade(orders[1], [orders[3], orders[2]], excess=0)

    wrap_service_add_orders(orders)
    actual_trade = orderbook.match_orderbook("STI.", uow=TEST_ORDER_UOW())[0]

    actual_trade.tid = expected_trade.tid # Hack to ignore auto-ID name
    assert expected_trade == actual_trade


# test_order tests the function of split_sell, here we just want to check the orderbook wrapper works
def test_split_sell():
    order = Order(txid="1", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1)
    order, remainder = orderbook.split_sell(order, 25)

    assert order.txid == "1"
    assert order.volume == 75

    assert remainder.txid == "1/1"
    assert remainder.volume == 25
    assert not remainder.closed


def test_execute_trade():
    buys = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    sells = [
        Order(txid="2/2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=150, ts=1),
    ]
    wrap_service_add_orders(buys + sells)
    trade = wrap_service_match_one(buys, sells)[0]

    confirmed_buys, confirmed_sells = wrap_service_execute_trade(trade)

    # Trade is closed
    assert trade.closed == True

    # Orders are closed
    with TEST_ORDER_UOW() as uow:
        for buy in confirmed_buys:
            assert uow.orders.get(buy.txid).closed == True
        for sell_i, sell in enumerate(confirmed_sells):
            sell = uow.orders.get(sell.txid)
            assert sell.closed == True
            if sell_i == len(confirmed_sells)-1:
                assert sell.volume == 100

                # Check for remainder sell
                remaining_sell = uow.orders.get_sell_book_for_symbol(sell.symbol)[0]
                assert remaining_sell is not None
                assert remaining_sell.txid == "2/3"
                assert remaining_sell.volume == 50


def test_summarise_empty_books():
    buy_book = []
    sell_book = []
    buy = sell = None

    expected_summary = {
        "dbuys": 0,
        "dsells": 0,
        "nbuys": 0,
        "nsells": 0,
        "vbuys": 0,
        "vsells": 0,
        "buy": None,
        "sell": None
    }
    actual_summary = orderbook.summarise_books(buy_book, sell_book, buy=buy, sell=sell)
    assert expected_summary == actual_summary

    with TEST_ORDER_UOW() as uow:
        uow.orders.clear()
    actual_summary = orderbook.summarise_books_for_symbol("STI.", uow=TEST_ORDER_UOW())
    assert expected_summary == actual_summary


def test_summarise_closed_books():
    buy_book = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1, closed=True),
    ]
    sell_book = [
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=2.0, volume=100, ts=1, closed=True),
    ]
    buy = 1
    sell = 2

    expected_summary = {
        "dbuys": 0,
        "dsells": 0,
        "nbuys": 0,
        "nsells": 0,
        "vbuys": 0,
        "vsells": 0,
        "buy": 1,
        "sell": 2
    }
    actual_summary = orderbook.summarise_books(buy_book, sell_book, buy=buy, sell=sell)
    assert expected_summary == actual_summary


def test_summarise_some_books():
    buy_book = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
        Order(txid="6", csid="1", side="BUY", symbol="STI.", price=1.0, volume=300, ts=1),
        Order(txid="4", csid="1", side="BUY", symbol="STI.", price=0.9, volume=100, ts=1),
        Order(txid="5", csid="1", side="BUY", symbol="STI.", price=0.9, volume=100, ts=1),
    ]
    sell_book = [
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=2.0, volume=100, ts=1),
        Order(txid="3", csid="1", side="SELL", symbol="STI.", price=2.0, volume=100, ts=1),
    ]
    buy = 1
    sell = 2

    expected_summary = {
        "dbuys": 4,
        "dsells": 2,
        "nbuys": 2,
        "nsells": 2,
        "vbuys": 400,
        "vsells": 200,
        "buy": 1,
        "sell": 2
    }
    actual_summary = orderbook.summarise_books(buy_book, sell_book, buy=buy, sell=sell)
    assert expected_summary == actual_summary


# Test the summarise_books_for_symbol returns a summary as expected
# Not necessarily testing the summarise_books itself
def test_summarise_some_books_for_symbol():
    buy_book = [
        Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
        Order(txid="6", csid="1", side="BUY", symbol="STI.", price=1.0, volume=300, ts=1),
        Order(txid="4", csid="1", side="BUY", symbol="STI.", price=0.9, volume=100, ts=1),
        Order(txid="5", csid="1", side="BUY", symbol="STI.", price=0.9, volume=100, ts=1),
    ]
    sell_book = [
        Order(txid="2", csid="1", side="SELL", symbol="STI.", price=2.0, volume=100, ts=1),
        Order(txid="3", csid="1", side="SELL", symbol="STI.", price=2.0, volume=100, ts=1),
    ]
    wrap_service_add_orders(buy_book + sell_book)

    expected_summary = {
        "dbuys": 4,
        "dsells": 2,
        "nbuys": 2,
        "nsells": 2,
        "vbuys": 400,
        "vsells": 200,
        "buy": 1,
        "sell": 2
    }

    actual_summary = orderbook.summarise_books_for_symbol("STI.", uow=TEST_ORDER_UOW())
    assert expected_summary == actual_summary
