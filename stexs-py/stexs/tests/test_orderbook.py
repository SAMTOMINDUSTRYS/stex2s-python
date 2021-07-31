import pytest
import stexs.io.persistence as iop
from stexs.domain import model
from stexs.services import orderbook

TEST_ORDER_UOW = iop.order.OrderMemoryUoW

def wrap_service_add_orders(orders):
    for order in orders:
        orderbook.add_order(order, uow=TEST_ORDER_UOW)

def wrap_service_match_one(buys, sells):
    return orderbook.match_one(buys, sells)

# Test add_order service (not the underlying persistence)
def test_add_order():
    expected_order = model.Order(
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


# TODO This needs to be done for each UoW as the sorting is done in the Repo...
def test_buybook_ordered_by_maxprice_mintime():
    orders = [
        model.Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
        model.Order(txid="2", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=2),
        model.Order(txid="3", csid="1", side="BUY", symbol="STI.", price=2.0, volume=100, ts=3),
    ]
    wrap_service_add_orders(orders)

    with TEST_ORDER_UOW() as uow:
        buybook = uow.orders.get_buy_book_for_symbol("STI.")

    assert buybook == [orders[2], orders[0], orders[1]]


# TODO This needs to be done for each UoW as the sorting is done in the Repo...
def test_sellbook_ordered_by_minprice_mintime():
    orders = [
        model.Order(txid="1", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1),
        model.Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=2),
        model.Order(txid="3", csid="1", side="SELL", symbol="STI.", price=0.5, volume=100, ts=3),
    ]
    wrap_service_add_orders(orders)

    with TEST_ORDER_UOW() as uow:
        sellbook = uow.orders.get_sell_book_for_symbol("STI.")

    assert sellbook == [orders[2], orders[0], orders[1]]


def test_match_order_with_empty_buy_has_no_trades():
    orders = [
        model.Order(txid="1", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    trades = wrap_service_match_one([], orders)
    assert len(trades) == 0


def test_match_order_with_empty_sell_has_no_trades():
    orders = [
        model.Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    trades = wrap_service_match_one(orders, [])
    assert len(trades) == 0


def test_match_buy_with_perfect_sell():
    buys = [
        model.Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    sells = [
        model.Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 1
    assert trades[0]["excess"] == 0
    assert trades[0]["buy"] == "1"
    assert trades[0]["sells"] == ["2"]


def test_match_buy_with_excess_sell():
    buys = [
        model.Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    sells = [
        model.Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=1000, ts=1),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 1
    assert trades[0]["excess"] == 900
    assert trades[0]["buy"] == "1"
    assert trades[0]["sells"] == ["2"]


def test_match_buy_with_multiple_sell():
    buys = [
        model.Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=1000, ts=1),
    ]
    sells = [
        model.Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=500, ts=1),
        model.Order(txid="3", csid="1", side="SELL", symbol="STI.", price=1.0, volume=250, ts=1),
        model.Order(txid="4", csid="1", side="SELL", symbol="STI.", price=1.0, volume=300, ts=1),
        model.Order(txid="5", csid="1", side="SELL", symbol="STI.", price=1.0, volume=1000, ts=1),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 1
    assert trades[0]["excess"] == 50
    assert trades[0]["buy"] == "1"
    assert trades[0]["sells"] == ["2", "3", "4"]


def test_match_buy_with_overpriced_sell():
    buys = [
        model.Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    sells = [
        model.Order(txid="2", csid="1", side="SELL", symbol="STI.", price=2.0, volume=500, ts=1),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 0


def test_match_buy_with_insufficient_sell():
    buys = [
        model.Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    sells = [
        model.Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=50, ts=1),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 0


def test_match_aborts_on_closed_buy():
    buys = [
        model.Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1, closed=True),
    ]
    sells = [
        model.Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 0


def test_match_skips_on_closed_sell():
    buys = [
        model.Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    sells = [
        model.Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1, closed=True),
        model.Order(txid="3", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 1
    assert trades[0]["sells"] == ["3"]

