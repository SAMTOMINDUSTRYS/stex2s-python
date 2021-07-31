import pytest
import stexs.io.persistence as iop
from stexs.domain import model
from stexs.services import orderbook

# TODO Need to turn this into some sort of fixture that nukes the memory between tests
TEST_ORDER_UOW = iop.order.OrderMemoryUoW

def wrap_service_add_orders(orders):
    for order in orders:
        orderbook.add_order(order, uow_cls=TEST_ORDER_UOW)

def wrap_service_close_txids(txids):
    orderbook.close_txids(txids, uow_cls=TEST_ORDER_UOW)

def wrap_service_match_one(buys, sells):
    return orderbook.match_one(buys, sells)

def wrap_service_execute_trade(trade):
    return orderbook.execute_trade(trade, uow_cls=TEST_ORDER_UOW)

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


def test_close_order():
    test_add_order()
    wrap_service_close_txids(["1"])

    with TEST_ORDER_UOW() as uow:
        actual_order = uow.orders.get("1")
    assert actual_order.closed is True


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
    assert trades[0].excess == 0
    assert trades[0].buy_txid == "1"
    assert trades[0].sell_txids == ["2"]
    assert trades[0].closed == False


def test_match_buy_with_excess_sell():
    buys = [
        model.Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    sells = [
        model.Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=1000, ts=1),
    ]
    trades = wrap_service_match_one(buys, sells)
    assert len(trades) == 1
    assert trades[0].excess == 900
    assert trades[0].buy_txid == "1"
    assert trades[0].sell_txids == ["2"]


def test_match_buy_with_multiple_sell():
    buys = [
        model.Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=1000, ts=1),
    ]
    sells = [
        model.Order(txid="2", csid="1", side="SELL", symbol="STI.", price=0.5, volume=500, ts=1),
        model.Order(txid="3", csid="1", side="SELL", symbol="STI.", price=1.0, volume=250, ts=1),
        model.Order(txid="4", csid="1", side="SELL", symbol="STI.", price=1.0, volume=300, ts=1),
        model.Order(txid="5", csid="1", side="SELL", symbol="STI.", price=1.0, volume=1000, ts=1),
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
        model.Order(txid="3", csid="1", side="SELL", symbol="STI.", price=1.0, volume=10, ts=1),
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
    assert trades[0].sell_txids == ["3"]


# Test the match_orderbook process returns a Trade matching expectations
# Not necessarily testing the matcher itself
def test_match_orderbook():
    expected_trade = model.Trade(
        symbol="STI.",
        buy_txid="4",
        sell_txids=["3", "2"],
        volume=200,
        avg_price=0.75,
        total_price=150,
        closed=False,
    )

    orders = [
        model.Order(txid="1", csid="1", side="BUY", symbol="STI.", price=0.5, volume=200, ts=1),
        model.Order(txid="4", csid="1", side="BUY", symbol="STI.", price=1.0, volume=200, ts=1),
        model.Order(txid="2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1),
        model.Order(txid="3", csid="1", side="SELL", symbol="STI.", price=0.5, volume=100, ts=1),
    ]
    wrap_service_add_orders(orders)
    actual_trade = orderbook.match_orderbook("STI.", uow_cls=TEST_ORDER_UOW)[0]

    assert expected_trade == actual_trade



def test_execute_trade():
    buys = [
        model.Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1),
    ]
    sells = [
        model.Order(txid="2/2", csid="1", side="SELL", symbol="STI.", price=1.0, volume=150, ts=1),
    ]
    wrap_service_add_orders(buys)
    wrap_service_add_orders(sells)
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
                sell_book = uow.orders.get_sell_book_for_symbol(sell.symbol)
                remaining_sell = None
                for rsell in sell_book:
                    if '/' in rsell.txid:
                        if rsell.txid.split('/')[0] == '2':
                            assert rsell.txid == '2/3'
                            remaining_sell = rsell

                assert remaining_sell is not None
                assert remaining_sell.volume == 50

