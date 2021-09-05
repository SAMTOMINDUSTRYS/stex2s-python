import pytest
import time

from stexs.domain.model import Stock
from stexs.domain.broker import Client
from stexs.services.broker import Broker
from stexs.services.exchange import Exchange
import stexs.io.persistence as iop

@pytest.fixture
def e2e_broker():
    broker = Broker("MAGENTA", "Magenta Holdings Corporation")
    broker.user_uow = iop.user.MemoryClientUoW
    broker.add_users([
        Client(csid="1", name="Sam", balance=100, holdings={"STI.": 100}),
        Client(csid="2", name="Tom", balance=100, holdings={"STI.": 150}),

    ])
    return broker

@pytest.fixture
def e2e_exchange(e2e_broker):
    stex = Exchange()

    # Clear orders
    with iop.order.OrderMemoryUoW() as uow:
        uow.orders.clear()

    # Reset stocks
    stex.stock_uow = iop.stock.MemoryStockUoW
    repo = iop.base.GenericMemoryRepository(prefix="stocks")
    repo.clear()
    stex.add_stocks([
        Stock(symbol="TEST", name="Test Industrys"),
        Stock(symbol="STI.", name="Sam and Tom Industrys"),
    ])

    stex.brokers["MAGENTA"] = e2e_broker

    return stex

def test_basic_trade(e2e_exchange):

    # Submit three orders such that the first order is satisfied by a combination
    # of the entire second order and third partial order

    ts = int(time.time()) - 30

    msg = {
        "txid": "1",
        "message_type": "new_order",
        "broker_id": "MAGENTA",
        "account_id": 1,
        "side": "BUY",
        "symbol": "STI.",
        "price": "1.00",
        "volume": 100,
        "sender_ts": ts,
    }
    r = e2e_exchange.recv(msg)
    print(r)

    msg = {
        "txid": "2",
        "message_type": "new_order",
        "broker_id": "MAGENTA",
        "account_id": 2,
        "side": "SELL",
        "symbol": "STI.",
        "price": "0.50",
        "volume": 50,
        "sender_ts": ts+10,
    }
    r = e2e_exchange.recv(msg)
    print(r)

    msg = {
        "txid": "3",
        "message_type": "new_order",
        "broker_id": "MAGENTA",
        "account_id": 2,
        "side": "SELL",
        "symbol": "STI.",
        "price": "0.75",
        "volume": 100,
        "sender_ts": ts+20,
    }
    r = e2e_exchange.recv(msg)
    print(r)

    test_uow = e2e_exchange.brokers["MAGENTA"].user_uow()
    with test_uow:
        sam = test_uow.users.get("1")
        tom = test_uow.users.get("2")

        assert sam.balance == 0
        assert tom.balance == 100 + (0.5*50) + (0.75*50)

        assert sam.holdings["STI."] == 200
        assert tom.holdings["STI."] == 0

