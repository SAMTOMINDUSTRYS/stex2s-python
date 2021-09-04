import pytest
import time

from stexs.domain import model
from stexs.services.exchange import Exchange
import stexs.io.persistence as iop

#TODO Will need to mock/test the sockety stuff eventually
class TestSocketClient():
    pass

@pytest.fixture
def exchange():
    stex = Exchange()

    # Stocks
    stex.stock_uow = iop.stock.MemoryStockUoW
    repo = iop.base.GenericMemoryRepository(prefix="stocks")
    repo.store.clear()
    repo.store._clear()
    stex.add_stocks([
        model.Stock(symbol="TEST", name="Test Industrys"),
        model.Stock(symbol="STI.", name="Sam and Tom Industrys"),
    ])

    # Mock broker
    class BasicBroker:
        def get_user(self, account_id):
            return account_id if account_id == 1 else None
        def validate_preorder(self, user, order):
            return True
        def update_users(self, buys, sells, executed):
            return True
    stex.brokers["MAGENTA"] = BasicBroker()
    return stex



def test_message_transaction_set(exchange):
    exchange.recv({"txid": 1, "message_type": "test"})
    exchange.recv({"txid": 800, "message_type": "test"})
    exchange.recv({"txid": 2, "message_type": "test"})
    exchange.recv({"txid": 808, "message_type": "test"})
    assert exchange.txid_set == set([1, 800, 2, 808])


def test_message_duplicate_transaction(exchange):
    exchange.txid_set.add(1)
    msg = {"txid": 1, "message_type": "test"}

    r = exchange.recv(msg)
    assert r["response_code"] == 1
    assert r["response_type"] == "exception"
    assert r["msg"] == "duplicate transaction"


def test_message_stale_transaction(exchange):
    stale_ts = int(time.time()) - 100
    msg = {"txid": 1, "message_type": "test", "sender_ts": stale_ts}

    r = exchange.recv(msg)
    assert r["response_code"] == 1
    assert r["response_type"] == "exception"
    assert r["msg"] == "stale transaction"


def test_list_stocks(exchange):
    msg = {"txid": 1, "message_type": "list_stocks"}
    r = exchange.recv(msg)
    assert r == sorted(["TEST", "STI."])


def test_add_order_ok(exchange):
    msg = {
        "txid": 1,
        "message_type": "new_order",
        "broker_id": "MAGENTA",
        "account_id": 1,
        "side": "BUY",
        "symbol": "STI.",
        "price": "1.01",
        "volume": 100,
        "sender_ts": int(time.time()),
    }
    r = exchange.recv(msg)
    assert r["response_type"] == "new_order"
    assert r["response_code"] == 0
    assert r["msg"] == "ok"


def test_add_order_unknown_broker(exchange):
    msg = {
        "txid": 1,
        "message_type": "new_order",
        "broker_id": "NOMAGENTA",
        "account_id": 1,
        "side": "BUY",
        "symbol": "STI.",
        "price": "1.01",
        "volume": 100,
        "sender_ts": int(time.time()),
    }
    r = exchange.recv(msg)
    assert r["response_type"] == "exception"
    assert r["response_code"] == 404
    assert r["msg"] == "malformed broker"


def test_add_order_unknown_user(exchange):
    msg = {
        "txid": 1,
        "message_type": "new_order",
        "broker_id": "MAGENTA",
        "account_id": 2,
        "side": "BUY",
        "symbol": "STI.",
        "price": "1.01",
        "volume": 100,
        "sender_ts": int(time.time()),
    }
    r = exchange.recv(msg)
    assert r["response_type"] == "exception"
    assert r["response_code"] == 404
    assert r["msg"] == "unknown user"


def test_add_order_bad_stock(exchange):
    msg = {
        "txid": 1,
        "message_type": "new_order",
        "broker_id": "MAGENTA",
        "account_id": 1,
        "side": "BUY",
        "symbol": "TSI.",
        "price": "1.01",
        "volume": 100,
        "sender_ts": int(time.time()),
    }
    r = exchange.recv(msg)
    assert r["response_type"] == "exception"
    assert r["response_code"] == 404
    assert r["msg"] == "unknown instrument"
