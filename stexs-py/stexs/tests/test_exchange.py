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
    return Exchange()

@pytest.fixture
def stock_uow():
    repo = iop.base.GenericMemoryRepository(prefix="stocks")
    repo.store.clear()
    repo.store._clear()
    uow = iop.stock.MemoryStockUoW # uses the GMR
    return uow


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


def test_list_stocks(exchange, stock_uow):
    exchange.stock_uow = stock_uow
    exchange.add_stocks([
        model.Stock(symbol="TEST", name="Test Industrys"),
        model.Stock(symbol="STI.", name="Sam and Tom Industrys"),
    ])

    msg = {"txid": 1, "message_type": "list_stocks"}
    r = exchange.recv(msg)
    assert r == sorted(["TEST", "STI."])


def test_add_order_ok(exchange):
    pass


def test_add_order_bad_stock(exchange):
    pass

