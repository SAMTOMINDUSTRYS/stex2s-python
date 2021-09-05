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


def test_message_unknown_message_type(exchange):
    r = exchange.recv({"txid": 1, "message_type": "invalid"})
    assert r["response_code"] == 1
    assert r["response_type"] == "exception"
    assert r["msg"] == "unknown message_type"


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


def test_add_order_unknown_stock(exchange):
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
    assert r["msg"] == "unknown symbol"


def test_instrument_summary_unknown_stock(exchange):
    msg = {"txid": 1, "message_type": "instrument_summary", "symbol": "TSI."}
    r = exchange.recv(msg)
    assert r["response_type"] == "exception"
    assert r["response_code"] == 404
    assert r["msg"] == "unknown symbol"


# TODO Should probably mock the stall but we're going to delete it soon
def test_format_instrument_summary(exchange):
    stall = model.MarketStall(stock=model.Stock(symbol="STI.", name="Sam and Tom Industrys"))
    price = 1.25
    stall.min_price = 1.00
    stall.max_price = price

    ts = int(time.time())
    stall.log_trade(
        model.Trade(
            tid="12345",
            symbol="STI.",
            buy_txid=1,
            total_price=price*100,
            avg_price=price,
            volume=100,
            ts=ts,
        )
    )
    stall.n_trades = 10
    stall.v_trades = 100

    summary = exchange.format_instrument_summary(stall)
    #assert summary["response_type"] == "instrument_summary"
    #assert summary["response_code"] == 0
    #assert summary["msg"] == "ok"
    assert summary["opening_price"] is None
    assert summary["closing_price"] is None
    assert summary["min_price"] == "1.0"
    assert summary["max_price"] == str(price)
    assert summary["num_trades"] == 10
    assert summary["vol_trades"] == 100
    assert summary["name"] == "Sam and Tom Industrys"
    assert summary["symbol"] == "STI."
    assert summary["last_trade_price"] == str(price)
    assert summary["last_trade_volume"] == 100
    assert summary["last_trade_ts"] == ts


def test_instrument_summary(exchange):
    msg = {"txid": 1, "message_type": "instrument_summary", "symbol": "STI."}
    r = exchange.recv(msg)
    assert r["response_type"] == "instrument_summary"
    assert r["response_code"] == 0
    assert r["msg"] == "ok"

