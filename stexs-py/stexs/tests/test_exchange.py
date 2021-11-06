import pytest
import time
from dataclasses import asdict as dataclasses_asdict

from stexs.domain import model
from stexs.domain.broker import OrderScreeningException
from stexs.services import orderbook
from stexs.services.exchange import Exchange
import stexs.io.persistence as iop

#TODO Will need to mock/test the sockety stuff eventually
class TestSocketClient():
    pass

@pytest.fixture
def patched_exchange():
    stex = Exchange()

    # Clear orders
    with iop.order.OrderMemoryUoW() as uow:
        uow.orders.clear()

    # Reset stocks
    stex.stock_uow = iop.stock.MemoryStockUoW
    repo = iop.base.GenericMemoryRepository(prefix="stocks")
    repo.store.clear()
    repo.store._clear()
    stex.add_stocks([
        model.Stock(symbol="TEST", name="Test Industrys"),
        model.Stock(symbol="STI.", name="Sam and Tom Industrys"),
    ])

    # Mock broker
    # TODO Not ideal as we are masking the behaviour of the real broker so need to be careful
    class BasicBroker:
        def get_user(self, account_id):
            return account_id if account_id in [1, 999] else None
        def validate_preorder(self, user, order, reference_price=None):
            if order.csid == 999:
                raise Exception("bang")
            elif order.side == "BUY":
                return True
            else:
                # Simulate Exception for any SELL
                raise OrderScreeningException("Insufficient holdings")

            return False
        def update_users(self, buys, sells, executed, reference_price=None):
            return True
    stex.brokers["MAGENTA"] = BasicBroker()

    return stex

def test_message_transaction_set(patched_exchange):
    patched_exchange.recv({"txid": 1, "message_type": "test"})
    patched_exchange.recv({"txid": 800, "message_type": "test"})
    patched_exchange.recv({"txid": 2, "message_type": "test"})
    patched_exchange.recv({"txid": 808, "message_type": "test"})
    assert patched_exchange.txid_set == set([1, 800, 2, 808])


def test_message_unknown_message_type(patched_exchange):
    r = patched_exchange.recv({"txid": 1, "message_type": "invalid"})
    assert r["response_code"] == 1
    assert r["response_type"] == "exception"
    assert r["msg"] == "unknown message_type"


def test_message_duplicate_transaction(patched_exchange):
    patched_exchange.txid_set.add(1)
    msg = {"txid": 1, "message_type": "test"}

    r = patched_exchange.recv(msg)
    assert r["response_code"] == 1
    assert r["response_type"] == "exception"
    assert r["msg"] == "duplicate transaction"


def test_message_stale_transaction(patched_exchange):
    stale_ts = int(time.time()) - 100
    msg = {"txid": 1, "message_type": "test", "sender_ts": stale_ts}

    r = patched_exchange.recv(msg)
    assert r["response_code"] == 1
    assert r["response_type"] == "exception"
    assert r["msg"] == "stale transaction"


def test_list_stocks(patched_exchange):
    msg = {"txid": 1, "message_type": "list_stocks"}
    r = patched_exchange.recv(msg)
    assert r == sorted(["TEST", "STI."])


def test_add_limit_order_ok(patched_exchange):
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
    r = patched_exchange.recv(msg)
    assert r["response_type"] == "new_order"
    assert r["response_code"] == 0
    assert r["msg"] == "ok"

def test_add_market_order_ok(patched_exchange):
    msg = {
        "txid": 1,
        "message_type": "new_order",
        "broker_id": "MAGENTA",
        "account_id": 1,
        "side": "BUY",
        "symbol": "STI.",
        "price": None,
        "volume": 100,
        "sender_ts": int(time.time()),
    }
    r = patched_exchange.recv(msg)
    assert r["response_type"] == "new_order"
    assert r["response_code"] == 0
    assert r["msg"] == "ok"


def test_add_order_bad_validate(patched_exchange):
    msg = {
        "txid": 1,
        "message_type": "new_order",
        "broker_id": "MAGENTA",
        "account_id": 1,
        "side": "SELL",
        "symbol": "STI.",
        "price": "1.01",
        "volume": 100,
        "sender_ts": int(time.time()),
    }
    r = patched_exchange.recv(msg)
    assert r["response_type"] == "exception"
    assert r["response_code"] == 77
    assert r["msg"] == "Insufficient holdings"


def test_add_order_explode_validate(patched_exchange):
    msg = {
        "txid": 1,
        "message_type": "new_order",
        "broker_id": "MAGENTA",
        "account_id": 999,
        "side": "BUY",
        "symbol": "STI.",
        "price": "1.01",
        "volume": 100,
        "sender_ts": int(time.time()),
    }
    r = patched_exchange.recv(msg)
    assert r["response_type"] == "exception"
    assert r["response_code"] == 70
    assert r["msg"] == "bang"


def test_add_order_unknown_broker(patched_exchange):
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
    r = patched_exchange.recv(msg)
    assert r["response_type"] == "exception"
    assert r["response_code"] == 404
    assert r["msg"] == "malformed broker"


def test_add_order_unknown_user(patched_exchange):
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
    r = patched_exchange.recv(msg)
    assert r["response_type"] == "exception"
    assert r["response_code"] == 404
    assert r["msg"] == "unknown user"


def test_add_order_unknown_stock(patched_exchange):
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
    r = patched_exchange.recv(msg)
    assert r["response_type"] == "exception"
    assert r["response_code"] == 404
    assert r["msg"] == "unknown symbol"


def test_instrument_summary_unknown_stock(patched_exchange):
    msg = {"txid": 1, "message_type": "instrument_summary", "symbol": "TSI."}
    r = patched_exchange.recv(msg)
    assert r["response_type"] == "exception"
    assert r["response_code"] == 404
    assert r["msg"] == "unknown symbol"


# TODO Should probably mock the stall here and test it properly somewhere else but we're going to delete it soon
def test_format_instrument_summary(patched_exchange):
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

    summary = patched_exchange.format_instrument_summary(stall)
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


def test_instrument_summary(patched_exchange):
    msg = {"txid": 1, "message_type": "instrument_summary", "symbol": "STI."}
    r = patched_exchange.recv(msg)
    assert r["response_type"] == "instrument_summary"
    assert r["response_code"] == 0
    assert r["msg"] == "ok"


def test_instrument_trade_history_unknown_stock(patched_exchange):
    msg = {"txid": 1, "message_type": "instrument_trade_history", "symbol": "TSI."}
    r = patched_exchange.recv(msg)
    assert r["response_type"] == "exception"
    assert r["response_code"] == 404
    assert r["msg"] == "unknown symbol"


# Again, belongs in a test suite for marketstall, but we're gonna get rid of it soon
def test_format_instrument_trade_history(patched_exchange):
    stall = model.MarketStall(stock=model.Stock(symbol="STI.", name="Sam and Tom Industrys"))
    trade = model.Trade(
        tid="12345",
        symbol="STI.",
        buy_txid=1,
        total_price=1.25*100,
        avg_price=1.25,
        volume=100,
        ts=int(time.time()),
    )
    stall.log_trade(trade)

    trade_history = patched_exchange.get_trade_history(stall)
    assert trade_history == [dataclasses_asdict(trade)]

def test_instrument_trade_history_empty(patched_exchange):
    msg = {"txid": 1, "message_type": "instrument_trade_history", "symbol": "STI."}
    r = patched_exchange.recv(msg)
    assert r["response_type"] == "instrument_trade_history"
    assert r["response_code"] == 0
    assert r["msg"] == "ok"
    assert r["symbol"] == "STI."
    assert r["trade_history"] == []


def test_instrument_orderbook_summary_unknown_stock(patched_exchange):
    msg = {"txid": 1, "message_type": "instrument_orderbook_summary", "symbol": "TSI."}
    r = patched_exchange.recv(msg)
    assert r["response_type"] == "exception"
    assert r["response_code"] == 404
    assert r["msg"] == "unknown symbol"


# We have already tested orderbook.summarise_books_for_symbol
def test_format_instrument_orderbook_summary(patched_exchange):
    summary = orderbook.summarise_books_for_symbol("STI.")

    msg = {"txid": 1, "message_type": "instrument_orderbook_summary", "symbol": "STI."}
    r = patched_exchange.recv(msg)
    assert r["response_type"] == "instrument_orderbook_summary"
    assert r["response_code"] == 0
    assert r["msg"] == "ok"
    assert r["depth_buys"] == summary["dbuys"]
    assert r["depth_sells"] == summary["dsells"]
    assert r["top_num_buys"] == summary["nbuys"]
    assert r["top_num_sells"] == summary["nsells"]
    assert r["top_vol_buys"] == summary["vbuys"]
    assert r["top_vol_sells"] == summary["vsells"]
    assert r["current_buy"] == str(summary["buy"])
    assert r["current_sell"] == str(summary["sell"])


def test_instrument_orderbook_unknown_stock(patched_exchange):
    msg = {"txid": 1, "message_type": "instrument_orderbook", "symbol": "TSI."}
    r = patched_exchange.recv(msg)
    assert r["response_type"] == "exception"
    assert r["response_code"] == 404
    assert r["msg"] == "unknown symbol"


# We have already tested orderbook.get_serialised_order_books_for_symbol
# TODO Test with n=10 properly
def test_format_instrument_orderbook_empty(patched_exchange):
    order_books = orderbook.get_serialised_order_books_for_symbol("STI.", n=10)

    msg = {"txid": 1, "message_type": "instrument_orderbook", "symbol": "STI."}
    r = patched_exchange.recv(msg)
    assert r["response_type"] == "instrument_orderbook"
    assert r["response_code"] == 0
    assert r["msg"] == "ok"
    assert r["symbol"] == "STI."
    assert r["buy_book"] == order_books["buy_book"]
    assert r["sell_book"] == order_books["sell_book"]

