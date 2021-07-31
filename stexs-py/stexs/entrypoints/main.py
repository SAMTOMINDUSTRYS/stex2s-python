from stexs.domain import model
from stexs.services.exchange import Exchange
from stexs.services.broker import Broker

if __name__ == "__main__":
    stex = Exchange()
    stocks = [
        model.Stock(symbol="STI.", name="Sam and Tom Industrys"),
        model.Stock(symbol="ARRM", name="AbeRystwyth RISC Machines"),
        model.Stock(symbol="ELAN", name="Elan Dataworks"),
    ]
    stex.add_stocks(stocks)

    broker = Broker(code="MAGENTA", name="Magenta Holdings Plc.")
    stex.add_broker(broker)
    broker.connect_exchange(stex) # Little hack to emulate a connection from Broker to Exchange

    clients = [
        model.Client(csid="1", name="Sam"),
    ]
    broker.add_users(clients)
    broker.adjust_balance(csid="1", adjust_balance=+100000)
    broker.adjust_holding(csid="1", symbol="STI.", adjust_qty=+10000)
    broker.adjust_holding(csid="1", symbol="ELAN", adjust_qty=+10000)



    stex.recv({
        "type": "order",
        "txid": "1",
        "broker": "MAGENTA",
        "csid": "1",
        "side": "SELL",
        "symbol": "STI.",
        "price": 8.02,
        "volume": 1000,
    })
    stex.recv({
        "type": "order",
        "txid": "2",
        "broker": "MAGENTA",
        "csid": "1",
        "side": "SELL",
        "symbol": "STI.",
        "price": 8.02,
        "volume": 1000,
    })
    stex.recv({
        "type": "order",
        "txid": "3",
        "broker": "MAGENTA",
        "csid": "1",
        "side": "BUY",
        "symbol": "STI.",
        "price": 8.02,
        "volume": 500,
    })
    stex.recv({
        "type": "order",
        "txid": "4",
        "broker": "MAGENTA",
        "csid": "1",
        "side": "BUY",
        "symbol": "STI.",
        "price": 8.02,
        "volume": 500,
    })
    stex.recv({
        "type": "order",
        "txid": "5",
        "broker": "MAGENTA",
        "csid": "1",
        "side": "SELL",
        "symbol": "STI.",
        "price": 7.99,
        "volume": 1000,
    })
    stex.recv({
        "type": "order",
        "txid": "6",
        "broker": "MAGENTA",
        "csid": "1",
        "side": "BUY",
        "symbol": "STI.",
        "price": 8.03,
        "volume": 1500,
    })
    stex.recv({
        "type": "order",
        "txid": "7",
        "broker": "MAGENTA",
        "csid": "1",
        "side": "BUY",
        "symbol": "STI.",
        "price": 8.00,
        "volume": 500,
    })
    stex.recv({
        "type": "order",
        "txid": "8",
        "broker": "MAGENTA",
        "csid": "1",
        "side": "SELL",
        "symbol": "ELAN",
        "price": 4.10,
        "volume": 1000,
    })
    stex.recv({
        "type": "order",
        "txid": "9",
        "broker": "MAGENTA",
        "csid": "1",
        "side": "BUY",
        "symbol": "ELAN",
        "price": 4.10,
        "volume": 500,
    })

    #stex.request_stall("STI.")

