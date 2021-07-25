from stexs.domain import model
from stexs.services.exchange import Exchange

if __name__ == "__main__":
    stex = Exchange()
    stocks = [
        model.Stock(symbol="STI.", name="Sam and Tom Industrys"),
        model.Stock(symbol="ARRM", name="AbeRystwyth RISC Machines"),
        model.Stock(symbol="ELAN", name="Elan Dataworks"),
    ]
    stex.add_stocks(stocks)

    clients = [
        model.Client(csid="1", name="Sam"),
    ]
    stex.add_users(clients)
    stex.adjust_balance(csid="1", adjust_balance=+100000)
    stex.adjust_holding(csid="1", symbol="STI.", adjust_qty=+10000)
    stex.adjust_holding(csid="1", symbol="ELAN", adjust_qty=+10000)


    stex.recv({
        "txid": "1",
        "csid": "1",
        "side": "SELL",
        "symbol": "STI.",
        "price": 8.02,
        "volume": 1000,
    })
    stex.recv({
        "txid": "2",
        "csid": "1",
        "side": "SELL",
        "symbol": "STI.",
        "price": 8.02,
        "volume": 1000,
    })
    stex.recv({
        "txid": "3",
        "csid": "1",
        "side": "BUY",
        "symbol": "STI.",
        "price": 8.02,
        "volume": 500,
    })
    stex.recv({
        "txid": "4",
        "csid": "1",
        "side": "BUY",
        "symbol": "STI.",
        "price": 8.02,
        "volume": 500,
    })
    stex.recv({
        "txid": "5",
        "csid": "1",
        "side": "SELL",
        "symbol": "STI.",
        "price": 7.99,
        "volume": 1000,
    })
    stex.recv({
        "txid": "6",
        "csid": "1",
        "side": "BUY",
        "symbol": "STI.",
        "price": 8.03,
        "volume": 1500,
    })
    stex.recv({
        "txid": "7",
        "csid": "1",
        "side": "BUY",
        "symbol": "STI.",
        "price": 8.00,
        "volume": 500,
    })
    stex.recv({
        "txid": "8",
        "csid": "1",
        "side": "SELL",
        "symbol": "ELAN",
        "price": 4.10,
        "volume": 1000,
    })
    stex.recv({
        "txid": "9",
        "csid": "1",
        "side": "BUY",
        "symbol": "ELAN",
        "price": 4.10,
        "volume": 500,
    })

    #stex.request_stall("STI.")
