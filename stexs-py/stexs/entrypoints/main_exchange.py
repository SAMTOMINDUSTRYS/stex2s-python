from stexs.domain import model
from stexs.services.exchange import Exchange
from stexs.services.broker import Broker
from stexs.services.logger import log
import stexs.config as config

import socket
import json

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

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(config.get_socket_host_and_port())
        log.debug("Listening: %s" % str(config.get_socket_host_and_port()))
        s.listen()

        while True:
            conn, addr = s.accept()
            with conn:
                log.debug("Connection from %s" % str(addr))
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break

                    payload = json.loads( data.decode("ascii") )
                    reply = stex.recv(payload)

                    payload = json.dumps(reply).encode("ascii")
                    conn.send(payload)

