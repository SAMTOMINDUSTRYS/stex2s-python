import stexs.config as config

import socket
import json
import random

if __name__ == "__main__":
    txid = 1
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect(config.get_socket_host_and_port())
            if client:
                try:
                    client.send(json.dumps({
                        "type": "order",
                        "txid": "%d" % txid,
                        "broker": "MAGENTA",
                        "csid": "1",
                        "side": random.choice(["BUY", "SELL"]),
                        "symbol": "STI.",
                        "price": round(random.gauss(1, 0.25), 3),
                        "volume": int(random.uniform(1, 10)),
                    }).encode('ascii'))
                except Exception as e:
                    print(e)

                data = client.recv(4096)
                if not data:
                    break
                payload = json.loads( data.decode("ascii") )
                print(payload)

            client.shutdown(1)
            client.close()
            txid += 1
