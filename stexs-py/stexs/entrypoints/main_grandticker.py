from rich import print, box
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from datetime import datetime
import random
from time import sleep, time
import stexs.config as config
import socket
import json

if __name__ == "__main__":
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="info", size=7),
        Layout(name="summary", size=5),
        Layout(name="tables"),
        Layout(name="messages", size=7),
        Layout(name="history"),
        #Layout(name="footer", size=6),
    )
    layout["tables"].split_row(
        Layout(name="buys"),
        Layout(name="sells"),
    )


    # Header
    # https://github.com/willmcgugan/rich/blob/master/examples/fullscreen.py
    class Header:
        def __rich__(self):
            grid = Table.grid(expand=True)
            grid.add_column(justify="center", ratio=1)
            grid.add_column(justify="right")
            grid.add_row(
                "Sam and Tom's Electronic Exchange",
                datetime.now().ctime().replace(":", "[blink]:[/]"),
            )
            return Panel(grid, style="bold black on #EC008C")
    layout["header"].update(Header())

    def make_info(ticker, title, curr_price, min_price, max_price, tot_vol, tot_trade):
        grid = Table.grid(expand=True, padding=(0,3,0,0))
        grid.add_column(justify="left", ratio=1)
        grid.add_column(justify="right")
        grid.add_column(justify="right")
        grid.add_row(
            "[bold white on black]%s[/] %s" % (ticker, title),
            "",
            "[b]%.3f[/] XST" % curr_price if curr_price else "-.--- XST",
        )
        grid.add_row(
            "",
            "",
            "[b]%.3f[/] MIN" % min_price if min_price else "-.--- MIN",
        )
        grid.add_row(
            "",
            "",
            "[b]%.3f[/] MAX" % max_price if max_price else "-.--- MAX",
        )
        grid.add_row(
            "",
            "",
            "[b]%03d[/] NUM" % tot_trade if tot_trade else "----- NUM",
        )
        grid.add_row(
            "",
            "",
            "[b]%03d[/] VOL" % tot_vol if tot_vol else "----- VOL",
        )
        return Panel(grid, style="white on black")


    def make_order_table(rows, direction, title="TX Table", n=25):

        columns = ["TX", "TS", "CSID", "Volume", "Price"]
        table = Table(expand=True, box=box.HORIZONTALS)

        if direction == "SELL":
            columns = reversed(columns)

        for col in columns:
            table.add_column(col)

        i = 0
        for tx in rows:
            i += 1
            if direction == "BUY":
                table.add_row(
                    tx["txid"],
                    str(tx["ts"]),
                    tx["csid"],
                    str(tx["volume"]),
                    str(tx["price"]),
                )
            elif direction == "SELL":
                table.add_row(
                    str(tx["price"]),
                    str(tx["volume"]),
                    tx["csid"],
                    str(tx["ts"]),
                    tx["txid"],
                )

            if i >= n:
                break

        return Panel(
            table,
            title=title,
            box=box.HORIZONTALS,
            style="white on %s" % ("#850900" if direction == "SELL" else "#408500")
        )

    def make_messages(rows, title="TX Messages"):
        table = Table(expand=True, box=box.HORIZONTALS)
        table.add_column("TXID")
        table.add_column("CSID")
        table.add_column("TS")
        table.add_column("Side")
        table.add_column("Price")
        table.add_column("Volume")

        for tx in rows:
            side_colour = "yellow"
            side_label = "  ????  "
            if tx["side"] == "BUY":
                side_label = "  BUY   "
                side_colour = "green"
            elif tx["side"] == "SELL":
                side_label = "  SELL  "
                side_colour = "red"

            table.add_row(
                tx["txid"],
                tx["account_id"],
                tx["ts"],
                "[white on %s]%s[/]" % (side_colour, side_label),
                str(tx["price"]),
                str(tx["volume"]),
            )
        return Panel(
            table,
            title=title
        )

    def make_trade_history(rows, title="Trade History"):
        table = Table(expand=True, box=box.HORIZONTALS)
        table.add_column("TrID", ratio=2)
        table.add_column("Unit Price", ratio=1)
        table.add_column("Total Price", ratio=1)
        table.add_column("Volume", ratio=1)
        table.add_column("Buy", ratio=1)
        table.add_column("Sells", ratio=1)

        for tx in rows:
            table.add_row(
                tx["tid"],
                "%.3f" % tx["avg_price"],
                "%.3f" % tx["total_price"],
                str(tx["volume"]),
                tx["buy_txid"],
                ','.join(tx["sell_txids"]),
            )
        return Panel(
            table,
            title=title
        )


    def make_summary(summary):
        summary_table = Table(expand=True, box=box.HORIZONTALS, style="black on yellow")
        for col in ["dbuys", "nbuys", "vbuys", "buy" , "---", "sell", "vsells", "nsells", "dsells"]:
            summary_table.add_column(col, justify="center", ratio=1)
        summary_table.add_row(
            str(summary["dbuys"]),
            str(summary["nbuys"]),
            str(summary["vbuys"]),
            str(summary["buy"]),
            '---',
            str(summary["sell"]),
            str(summary["vsells"]),
            str(summary["nsells"]),
            str(summary["dsells"]),
        )
        return summary_table

    def make_footer(msg_payload):
        return Panel(
            str(msg_payload),
            title="Last message",
        )


    with Live(layout, refresh_per_second=1, screen=True) as l:
        txid = 1
        while True:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect(config.get_socket_host_and_port())
                if client:
                    try:
                        if txid == 1:
                            client.send(json.dumps({
                                "message_type": "list_stocks",
                                "txid": "%d" % txid,
                            }).encode('ascii'))
                        elif txid % 10 == 0:
                            client.send(json.dumps({
                                "message_type": "summary",
                                "txid": "%d" % txid,
                                "symbol": "STI.",
                            }).encode('ascii'))
                        else:
                            msg = {
                                "message_type": "new_order",
                                "type": "order",
                                "txid": "%d" % txid,
                                "broker_id": "MAGENTA",
                                "account_id": "1",
                                "side": random.choice(["BUY", "SELL"]),
                                "symbol": "STI.",
                                "price": round(random.gauss(1, 0.25), 3),
                                "volume": int(random.uniform(1, 10)),
                                "ts": str(int(time())),
                            }
                            client.send(json.dumps(msg).encode("ascii"))
                    except Exception as e:
                        raise e

                    data = client.recv(65536)
                    if not data:
                        break
                    payload = json.loads( data.decode("ascii") )

                    if txid % 10 == 0:
                        payload = payload["STI."]
                        layout["summary"].update(make_summary(payload["order_summary"]))

                        last_buy = payload["ticker_summary"]["last_price"]
                        min_price = payload["ticker_summary"]["min_price"]
                        max_price = payload["ticker_summary"]["max_price"]
                        tot_vol = payload["ticker_summary"]["v_trades"]
                        n_trade = payload["ticker_summary"]["n_trades"]
                        symbol = payload["ticker_summary"]["stock"]["symbol"]
                        name = payload["ticker_summary"]["stock"]["name"]
                        layout["info"].update(make_info(symbol, name, last_buy, min_price, max_price, tot_vol, n_trade))
                        layout["history"].update(make_trade_history(payload["ticker_summary"]["order_history"][-10:]))

                        buys_book = payload["order_books"]["buy_book"]
                        sells_book = payload["order_books"]["sell_book"]
                        layout["buys"].update(make_order_table(buys_book, direction="BUY", title="Buy Book"))
                        layout["sells"].update(make_order_table(sells_book, direction="SELL", title="Sell Book"))
                    elif txid > 1:
                        layout["messages"].update(make_messages([msg]))

                client.shutdown(1)
                client.close()
                txid += 1
                sleep(0.1)

