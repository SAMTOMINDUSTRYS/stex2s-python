from rich import print, box
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from datetime import datetime
import random
from time import sleep, time
import stexs.config as config
import socket
import json
import sys
import tty
import termios
from pynput import keyboard

if __name__ == "__main__":
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="info", size=7),
        Layout(name="summary", size=5),
        Layout(name="tables"),
        Layout(name="messages", size=7),
        Layout(name="history"),
        Layout(name="footer", size=6),
        Layout(name="status", size=1),
    )
    layout["tables"].split_row(
        Layout(name="buys"),
        Layout(name="sells"),
    )


    class STEXEvents(keyboard.Events):
        def __init__(self):
            super(keyboard.Events, self).__init__(
                on_press=self.Press,
                on_release=self.Release,
                #suppress=True, enable this to either crash the shell, or take complete exclusive input from X such that you need sysreq to get the system back
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

        curr_price = float(curr_price) if curr_price is not None else None
        min_price = float(min_price) if min_price is not None else None
        max_price = float(max_price) if max_price is not None else None

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
                str(tx["sender_ts"]),
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
            str(summary["depth_buys"]),
            str(summary["top_num_buys"]),
            str(summary["top_vol_buys"]),
            str(summary["current_buy"]),
            '---',
            str(summary["current_sell"]),
            str(summary["top_vol_sells"]),
            str(summary["top_num_sells"]),
            str(summary["depth_sells"]),
        )
        return summary_table

    def make_footer(msg_payload):
        return Panel(
            str(msg_payload),
            title="Last message",
        )

    def make_status(status=None):
        if not status:
            return "[bold black on #EC008C]Controls[/] [bold black on white]Space[/] Play next message [bold black on white]o[/] Order [bold black on white]q[/] Quit"
        else:
            return status

    def recv_payload(client):
        data = client.recv(65536)
        if not data:
            return None
        payload = json.loads( data.decode("ascii") )
        layout["footer"].update(make_footer(payload))
        return payload

    def hoot_char():
        # jfc what the f am i doing
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        new = termios.tcgetattr(fd)

        # ICRNL Translate carriage return to newline on input
        # not IGNCR Ignore carriage return on input
        new[tty.IFLAG] = new[tty.IFLAG] | termios.ICRNL & ~termios.IGNCR

        # not ECHO
        # not ICANON (non canonical mode)
        #   Disables line-by-line
        #   Disables line editing
        # not IEXTEN
        new[tty.LFLAG] = new[tty.LFLAG] & ~(termios.ECHO | termios.ICANON | termios.IEXTEN)

        # VMIN Minimum number of characters for noncanonical read
        new[tty.CC][termios.VMIN] = 1

        try:
            # Apply
            termios.tcsetattr(fd, termios.TCSANOW, new)
            ch = sys.stdin.read(1)
        finally:
            # Reset
            termios.tcsetattr(fd, termios.TCSADRAIN, old)
        return ch


    with Live(layout, refresh_per_second=25, screen=True) as l:
        txid = 1
        random_client_id = str(uuid.uuid4())[:4]
        while True:
            layout["status"].update(make_status())
            auto_order = True
            manual_order = ""
            manual_side = manual_price = manual_vol = None

            # https://pynput.readthedocs.io/en/latest/keyboard.html#monitoring-the-keyboard
            with STEXEvents() as events:
                for event in events:
                    if event.key == keyboard.Key.space:
                        break
                    elif hasattr(event.key, "char"):
                        if event.key.char == 'q':
                            sys.exit(0)
                        elif event.key.char == 'o':
                            auto_order = False
                            break
                char = hoot_char() # Naive consume from stdin as pynput can't suppress, works for now

            if not auto_order:
                manual_order = ""
                manual_prompt_base = "[bold white on black]Enter an order in the format: [[green]BUY[/]|[red]SELL[/]] [[cyan]VOL[/]]@[[yellow]PRICE[/]][/] "
                manual_prompt = manual_prompt_base
                not_valid = True
                while not_valid:
                    layout["status"].update(make_status(manual_prompt))

                    char = hoot_char()

                    if char == '\x7f':
                        # Backspace
                        manual_order = manual_order[:-1]
                        manual_prompt = manual_prompt[:-1]
                    elif char == '\n':
                        # Submit
                        try:
                            manual_side, volprice = manual_order.split(" ")
                            manual_side = manual_side.upper()
                            manual_vol, manual_price = volprice.split("@")
                            not_valid = False
                        except Exception as e:
                            manual_prompt = manual_prompt_base
                            manual_order = ""
                            not_valid = True
                    elif char is not None:
                        # Assume char is good
                        manual_order += str(char)
                        manual_prompt += str(char)


            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect(config.get_socket_host_and_port())
                if client:
                    # Send an order
                    msg = {
                        "message_type": "new_order",
                        "type": "order",
                        "txid": "%s-%d" % (random_client_id, txid),
                        "broker_id": "MAGENTA",
                        "account_id": "1",
                        "side": random.choice(["BUY", "SELL"]),
                        "symbol": "STI.",
                        "price": str(round(random.gauss(1, 0.25), 3)),
                        "volume": int(random.uniform(1, 10)),
                        "sender_ts": int(time()),
                    }
                    if not auto_order:
                        msg["side"] = manual_side
                        msg["price"] = manual_price
                        msg["volume"] = int(manual_vol)
                        auto_order = True

                    client.send(json.dumps(msg).encode("ascii"))
                    layout["messages"].update(make_messages([msg]))

                    payload = recv_payload(client)

                    # Update GUI
                    client.send(json.dumps({
                        "message_type": "instrument_summary",
                        "symbol": "STI.",
                    }).encode('ascii'))
                    payload = recv_payload(client)

                    last_buy = payload["last_trade_price"]
                    min_price = payload["min_price"]
                    max_price = payload["max_price"]
                    tot_vol = payload["vol_trades"]
                    n_trade = payload["num_trades"]
                    symbol = payload["symbol"]
                    name = payload["name"]
                    #TODO Open/close, last_trade vol/ts
                    layout["info"].update(make_info(symbol, name, last_buy, min_price, max_price, tot_vol, n_trade))

                    client.send(json.dumps({
                        "message_type": "instrument_trade_history",
                        "symbol": "STI.",
                    }).encode('ascii'))
                    payload = recv_payload(client)
                    layout["history"].update(make_trade_history(payload["trade_history"][-10:]))

                    client.send(json.dumps({
                        "message_type": "instrument_orderbook_summary",
                        "symbol": "STI.",
                    }).encode('ascii'))
                    payload = recv_payload(client)
                    layout["summary"].update(make_summary(payload))

                    client.send(json.dumps({
                        "message_type": "instrument_orderbook",
                        "symbol": "STI.",
                    }).encode('ascii'))
                    payload = recv_payload(client)

                    buys_book = payload["buy_book"]
                    sells_book = payload["sell_book"]
                    layout["buys"].update(make_order_table(buys_book, direction="BUY", title="Buy Book"))
                    layout["sells"].update(make_order_table(sells_book, direction="SELL", title="Sell Book"))

                client.shutdown(1)
                client.close()
                txid += 1
                sleep(0.1)

