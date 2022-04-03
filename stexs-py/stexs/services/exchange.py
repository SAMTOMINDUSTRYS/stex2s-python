from stexs.domain import model
from stexs.domain.order import Order
from stexs.domain.broker import OrderScreeningException
from stexs.services.logger import log
from stexs.services import orderbook, matcher
import stexs.io.persistence as iop
from typing import List, Dict
import time
from dataclasses import asdict as dataclasses_asdict

#TODO This should probably get injected somewhere but this works for now
STOCK_UOW = iop.stock.MemoryStockUoW

def _default_stock_uow():
    return STOCK_UOW()

def add_stock(stock: model.Stock, uow=None):
    if not uow:
        uow = _default_stock_uow()
    with uow:
        uow.stocks.add(stock)
        uow.commit()

def list_stocks(uow=None):
    if not uow:
        uow = _default_stock_uow()
    with uow:
        return uow.stocks.list()


class Exchange:

    def __init__(self, *args, **kwargs):
        self.txid_set = set([]) # set = field(default_factory=set)
        self.stalls = {} # Dict[str, model.MarketStall] = field(default_factory = dict)
        self.brokers = {}

        # TODO Little hack for now
        self.stock_uow = _default_stock_uow

    def add_stocks(self, stocks: List[model.Stock]):
        for stock in stocks:
            add_stock(stock, uow=self.stock_uow())
            self.stalls[stock.symbol] = model.MarketStall(stock=stock)
            matcher.add_book(stock.symbol, reference_price=1)

    def list_stocks(self):
        return list_stocks(uow=self.stock_uow())

    def add_broker(self, broker):
        self.brokers[broker.code] = broker

    def update_users(self, buys, sells, executed=False, reference_price=None):
        # Emit buys and sells to brokers
        for broker in self.brokers:
            self.brokers[broker].update_users(buys, sells, executed=executed, reference_price=reference_price)

    def handle_order(self, msg):
        response = {}

        if msg["broker_id"] not in self.brokers:
            return {
                "response_type": "exception",
                "response_code": 404,
                "msg": "malformed broker",
            }
        user = self.brokers[msg["broker_id"]].get_user(msg["account_id"])
        if not user:
            return {
                "response_type": "exception",
                "response_code": 404,
                "msg": "unknown user",
            }

        # Coerce to float (but use some sort of money class)
        # TODO CRIT https://github.com/SAMTOMINDUSTRYS/stex2s-python/issues/2
        if msg["price"] is not None and msg["price"] != '':
            price = float(msg["price"])
        else:
            # Allow market orders with price of None
            price = None

        #TODO CRIT Order vol > 0
        order = Order(
            txid=msg["txid"], # TODO need a customer side and exchange side tx
            csid=msg["account_id"],
            side=msg["side"],
            symbol=msg["symbol"],
            price=price if price != float("inf") and price != float("-inf") else None,
            volume=msg["volume"],
            ts=int(time.time()),
        )
        with self.stock_uow() as uow:
            try:
                symbol = uow.stocks.get(order.symbol).symbol
            except AttributeError:
                return {
                    "response_type": "exception",
                    "response_code": 404,
                    "msg": "unknown symbol",
                }

        # Check this order can be completed before processing it
        # Good transaction isolation is going to be needed to ensure balance
        # and holdings stay positive in the event of concurrent order handlers
        try:
            self.brokers[msg["broker_id"]].validate_preorder(user, order, reference_price=self.stalls[order.symbol].last_price)
        except OrderScreeningException as e:
            log.debug(e)
            return {
                "response_type": "exception",
                "response_code": 77,
                "msg": str(e),
            }
        except Exception as e:
            log.debug(e)
            return {
                "response_type": "exception",
                "response_code": 70,
                "msg": str(e),
            }

        # Process order
        buys, sells = orderbook.add_order(order) # Add order to canonical order repo
        matcher.add_order(order) # Add order to lightweight matching engine
        self.update_users(buys, sells, reference_price=self.stalls[order.symbol].last_price)

        summary = orderbook.summarise_books_for_symbol(symbol)
        log.info("[bold green]BOOK[/] [b]%s[/] %s" % (symbol, str(summary)))

        # Repeat trading until no trades are left
        while True:
            # Need to handle the market tick async from messages but this will do for now
            proposed_trades = matcher.match_orderbook(symbol)
            if len(proposed_trades) == 0:
                break

            for trade in proposed_trades:
                buys, sells = orderbook.execute_trade(trade) # commit the Trade and close the orders
                # update client holdings and balances
                self.update_users(buys, sells, executed=True, reference_price=self.stalls[order.symbol].last_price)
                self.stalls[symbol].log_trade(trade)
                log.info(trade)

            summary = orderbook.summarise_books_for_symbol(symbol)
            log.info("[bold green]BOOK[/] [b]%s[/] %s" % (symbol, str(summary)))

        return {
            "order": dataclasses_asdict(order),
            "response_type": "new_order",
            "response_code": 0,
            "msg": "ok",
        }


    def clear_trade(self):
        pass

    def format_instrument_summary(self, stall):
        reply = {
            "opening_price": None,
            "closing_price": None,
            "min_price": str(stall.min_price) if stall.min_price else None, # TODO CRIT str
            "max_price": str(stall.max_price) if stall.max_price else None,
            "num_trades": stall.n_trades,
            "vol_trades": stall.v_trades,
            "name": stall.stock.name,
            "symbol": stall.stock.symbol,
            "last_trade_price": None,
            "last_trade_volume": None,
            "last_trade_ts": None,
        }
        if len(stall.order_history) > 0:
            last_trade = stall.order_history[-1]
            reply["last_trade_price"] = str(last_trade.avg_price) # TODO CRIT str
            reply["last_trade_volume"] = last_trade.volume
            reply["last_trade_ts"] = last_trade.ts
        return reply

    def get_trade_history(self, stall):
        return [dataclasses_asdict(order) for order in stall.order_history]

    def recv(self, msg):
        if "txid" in msg:
            if msg["txid"] in self.txid_set:
                return {
                    "response_type": "exception",
                    "response_code": 1,
                    "msg": "duplicate transaction",
                }
            else:
                # Idempotent txid
                self.txid_set.add(msg["txid"])

        if "sender_ts" in msg:
            if msg["sender_ts"] < (int(time.time()) - 60):
                return {
                    "response_type": "exception",
                    "response_code": 1,
                    "msg": "stale transaction",
                }

        if msg["message_type"] == "new_order":
            reply = self.handle_order(msg)

        elif msg["message_type"] == "list_stocks":
            reply = sorted(list(self.list_stocks())) # list to serialize

        elif msg["message_type"] == "instrument_summary":
            with self.stock_uow() as uow:
                ok = True
                try:
                    symbol = uow.stocks.get(msg["symbol"]).symbol
                except AttributeError:
                    reply = {
                        "response_type": "exception",
                        "response_code": 404,
                        "msg": "unknown symbol",
                    }
                    ok = False

                if ok:
                    reply = self.format_instrument_summary(self.stalls[symbol])
                    if reply:
                        reply.update({
                            "response_type": "instrument_summary",
                            "response_code": 0,
                            "msg": "ok",
                        })
                    log.critical(reply)

        elif msg["message_type"] == "instrument_trade_history":
            with self.stock_uow() as uow:
                ok = True
                try:
                    symbol = uow.stocks.get(msg["symbol"]).symbol
                except AttributeError:
                    reply = {
                        "response_type": "exception",
                        "response_code": 404,
                        "msg": "unknown symbol",
                    }
                    ok = False

                if ok:
                    reply = {
                        "response_type": "instrument_trade_history",
                        "response_code": 0,
                        "msg": "ok",
                        "symbol": symbol,
                        "trade_history": self.get_trade_history(self.stalls[symbol]),
                    }

        elif msg["message_type"] == "instrument_orderbook_summary":
            with self.stock_uow() as uow:
                ok = True
                try:
                    symbol = uow.stocks.get(msg["symbol"]).symbol
                except AttributeError:
                    reply = {
                        "response_type": "exception",
                        "response_code": 404,
                        "msg": "unknown symbol",
                    }
                    ok = False

                if ok:
                    summary = orderbook.summarise_books_for_symbol(symbol, reference_price=self.stalls[symbol].last_price)
                    reply = {
                        "response_type": "instrument_orderbook_summary",
                        "response_code": 0,
                        "msg": "ok",
                        "symbol": symbol,
                        "depth_buys": summary["dbuys"],
                        "depth_sells": summary["dsells"],
                        "top_num_buys": summary["nbuys"],
                        "top_num_sells": summary["nsells"],
                        "top_vol_buys": summary["vbuys"],
                        "top_vol_sells": summary["vsells"],
                        "current_buy": str(summary["buy"]), #TODO CRIT str
                        "current_sell": str(summary["sell"]),
                    }

        elif msg["message_type"] == "instrument_orderbook":
            with self.stock_uow() as uow:
                ok = True
                try:
                    symbol = uow.stocks.get(msg["symbol"]).symbol
                except AttributeError:
                    reply = {
                        "response_type": "exception",
                        "response_code": 404,
                        "msg": "unknown symbol",
                    }
                    ok = False

                if ok:
                    order_books = orderbook.get_serialised_order_books_for_symbol(symbol, n=10)
                    reply = {
                        "response_type": "instrument_orderbook",
                        "response_code": 0,
                        "msg": "ok",
                        "symbol": symbol,
                        "buy_book": order_books["buy_book"],
                        "sell_book": order_books["sell_book"],
                    }

        else:
            reply = {
                "response_type": "exception",
                "response_code": 1,
                "msg": "unknown message_type",
            }

        return reply
