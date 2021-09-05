from stexs.domain import model
from stexs.domain.broker import (
    InsufficientBalanceException,
    InsufficientHoldingException,
)
from stexs.services.logger import log
import stexs.io.persistence as iop
from typing import List, Dict

class Broker:

    def __init__(self, code, name, *args, **kwargs):
        # TODO Little hack for now
        self.code = code
        self.name = name
        self.user_uow = iop.user.MemoryClientUoW

    def get_user(self, csid: str):
        with self.user_uow() as uow:
            return uow.users.get(csid)

    def add_users(self, clients: List[model.Client]):
        with self.user_uow() as uow:
            for client in clients:
                uow.users.add(client)
            uow.commit()

    def validate_preorder(self, user, order):
        if order.side == "BUY":
            if order.price * order.volume > user.balance:
                raise InsufficientBalanceException("Insufficient balance")
        elif order.side == "SELL":
            if order.symbol not in user.holdings:
                raise InsufficientHoldingException("Insufficient holding")
            else:
                if user.holdings[order.symbol] < order.volume:
                    raise InsufficientHoldingException("Insufficient holding")

    def update_users(self, buy_orders, sell_orders, executed=False):
        if not executed:
            for order in buy_orders:
                self.adjust_balance(order.csid, order.price * order.volume * -1)
            for order in sell_orders:
                self.adjust_holding(order.csid, order.symbol, order.volume * -1)
        else:
            for order in buy_orders:
                self.adjust_holding(order.csid, order.symbol, order.volume)
            for order in sell_orders:
                # CRIT TODO Check this works with splits
                self.adjust_balance(order.csid, order.price * order.volume)

    def adjust_balance(self, csid, adjust_balance):
        with self.user_uow() as uow:
            user = uow.users.get(csid)
            user.balance += adjust_balance
            uow.commit()

        log.info("[bold magenta]USER[/] [b]CASH[/] %s=%.3f" % (csid, user.balance))

    def adjust_holding(self, csid, symbol, adjust_qty):
        # TODO Validate holdings (broker would probably cache the stock list)
        #stock_list = self.exchange.list_stocks()
        #if not symbol in stock_list:
        #    raise Exception("Unknown symbol")

        with self.user_uow() as uow:
            user = uow.users.get(csid)
            if symbol not in user.holdings:
                user.holdings[symbol] = 0
            user.holdings[symbol] += adjust_qty
            uow.commit()

        log.info("[bold magenta]USER[/] [b]HOLD[/] %s:%s=%.3f" % (csid, symbol, user.holdings[symbol]))

