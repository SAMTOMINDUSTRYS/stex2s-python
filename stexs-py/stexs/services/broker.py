from stexs.domain.broker import (
    Client,
    OrderScreeningException,
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

    def get_user(self, csid: str, uow=None):
        if not uow:
            uow = self.user_uow()

        with uow:
            return uow.users.get(csid)

    def add_users(self, clients: List[Client], uow=None):
        if not uow:
            uow = self.user_uow()

        with uow:
            for client in clients:
                uow.users.add(client)
            uow.commit()

    def validate_preorder(self, user, order, reference_price=None):
        # Replace the order.price with reference_price if the user is submitting a market order
        try:
            order_price = order.price
            if not order_price:
                order_price = reference_price
            user.screen_order(order.side, order.symbol, order_price, order.volume)
        except OrderScreeningException as e:
            raise e
        return True

    def update_users(self, buy_orders, sell_orders, executed=False, uow=None, reference_price=None):
        if not uow:
            uow = self.user_uow()

        with uow:
            if not executed:
                for order in buy_orders:
                    # order.price may be None before execution
                    # Use the reference_price
                    order_price = order.price
                    if not order_price:
                        order_price = reference_price

                    self.adjust_balance(order.csid, order_price * order.volume * -1, uow=uow)
                for order in sell_orders:
                    self.adjust_holding(order.csid, order.symbol, order.volume * -1, uow=uow)
            else:
                for order in buy_orders:
                    self.adjust_holding(order.csid, order.symbol, order.volume, uow=uow)
                for order in sell_orders:
                    # CRIT TODO Check this works with splits
                    self.adjust_balance(order.csid, order.price * order.volume, uow=uow)
            uow.commit()

    def adjust_balance(self, csid, adjust_balance, uow=None):
        if not uow:
            uow = self.user_uow()

        with uow:
            user = uow.users.get(csid)
            user.adjust_balance(adjust_balance)

        log.info("[bold magenta]USER[/] [b]CASH[/] %s=%.3f" % (csid, user.balance))

    def adjust_holding(self, csid, symbol, adjust_qty, uow=None):
        if not uow:
            uow = self.user_uow()

        # TODO Validate holdings (broker would probably cache the stock list)
        #stock_list = self.exchange.list_stocks()
        #if not symbol in stock_list:
        #    raise Exception("Unknown symbol")

        with uow:
            user = uow.users.get(csid)
            user.adjust_holding(symbol, adjust_qty)

        log.info("[bold magenta]USER[/] [b]HOLD[/] %s:%s=%.3f" % (csid, symbol, user.holdings[symbol]))

