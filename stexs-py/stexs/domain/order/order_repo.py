import abc
from .order import Order

class OrderRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, order: Order):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, txid: str):
        raise NotImplementedError

    @abc.abstractmethod
    def get_buy_book_for_symbol(self, symbol: str):
        raise NotImplementedError

    @abc.abstractmethod
    def get_sell_book_for_symbol(self, symbol: str):
        raise NotImplementedError

