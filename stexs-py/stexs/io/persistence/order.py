from stexs.io.persistence.base import AbstractUoW, GenericMemoryRepository

class OrderMemoryUoW(AbstractUoW):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orders = GenericMemoryRepository(prefix="orders")

    def commit(self):
        self.orders._commit()
        self.committed = True

    def rollback(self):
        pass

