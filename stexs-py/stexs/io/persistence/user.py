from stexs.io.persistence.base import AbstractUoW, GenericMemoryRepository
from stexs.services.logger import log
from stexs.domain import model

class MemoryClientUoW(AbstractUoW):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.users = GenericMemoryRepository(prefix="clients")

    def commit(self):
        self.users._commit()
        for user_id, user in self.users._staged_objects.items():
            if self.users._versions[user_id] == 1:
                log.info("[bold red]USER[/] Registered [b]%s[/] %s" % (user.csid, user.name))
        self.committed = True

    def rollback(self):
        pass

