from stexs.io.persistence.base import AbstractUoW, GenericMemoryRepository
from stexs.services.logger import log
from stexs.domain import model

class MemoryClientUoW(AbstractUoW):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.users = GenericMemoryRepository(prefix="clients")

    def commit(self):
        for user_id, version in self.users.store._staged_versions.items():
            if version == 0:
                user = self.users.store._staged_objects[user_id]
                log.info("[bold red]USER[/] Registered [b]%s[/] %s" % (user.csid, user.name))
        self.users._commit()

    def rollback(self):
        pass

