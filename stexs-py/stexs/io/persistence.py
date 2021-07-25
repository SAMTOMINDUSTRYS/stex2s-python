# Allegedly a Repository is for abstracting collections of Domain objects
# UoW and Repo are tightly coupled ("collaborators")

import abc
from stexs.domain import model

from stexs.services.logger import log

class AbstractRepository(abc.ABC):

    @abc.abstractmethod
    def add(self, thing):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, thing_id):
        raise NotImplementedError

# Arch Patterns w/Python suggests the UoW live as a service of its own but seems
# they should live much closer together given the mapping from Repo to UoW is
# essentially 1:1
class AbstractUoW(abc.ABC):
    def __init__(self, *args, **kwargs):
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.rollback()

    @abc.abstractmethod
    def commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self):
        raise NotImplementedError

###############################################################################

class AbstractClientRepository(AbstractRepository):
    pass

class AbstractClientUoW(AbstractUoW):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.users = AbstractClientRepository()

class MemoryClientRepository(AbstractClientRepository):

    # Class variable allows us to mock a crap memory DB
    # as values will persist across instances of MemoryClientRepository
    _users = {}

    def __init__(self, *args, **kwargs):
        self._staged_users = {}

    def add(self, user: model.Client):
        self._staged_users[user.csid] = user

    def get(self, csid: str):
        return self._users.get(user.csid)

    def commit(self):
        for csid, user in self._staged_users.items():
            self._users[csid] = user
            log.info("[bold red]USER[/] Registered [b]%s[/] %s" % (user.csid, user.name))

class MemoryClientUoW(AbstractUoW):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.users = MemoryClientRepository()

    def commit(self):
        self.users.commit()
        self.committed = True

    def rollback(self):
        pass

###############################################################################

