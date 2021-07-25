# Allegedly a Repository is for abstracting collections of Domain objects
# UoW and Repo are tightly coupled ("collaborators")

import abc
from stexs.domain import model
import copy

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

    # Keep tabs on object version checked out by `get` and ensure it is matched
    # when committing as a means to detect concurrent commits, effectively provides
    # compare-and-set (could still be caught in a race condition)
    _versions = {}

    def __init__(self, *args, **kwargs):
        self._staged_users = {}
        self._staged_versions = {}

    def add(self, user: model.Client):
        self._staged_users[user.csid] = user

    def get(self, csid: str):
        # Providing read committed isolation as only committed data can be
        # read from _users and _staged_users cannot be read by other UoW
        # Does not guard against read skew and the like...
        self._staged_users[csid] = copy.deepcopy(self._users.get(csid))
        self._staged_versions[csid] = self._versions[csid]
        return self._staged_users[csid]

    def commit(self):
        for csid, user in self._staged_users.items():
            if not self._users.get(csid):
                log.info("[bold red]USER[/] Registered [b]%s[/] %s" % (user.csid, user.name))
                self._versions[csid] = 0
            else:
                if self._versions[csid] != self._staged_versions[csid]:
                    raise Exception("Concurrent commit rejected")
            self._users[csid] = user
            self._versions[csid] += 1


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

