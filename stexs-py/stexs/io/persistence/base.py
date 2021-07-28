# Allegedly a Repository is for abstracting collections of Domain objects
# UoW and Repo are tightly coupled ("collaborators")

import abc
from stexs.domain import model
import copy

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

class GenericMemoryRepository(AbstractRepository):
    # TODO This seems to act more like a UoW than the UoW does !
    # CRIT TODO The refs to self._object only happen to work as the struct id is shared
    #           but should use GenericMemoryRepository._objects to prevent shadowing

    # Class variable allows us to mock a crap memory DB
    # as values will persist across instances of MemoryClientRepository
    _objects = {}

    # Keep tabs on object version checked out by `get` and ensure it is matched
    # when committing as a means to detect concurrent commits, effectively provides
    # compare-and-set (could still be caught in a race condition)
    _versions = {}

    def __init__(self, prefix, *args, **kwargs):
        # Prefix will avoid clashes between _objects with the same IDs across
        # different namespaces. ie. Everything using the GenericMemoryRepository is
        # using the same _objects dictionary. This will not be particularly
        # pretty but will work for now!
        self.prefix = prefix
        self._staged_objects = {}
        self._staged_versions = {}

    def get_obj_id(self, obj_id):
        return "%s-%s" % (self.prefix, obj_id)

    def add(self, obj):
        obj_id = self.get_obj_id(obj.stexid)
        self._staged_objects[obj_id] = obj

    def get(self, obj_id: str):
        obj_id = self.get_obj_id(obj_id)

        # Providing read committed isolation as only committed data can be
        # read from _objects and _staged_objects cannot be read by other UoW
        # Does not guard against read skew and the like...
        self._staged_objects[obj_id] = copy.deepcopy(self._objects.get(obj_id))
        self._staged_versions[obj_id] = self._versions[obj_id]
        return self._staged_objects[obj_id]

    def _commit(self):
        for obj_id, obj in self._staged_objects.items():
            if not self._objects.get(obj_id):
                self._versions[obj_id] = 0
            else:
                if self._versions[obj_id] != self._staged_versions[obj_id]:
                    raise Exception("Concurrent commit rejected")
            self._objects[obj_id] = obj
            self._versions[obj_id] += 1

        # Reset staged objects?
        # CRIT TODO Could break commit - edit - commit workflow
        self._staged_objects = {}


###############################################################################

from sqlalchemy.exc import NoResultFound # Could wrap this?
from stexs.adapters.stex_sqlite import StexSqliteSessionFactory

class GenericSqliteRepository(AbstractRepository):

    def __init__(self, session, *args, **kwargs):
        self.session = session

    def add(self, obj):
        self.session.add(obj)

    @abc.abstractmethod
    def _get(self, obj_id: str):
        raise NotImplementedError

    def get(self, obj_id: str):
        try:
            return self._get(obj_id)
        except NoResultFound:
            return None

class GenericSqliteUoW(AbstractUoW):
    def __enter__(self, session_factory=StexSqliteSessionFactory):
        self.session = session_factory.get_session()
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)
        self.session.close()

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()


###############################################################################
