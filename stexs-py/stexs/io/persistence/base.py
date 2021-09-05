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
from stexs.services.logger import log

class GenericVersionedMemoryDict():

    def __init__(self, *args, **kwargs):
        self._objects = {}

        # Keep tabs on object version checked out by `get` and ensure it is matched
        # when committing as a means to detect concurrent commits, effectively provides
        # compare-and-set (could still be caught in a race condition)
        self._versions = {}

    def _xget(self, key_path):
        node = self._objects
        for key in key_path.split('>'):
            node = node.get(key)
            if node is None:
                 return {}
        return node

    def _get(self, key_path):
        # Providing read committed isolation as only committed data can be
        # read from _objects and _staged_objects cannot be read by other UoW
        # Does not guard against read skew and the like...
        if key_path not in self._versions:
            return None, None

        node = self._xget(key_path)
        return node, self._versions[key_path]

    def _add(self, key_path, obj):
        node = self._objects
        path = key_path.split('>')
        for i, key in enumerate(path):
            if i == len(path)-1:
                # Insert / update
                node[key] = obj
            else:
                if key not in node:
                    node[key] = {}
                node = node[key]

    def _check(self, key_path):
        node = self._objects
        for key in key_path.split('>'):
            node = node.get(key)
            if not node:
                 break
        return node is not None

class GenericVersionedMemoryDictWrapper():

    def __init__(self, *args, **kwargs):
        self._store = GenericVersionedMemoryDict()
        self._staged_objects = {}
        self._staged_versions = {}

    def _check(self, key_path):
        return self._store._check(key_path)

    def _add(self, key_path, obj):
        if key_path not in self._staged_objects:
            self._staged_versions[key_path] = 0
        self._staged_objects[key_path] = obj

    def _get(self, key_path):
        # Providing read committed isolation as only committed data can be
        # read from _objects and _staged_objects cannot be read by other UoW
        # Does not guard against read skew and the like...
        if key_path in self._staged_objects:
            return self._staged_objects[key_path]
        else:
            obj, version = self._store._get(key_path)

            if obj:
                # Copy object to _staged_objects
                self._staged_objects[key_path] = copy.deepcopy(obj)

                # Cache checked-out version
                self._staged_versions[key_path] = version

                return self._staged_objects[key_path]

    def _list(self, key_path):
        return self._store._xget(key_path).keys()

    def _commit(self):
        commits = {}

        for obj_key_path, obj in self._staged_objects.items():
            if not self._check(obj_key_path):
                # New
                self._store._versions[obj_key_path] = 0
            else:
                # Check version for concurrent transaction
                if self._store._versions[obj_key_path] != self._staged_versions[obj_key_path]:
                    raise Exception("Concurrent commit rejected")

            # Commit to store and increment version
            self._store._add(obj_key_path, obj)
            self._store._versions[obj_key_path] += 1

            commits[obj_key_path] = self._store._versions[obj_key_path]

        # Reset staged objects?
        # CRIT TODO Could break commit - edit - commit workflow
        self.clear()

        return commits

    def clear(self):
        self._staged_objects.clear()
        self._staged_versions.clear()

    def clear_prefix(self, prefix):
        to_del = ["%s>%s" % (prefix, str(kid)) for kid in self._store._objects[prefix]]
        for k in to_del:
            del self._store._versions[k]

            if k in self._staged_objects:
                del self._staged_objects[k]
                del self._staged_versions[k]
        del self._store._objects[prefix]


    def _clear(self):
        self._store._objects.clear()
        self._store._versions.clear()


class GenericMemoryRepository(AbstractRepository):
    store = GenericVersionedMemoryDictWrapper()

    def __init__(self, prefix, *args, **kwargs):
        self.prefix = prefix

    def get_obj_id(self, obj_id):
        return "%s>%s" % (self.prefix, obj_id)

    def add(self, obj):
        obj_id = self.get_obj_id(obj.stexid)
        self.store._add(obj_id, obj)

    def get(self, obj_id: str):
        obj_id = self.get_obj_id(obj_id)
        return self.store._get(obj_id)

    def clear(self):
        self.store.clear_prefix(self.prefix)

    def list(self):
        return set(self.store._list(self.prefix))

    def _commit(self):
        self.store._commit()


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
