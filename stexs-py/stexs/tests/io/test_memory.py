import pytest
from dataclasses import dataclass
from stexs.io.persistence.base import GenericMemoryRepository

@dataclass
class StexRecord:
    stexid: str

@pytest.fixture
def repo():
    repo = GenericMemoryRepository(prefix="hoot")
    repo.store.clear()
    repo.store._clear()
    return repo


def test_memory_add(repo):

    assert len(repo.store._staged_objects) == 0

    obj = StexRecord(stexid='1')
    obj_id = repo.get_obj_id('1')
    repo.add(obj)

    assert len(repo.store._staged_objects) == 1
    assert repo.store._staged_versions[obj_id] == 0


def test_memory_get(repo):

    assert len(repo.store._staged_objects) == 0
    assert len(repo.store._staged_versions) == 0

    obj = StexRecord(stexid='1')
    obj_id = repo.get_obj_id('1')

    _objects = {
        "hoot": {
            '1': obj,
        }
    }
    _versions = {
        "hoot>1": 8,
    }

    repo.store._store._objects = _objects
    repo.store._store._versions = _versions

    # Returned object matches the forcibly added object
    assert repo.get('1') == obj
    assert len(repo.store._staged_objects) == 1
    assert repo.store._staged_objects[obj_id] == obj

    # Returned object is a copy of the real deal
    assert repo.store._staged_objects[obj_id] == repo.store._store._objects["hoot"]['1']
    assert id(repo.store._staged_objects[obj_id]) != id(repo.store._store._objects["hoot"]['1'])

    assert repo.store._staged_versions[obj_id] == 8


def test_memory_get_none(repo):
    obj = repo.get('1')
    assert obj is None
    assert len(repo.store._staged_objects) == 0


def test_memory_list(repo):
    _objects = {
        "hoot": {
            '1': StexRecord(stexid='1'),
            '8': StexRecord(stexid='8'),
            '2': StexRecord(stexid='2'),
        }
    }
    _versions = {
        "hoot>1": 1,
        "hoot>2": 2,
        "hoot>8": 3,
    }
    repo.store._store._objects = _objects
    repo.store._store._versions = _versions

    assert repo.list() == set(['1', '2', '8'])


def test_memory_commit(repo):

    obj = StexRecord(stexid='1')
    obj_id = repo.get_obj_id('1')

    _staged_objects = {
        obj_id: obj
    }
    _staged_versions = {
        obj_id: 0
    }

    repo.store._staged_objects = _staged_objects
    repo.store._staged_versions = _staged_versions

    repo._commit()

    # Check stages are clear
    assert len(repo.store._staged_objects) == 0
    assert len(repo.store._staged_versions) == 0

    # Check _object has appeared
    assert len(repo.store._store._objects) == 1
    assert len(repo.store._store._versions) == 1

    # Check the obj itself
    assert repo.store._store._objects["hoot"]['1'] == obj
    assert repo.store._store._versions[obj_id] == 1


def test_memory_concurrent_commit(repo):
    obj_id = repo.get_obj_id('1')

    _objects = {
        "hoot": {
            '1': StexRecord(stexid='1')
        },
    }
    _versions = {
        obj_id: 2
    }

    _staged_objects = {
        obj_id: StexRecord(stexid='2')
    }
    _staged_versions = {
        obj_id: 1
    }

    repo.store._store._objects = _objects
    repo.store._store._versions = _versions
    repo.store._staged_objects = _staged_objects
    repo.store._staged_versions = _staged_versions

    with pytest.raises(Exception, match="Concurrent commit rejected"):
        repo._commit()


def test_stage_clear(repo):

    obj = StexRecord(stexid='1')
    obj_id = repo.get_obj_id('1')
    repo.add(obj)

    assert len(repo.store._staged_objects) == 1
    assert len(repo.store._staged_versions) == 1

    repo.store.clear()
    assert len(repo.store._staged_objects) == 0
    assert len(repo.store._staged_versions) == 0
