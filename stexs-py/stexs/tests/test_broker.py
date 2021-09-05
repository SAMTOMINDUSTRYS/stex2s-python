import pytest

from stexs.domain.broker import Client
from stexs.services.broker import Broker
import stexs.io.persistence as iop

###############################################################################
# Domain
# There should be no Service, Repo or UoW funny business in here

#TODO CRIT not sure what to do about negative holdings yet

@pytest.fixture
def client():
    return Client(csid="1", name="Sam", balance=0, holdings={"STI.": 100})

def test_domain_adjust_balance_up(client):
    current_balance = client.balance
    adjustment = 100
    client.adjust_balance(adjustment)
    assert client.balance == (current_balance + adjustment)

def test_domain_adjust_balance_down(client):
    current_balance = client.balance
    adjustment = -100
    client.adjust_balance(adjustment)
    assert client.balance == (current_balance + adjustment)

def test_domain_adjust_holding_new(client):
    assert "ELAN" not in client.holdings
    adjustment = 100
    client.adjust_holding("ELAN", 100)
    assert client.holdings["ELAN"] == adjustment

def test_domain_adjust_holding_existing_up(client):
    current_holding = client.holdings["STI."]
    adjustment = 100
    client.adjust_holding("STI.", adjustment)
    assert client.holdings["STI."] == (current_holding + adjustment)

def test_domain_adjust_holding_existing_down(client):
    current_holding = client.holdings["STI."]
    adjustment = -100
    client.adjust_holding("STI.", adjustment)
    assert client.holdings["STI."] == (current_holding + adjustment)

###############################################################################
# Service

@pytest.fixture
def uow():
    repo = iop.base.GenericMemoryRepository(prefix="clients")
    repo.store.clear()
    repo.store._clear()
    repo.store._store._objects["clients"] = {}
    repo.store._store._objects["clients"]["1"] = Client(csid="1", name="Sam", balance=100, holdings={"STI.": 100})
    repo.store._store._versions["clients>1"] = 1
    return iop.user.MemoryClientUoW # uses the GMR

@pytest.fixture
def broker(uow):
    broker = Broker("MAGENTA", "Magenta Holdings Corporation")
    broker.user_uow = uow
    return broker

def test_service_get_unknown_user(broker, uow):
    assert broker.get_user("8", uow=uow()) is None

def test_service_get_known_user(broker, uow):
    user = broker.get_user("1", uow=uow())
    assert user.csid == "1"
    assert user.name == "Sam"
    assert user.balance == 100
    assert user.holdings == {"STI.": 100}

def test_add_and_get_user(broker, uow):
    broker.add_users([
        Client(csid="2", name="Tom"),
        Client(csid="3", name="Sammy"),
        Client(csid="4", name="Daisy"),
    ], uow=uow())

    with uow() as test_uow:
        assert len(test_uow.users.store._store._objects["clients"]) == 4

    with uow() as test_uow:
        sam = test_uow.users.get("2")
        assert sam.csid == "2"
        assert sam.name == "Tom"

# Cheat function that calls adjust_balance and adjust_holding
def test_update_users():
    pass


def test_service_adjust_balance(broker, uow):
    with uow() as test_uow:
        client = test_uow.users.get("1")
        balance = client.balance
        broker.adjust_balance("1", 100, uow=test_uow)
        assert client.balance == 200

    with uow() as test_uow:
        client = test_uow.users.get("1")
        assert client.balance == 200

    with uow() as test_uow:
        client = test_uow.users.get("1")
        balance = client.balance
        broker.adjust_balance("1", -100, uow=test_uow)
        assert client.balance == 100

    with uow() as test_uow:
        client = test_uow.users.get("1")
        assert client.balance == 100


def test_service_adjust_balance(broker, uow):
    with uow() as test_uow:
        client = test_uow.users.get("1")
        holding = client.holdings["STI."]
        broker.adjust_holding("1", "STI.", 100, uow=test_uow)
        assert client.holdings["STI."] == 200

    with uow() as test_uow:
        client = test_uow.users.get("1")
        assert client.holdings["STI."] == 200

    with uow() as test_uow:
        client = test_uow.users.get("1")
        holding = client.holdings["STI."]
        broker.adjust_holding("1", "STI.", -100, uow=test_uow)
        assert client.holdings["STI."] == 100

    with uow() as test_uow:
        client = test_uow.users.get("1")
        assert client.holdings["STI."] == 100

