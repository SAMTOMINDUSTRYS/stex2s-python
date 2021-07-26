from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime, Table, MetaData, create_engine
from sqlalchemy.orm import registry, sessionmaker
from sqlalchemy.exc import NoResultFound

from stexs import config
from stexs.domain import model

# TODO Does this belong here?
# TODO Is this overkill?
class StexSqliteSessionFactory():
    __engine = None

    @classmethod
    def get_session(cls):
        # TODO Probably do this once in bootstrap somewhere?
        if not cls.__engine:
            cls.__engine = create_engine(config.SQLITE_URL)
            metadata.create_all(cls.__engine) # TODO Creating tables probably not in scope of session making...
        return sessionmaker(
                autocommit=False,
                autoflush=False,
                expire_on_commit=False, # CRIT TODO Currently allows objects to persist outside UoW
                bind=cls.__engine)()


# Using a imperative "classical" mapping here, apparently declarative mapping is the hip new business
# https://docs.sqlalchemy.org/en/14/orm/mapping_styles.html#imperative-mapping-with-dataclasses-and-attrs
metadata = MetaData()
mapper_registry = registry()

stocks = Table(
    'stocks', metadata,
    Column('symbol', String, primary_key=True),
    Column('name', String),
)

# Map Table to dataclass
# Cosmic Python recommends you implement a start_mappers function to turn these on and off
# I haven't done this as I am yet to work out why it matters...
stocks_mapper = mapper_registry.map_imperatively(model.Stock, stocks)

