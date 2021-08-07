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
            cls.__engine = create_engine(config.get_sqlite_url())
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

# Import tables for mapping
# TODO No idea what the nice way to do this is?
from . import stocks
