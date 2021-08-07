from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime, Table, MetaData, create_engine
from stexs.domain import model
from stexs.adapters.stex_sqlite import database

stocks = Table(
    'stocks', database.metadata,
    Column('symbol', String, primary_key=True),
    Column('name', String),
)

# Map Table to dataclass
stocks_mapper = database.mapper_registry.map_imperatively(model.Stock, stocks)
