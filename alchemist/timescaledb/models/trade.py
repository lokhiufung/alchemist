import enum

from sqlalchemy import Column, String, Float, Integer, DateTime, Enum

from alchemist.enums import *
from alchemist.timescaledb.models.base import Base


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_name = Column(String, nullable=False)
    ts = Column(DateTime, nullable=False)
    order_id = Column(String, nullable=False)
    product = Column(String, nullable=False)
    filled_price = Column(Float, nullable=False)
    filled_size = Column(Float, nullable=False)
    side = Column(Enum(SideEnum), nullable=False)  # Example: "LONG" or "SHORT"