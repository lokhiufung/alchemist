from sqlalchemy import Column, String, Float, Integer, DateTime, Enum

from alchemist.timescaledb.models.base import Base
from alchemist.enums import SideEnum


class Position(Base):
    __tablename__ = "positions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_name = Column(String, nullable=False)
    ts = Column(DateTime, nullable=False)
    product = Column(String, nullable=False)
    size = Column(Float, nullable=False)
    side = Column(Enum(SideEnum), nullable=False)
    avg_price = Column(Float, nullable=False)
    realized_pnl = Column(Float, nullable=False)  # Realized profit and loss
    unrealized_pnl = Column(Float, nullable=False)  # Unrealized profit and loss