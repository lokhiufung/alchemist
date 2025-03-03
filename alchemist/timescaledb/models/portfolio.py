from sqlalchemy import Column, String, Float, Integer, DateTime

from alchemist.timescaledb.models.base import Base

class Portfolio(Base):
    __tablename__ = "portfolios"

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_name = Column(String, nullable=False)
    ts = Column(DateTime, nullable=False)
    balance = Column(Float, nullable=False)
    margin_used = Column(Float, nullable=False)
    total_value = Column(Float, nullable=False)