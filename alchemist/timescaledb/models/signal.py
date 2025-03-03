from sqlalchemy import Column, String, Float, Integer, DateTime

from alchemist.timescaledb.models.base import Base


class Signal(Base):
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_name = Column(String, nullable=False)
    ts = Column(DateTime, nullable=False)
    signal_name = Column(String, nullable=False) 
    value = Column(Float, nullable=False)  # i.e the value ofthe signal