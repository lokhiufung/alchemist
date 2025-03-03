from sqlalchemy import Column, String, Float, Integer, DateTime, Enum

from alchemist.timescaledb.models.base import Base
from alchemist.enums import TimeInForceEnum, SideEnum, OrderStatusEnum, OrderTypeEnum

class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_name = Column(String, nullable=False)
    ts = Column(DateTime, nullable=False)
    order_id = Column(String, nullable=False)
    order_type = Column(Enum(OrderTypeEnum), nullable=False)
    time_in_force = Column(Enum(TimeInForceEnum), nullable=False)
    product = Column(String, nullable=False)
    status = Column(Enum(OrderStatusEnum), nullable=False)  # Example: "PENDING", "FILLED"
    size = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    side = Column(Enum(SideEnum), nullable=False)  # Example: "BUY", "SELL"