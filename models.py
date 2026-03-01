import uuid
from sqlalchemy import Column, String, DateTime, Float, BigInteger, Index, Boolean
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from database import Base
from sqlalchemy import UniqueConstraint

class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String, unique=True, nullable=False, index=True)
    role = Column(String, nullable=False, default="viewer")
    plan_type = Column(String, nullable=False, default="free")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class MarketCandle(Base):
    __tablename__ = "market_candles"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    symbol = Column(String, nullable=False, index=True)
    timeframe = Column(String, nullable=False, index=True)  # "1d", "2h", etc
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)

    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
    UniqueConstraint("symbol", "timeframe", "timestamp", name="uq_symbol_tf_ts"),
    Index("idx_symbol_timeframe_timestamp", "symbol", "timeframe", "timestamp"),
)