from datetime import datetime

from backend.database import Base
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    password = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class FileProcessingHistory(Base):
    __tablename__ = "file_processing_history"

    id = Column(Integer, primary_key=True, index=True)
    object_path = Column(String, nullable=False, index=True)
    table_name = Column(String, nullable=False, index=True)
    ddl_statement = Column(Text, nullable=False)
    partition_values = Column(JSONB, nullable=True)
    num_columns = Column(Integer, nullable=True)
    num_rows = Column(Integer, nullable=True)
    status = Column(String, nullable=False, index=True)  # 'success' or 'failed'
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
