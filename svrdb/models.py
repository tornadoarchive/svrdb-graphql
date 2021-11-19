from sqlalchemy import (
    Column, Integer, String, DateTime, Float, ForeignKey, create_engine, Numeric
)
from datetime import datetime

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

db_url = 'mysql+pymysql://user:password@db:3306/spc'
engine = create_engine(db_url, echo=True, future=True)

Base = declarative_base()


class SPCEvent:
    id: int = Column(Integer, primary_key=True)
    datetime: datetime = Column(DateTime, index=True, nullable=False)
    state: str = Column(String(255), index=True, nullable=False)
    fatalities: int = Column(Integer, nullable=False)
    injuries: int = Column(Integer, nullable=False)
    # TODO: we need to change these to numeric, but must look for max values
    loss: float = Column(Float, nullable=False)
    closs: float = Column(Float, nullable=False)


class County(Base):
    __tablename__ = 'county'
    id: int = Column(Integer, primary_key=True)
    state: str = Column(String(255), nullable=False)
    state_fips: int = Column(Integer, nullable=False, index=False)
    county_fips: int = Column(Integer, nullable=False, index=False)
    county: str = Column(String(255), nullable=False)


class Hail(Base, SPCEvent):
    __tablename__ = 'hail'

    magnitude: float = Column(Numeric(4, 2), nullable=False, index=True)
    lat: float = Column(Numeric(4, 2), nullable=False, index=True)
    lon: float = Column(Numeric(5, 2), nullable=False, index=True)
    # a lot of records have missing county data
    county_id: int = Column(Integer, ForeignKey('county.id'), nullable=True)
    county: County = relationship('County')