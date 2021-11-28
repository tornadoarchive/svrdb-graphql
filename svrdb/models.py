from typing import List

from sqlalchemy import (
    Column, Integer, String, DateTime, Float, ForeignKey, create_engine, Numeric,
    Boolean, Table)
from datetime import datetime

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session, declarative_mixin, declared_attr

db_url = 'mysql+pymysql://user:password@db:3306/spc'
engine = create_engine(db_url, echo=True, future=True)

Base = declarative_base()


def get_session():
    return Session(bind=engine, future=True)


@declarative_mixin
class _SPCEvent:
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


@declarative_mixin
class _PointEvent(_SPCEvent):
    lat: float = Column(Numeric(4, 2), nullable=False, index=True)
    lon: float = Column(Numeric(5, 2), nullable=False, index=True)

    @declared_attr
    def county_id(cls) -> int:
        # a lot of records have missing county data
        return Column(Integer, ForeignKey('county.id'), nullable=True)

    @declared_attr
    def county(cls) -> County:
        return relationship('County')


class Hail(Base, _PointEvent):
    __tablename__ = 'hail'
    magnitude: float = Column(Numeric(4, 2), nullable=False, index=True)


class Wind(Base, _PointEvent):
    __tablename__ = 'wind'
    magnitude: float = Column(Integer, nullable=False, index=True)


@declarative_mixin
class _PathEvent(_SPCEvent):
    length: float = Column(Numeric(5, 2), nullable=False)
    width: float = Column(Numeric(6, 2), nullable=False)
    start_lat: float = Column(Numeric(4, 2), nullable=False, index=True)
    start_lon: float = Column(Numeric(5, 2), nullable=False, index=True)
    end_lat: float = Column(Numeric(4, 2), nullable=False, index=True)
    end_lon: float = Column(Numeric(5, 2), nullable=False, index=True)


class Tornado(Base, _PathEvent):
    __tablename__ = 'tornado'
    magnitude: float = Column(Integer, nullable=True, index=True)
    magnitude_unk: bool = Column(Boolean, nullable=False)

    segments = relationship('TornadoSegment')


tornado_segment_counties = Table('tornado_segment_county', Base.metadata,
    Column('tornado_segment_id', ForeignKey('tornado_segment.id')),
    Column('county_id', ForeignKey('county.id'))
)


class TornadoSegment(Base, _PathEvent):
    __tablename__ = 'tornado_segment'
    magnitude: float = Column(Integer, nullable=True, index=True)
    magnitude_unk: bool = Column(Boolean, nullable=False)

    tornado_id: int = Column(Integer, ForeignKey('tornado.id'))
    counties: List[County] = relationship('County', secondary=tornado_segment_counties)

