from datetime import datetime
from enum import Enum

from sqlalchemy import (
    Column, Integer, String, DateTime, Float, ForeignKey, create_engine, Numeric,
    Boolean
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session, declarative_mixin, declared_attr
from decouple import config


class DBConfig(Enum):
    DRIVER = config('MYSQL_DRIVER', default='pymysql')
    IMAGE = config('DB_CONTAINER', default='db')
    USER = config('MYSQL_USER')
    PASSWORD = config('MYSQL_PASSWORD')
    DB = config('MYSQL_DATABASE')
    PORT = config('MYSQL_PORT', default=3306, cast=int)

    @classmethod
    def mysql_conn_str(cls):
        return f'mysql+{cls.DRIVER.value}://' \
               f'{cls.USER.value}:{cls.PASSWORD.value}@{cls.IMAGE.value}:{cls.PORT.value}/{cls.DB.value}'


class Tables(Enum):
    TORNADO = config('TORNADO_TABLE', default='tornado')
    TORNADO_SEGMENT = config('TORNADO_SEGMENT_TABLE', default='tornado_segment')
    TORNADO_SEGMENT_COUNTY = config('TORNADO_SEGMENT_COUNTY_TABLE', default='tornado_segment_county')
    COUNTY = config('COUNTY_TABLE', default='county')
    HAIL = config('HAIL_TABLE', default='hail')
    WIND = config('WIND_TABLE', default='wind')


db_url = DBConfig.mysql_conn_str()
engine = create_engine(db_url, echo=True, future=True)

Base = declarative_base()


def get_session():
    return Session(bind=engine, future=True)


@declarative_mixin
class _Event:
    id: int = Column(Integer, primary_key=True)
    datetime: datetime = Column(DateTime, index=True, nullable=False)
    state: str = Column(String(255), index=True, nullable=False)
    fatalities: int = Column(Integer, nullable=False)
    injuries: int = Column(Integer, nullable=False)
    # TODO: we need to change these to numeric, but must look for max values
    loss: float = Column(Float, nullable=False)
    closs: float = Column(Float, nullable=False)
    country: str = Column(String(255), index=True, nullable=False, default='US')


class County(Base):
    __tablename__ = 'county'
    id: int = Column(Integer, primary_key=True)
    state: str = Column(String(255), nullable=False)
    state_fips: int = Column(Integer, nullable=False, index=False)
    county_fips: int = Column(Integer, nullable=False, index=False)
    county: str = Column(String(255), nullable=False)


@declarative_mixin
class _PointEvent(_Event):
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
class _PathEvent(_Event):
    length: float = Column(Numeric(5, 2), nullable=False)
    width: float = Column(Numeric(6, 2), nullable=False)
    start_lat: float = Column(Numeric(6, 4), nullable=False, index=True)
    start_lon: float = Column(Numeric(7, 4), nullable=False, index=True)
    end_lat: float = Column(Numeric(6, 4), nullable=False)
    end_lon: float = Column(Numeric(7, 4), nullable=False)


class Tornado(Base, _PathEvent):
    __tablename__ = 'tornado'
    magnitude: float = Column(Integer, nullable=True, index=True)
    magnitude_unk: bool = Column(Boolean, nullable=False)

    segments = relationship('TornadoSegment', backref='tornado')


class TornadoSegment(Base, _PathEvent):
    __tablename__ = 'tornado_segment'
    magnitude: float = Column(Integer, nullable=True, index=True)
    magnitude_unk: bool = Column(Boolean, nullable=False)

    tornado_id: int = Column(Integer, ForeignKey('tornado.id'), nullable=False)
    counties = relationship('TornadoSegmentCounty')


class TornadoSegmentCounty(Base):
    __tablename__ = 'tornado_segment_county'
    id: int = Column(Integer, primary_key=True)
    tornado_segment_id: int = Column(ForeignKey('tornado_segment.id'), nullable=False)
    county_id: int = Column(ForeignKey('county.id'), nullable=False)
    county_order: int = Column(Integer, nullable=False)
    county: County = relationship('County')
