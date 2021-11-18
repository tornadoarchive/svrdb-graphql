from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
db_url = 'mysql+pymysql://user:password@db:3306/spc'
engine = None

# this step imports the models
from .models import *


def setup_db(drop=True):
    global engine
    engine = create_engine(db_url, echo=True, future=True)
    if drop:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


setup_db()