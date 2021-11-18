from sqlalchemy import create_engine
from .models import Base

db_url = 'mysql+pymysql://user:password@db:3306/spc'
engine = None


def setup_db(drop=False):
    global engine
    engine = create_engine(db_url, echo=True, future=True)
    if drop and engine is None:
        Base.metadata.drop_all(engine)
    if engine is None:
        Base.metadata.create_all(engine)


if __name__ == '__main__':
    print('Creating tables')
    setup_db()
    print('Done')
