import os

from sqlalchemy.orm import Session
import pandas as pd

from svrdb.models import County, Base, engine


def seed_counties(session):
    # TODO: parameterize data file
    fips_fname = os.path.join(os.path.dirname(__file__), 'data', 'us_cty_fips.txt')
    county_df = pd.read_csv(fips_fname,
                            names=['state', 'state_fips', 'county_fips', 'county', 'class'],
                            index_col=False)
    del county_df['class']
    records = county_df.to_dict(orient='records')
    session.bulk_save_objects([County(**rec) for rec in records], return_defaults=True)


def seed(create_tables=True):
    if create_tables:
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

    session = Session(bind=engine)
    seed_counties(session)
    session.commit()


if __name__ == '__main__':
    seed()
