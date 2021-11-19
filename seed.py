import os

from sqlalchemy.orm import Session
import pandas as pd
import numpy as np

from svrdb.models import County, Base, engine, Hail


def seed(recreate_tables=True):
    if recreate_tables:
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

    session = Session(bind=engine)

    county_ref = seed_counties(session)
    seed_hail(session, county_ref)

    session.commit()


def seed_counties(session):
    fips_fname = _get_data_file('us_cty_fips.txt')
    county_df = pd.read_csv(fips_fname,
                            names=['state', 'state_fips', 'county_fips', 'county'],
                            index_col=False)

    records = county_df.to_dict(orient='records')
    db_counties = [County(id=i, **rec) for i, rec in enumerate(records, start=1)]
    session.bulk_save_objects(db_counties)

    # return the df for in-memory lookup on county id's
    county_df['county_id'] = [cty.id for cty in db_counties]
    return county_df


def seed_hail(session, county_ref):
    columns = {
        'state':'st',
        'magnitude':'mag',
        'fatalities':'fat',
        'injuries':'inj',
        'lat':'slat',
        'lon':'slon',
        'datetime': 'datetime',
        'loss': 'loss',
        'closs': 'closs'
    }

    file = _get_data_file('1955-2019_hail.csv')
    haildf = pd.read_csv(file, parse_dates=[['date', 'time']], index_col=False)

    dts = pd.to_timedelta(haildf['tz'].apply(lambda tz: 0 if tz == 9 else 6), unit='H')
    converted = haildf['date_time'] + dts
    haildf['datetime'] = converted

    subset = haildf[['stf', 'f1'] + list(columns.values())]
    subset = subset.merge(county_ref, left_on=['stf', 'f1'], right_on=['state_fips', 'county_fips'], how='left')
    subset = subset[list(columns.values()) + ['county_id']]
    subset.columns = list(columns.keys()) + ['county_id']

    records = subset.replace({np.nan: None}).to_dict(orient='records')
    session.bulk_save_objects([Hail(id=i, **rec) for i, rec in enumerate(records, start=1)])


def _get_data_file(filename):
    return os.path.join(os.path.dirname(__file__), 'data', filename)


if __name__ == '__main__':
    seed()
