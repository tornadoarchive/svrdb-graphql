import os

from sqlalchemy.orm import Session
import pandas as pd
import numpy as np

from svrdb.models import County, Base, engine, Hail, Wind


def seed(recreate_tables=True):
    if recreate_tables:
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

    session = Session(bind=engine)

    county_ref = seed_counties(session)
    seed_hail(session, county_ref)
    seed_wind(session, county_ref)

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
    file = _get_data_file('1955-2019_hail.csv')
    records = _common_generate_records(county_ref, file)
    session.bulk_save_objects([Hail(id=i, **rec) for i, rec in enumerate(records, start=1)])


def seed_wind(session, county_ref):
    file = _get_data_file('1955-2019_wind.csv')
    records = _common_generate_records(county_ref, file)
    session.bulk_save_objects([Wind(id=i, **rec) for i, rec in enumerate(records, start=1)])


def _common_generate_records(county_ref, file):
    columns = {
        'state': 'st',
        'magnitude': 'mag',
        'fatalities': 'fat',
        'injuries': 'inj',
        'lat': 'slat',
        'lon': 'slon',
        'datetime': 'datetime',
        'loss': 'loss',
        'closs': 'closs'
    }
    df = pd.read_csv(file, parse_dates=[['date', 'time']], index_col=False)

    dts = pd.to_timedelta(df['tz'].apply(lambda tz: 0 if tz == 9 else 6), unit='H')
    converted = df['date_time'] + dts
    df['datetime'] = converted

    subset = df[['stf', 'f1'] + list(columns.values())]
    subset = subset.merge(county_ref, left_on=['stf', 'f1'], right_on=['state_fips', 'county_fips'], how='left')
    subset = subset[list(columns.values()) + ['county_id']]
    subset.columns = list(columns.keys()) + ['county_id']

    return subset.replace({np.nan: None}).to_dict(orient='records')


def _get_data_file(filename):
    return os.path.join(os.path.dirname(__file__), 'data', filename)


if __name__ == '__main__':
    seed()
