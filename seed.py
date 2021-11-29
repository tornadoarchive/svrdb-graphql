import os

import pandas as pd
import numpy as np

from svrdb.models import County, Base, engine, Hail, Wind, get_session, Tornado, TornadoSegment, TornadoSegmentCounty


def seed(recreate_tables=True):
    if recreate_tables:
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

    with get_session() as session:
        county_ref = seed_counties(session)
        
        seed_tornadoes(session, county_ref)
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


def seed_tornadoes(session, county_ref):
    file = _get_data_file('1950-2019_all_tornadoes.csv')
    df = pd.read_csv(file, parse_dates=[['date', 'time']], index_col=False)
    dts = pd.to_timedelta(df['tz'].apply(lambda tz: 0 if tz == 9 else 6), unit='H')
    converted = df['date_time'] + dts
    df['datetime'] = converted

    is_easy_parse = (df.ns == 1) & (df.sn == 1) & (df.sg == 1) & (df.f4 == 0)
    single_seg_tors = df[is_easy_parse]

    columns = {
        'state': 'st',
        'magnitude': 'mag',
        'fatalities': 'fat',
        'injuries': 'inj',
        'start_lat': 'slat',
        'start_lon': 'slon',
        'end_lat': 'elat',
        'end_lon': 'elon',
        'length': 'len',
        'width': 'wid',
        'datetime': 'datetime',
        'loss': 'loss',
        'closs': 'closs',
        'magnitude_unk': 'fc'
    }

    df_single_seg = single_seg_tors[list(columns.values())]
    df_single_seg.columns = list(columns.keys())

    records = df_single_seg.replace({np.nan: None}).to_dict(orient='records')
    tors, segs, seg_cties = [], [], []

    def find_county(county_col):
        merged = single_seg_tors[['stf', county_col]].merge(
            county_ref,
            left_on=['stf', county_col],
            right_on=['state_fips', 'county_fips'],
            how='left'
        )
        return merged['county_id']

    county_map = pd.concat(
        [find_county(col) for col in ('f1', 'f2', 'f3', 'f4')],
        axis=1
    ).replace({np.nan: None})
    county_map.columns = [f'county_id_{i}' for i in range(1, 5)]

    # TODO: figure out why backref doesn't work... for now join ids manually
    for idx, rec in enumerate(records, start=1):
        tor = Tornado(id=idx, **rec)
        seg = TornadoSegment(id=idx, **rec)
        seg_counties = [
            TornadoSegmentCounty(
                tornado_segment_id=seg.id,
                county_id=county_map.iloc[idx - 1][f'county_id_{cty_idx}'],
                county_order=cty_idx
            )
            for cty_idx in range(1, 5)
            if county_map.iloc[idx - 1][f'county_id_{cty_idx}'] is not None
        ]
        seg.tornado_id = tor.id
        seg.counties = seg_counties
        tors.append(tor)
        segs.append(seg)
        seg_cties += seg_counties

    session.bulk_save_objects(tors + segs + seg_cties)


def seed_hail(session, county_ref):
    file = _get_data_file('1955-2019_hail.csv')
    records = _generate_point_records(county_ref, file)
    session.bulk_save_objects([
        Hail(id=idx, **rec) for idx, rec in enumerate(records, start=1)
    ])


def seed_wind(session, county_ref):
    file = _get_data_file('1955-2019_wind.csv')
    records = _generate_point_records(county_ref, file)
    session.bulk_save_objects([
        Wind(id=idx, **rec) for idx, rec in enumerate(records, start=1)
    ])


def _generate_point_records(county_ref, file):
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
