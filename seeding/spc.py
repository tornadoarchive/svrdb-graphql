import numpy as np
import pandas as pd

from seeding.datasrcs import files
from seeding.spc_corrections import correct_tor_records
from svrdb.models import (
    Hail, Wind, Tornado,
    TornadoSegment, TornadoSegmentCounty
)


def seed_tornadoes(session, county_ref):
    df = pd.read_csv(files.SPC_TOR, parse_dates=[['date', 'time']], index_col=False)
    df = correct_tor_records(df)

    dts = pd.to_timedelta(df['tz'].apply(lambda tz: 0 if tz == 9 else 6), unit='H')
    converted = df['date_time'] + dts
    df['datetime'] = converted

    def find_counties(targ_df):
        counties = []
        for county_col in ('f1', 'f2', 'f3', 'f4'):
            merged = targ_df[['stf', county_col]].merge(
                county_ref,
                left_on=['stf', county_col],
                right_on=['state_fips', 'county_fips'],
                how='left'
            )
            counties.append(merged['county_id'])

        county_map = pd.concat(counties, axis=1).replace({np.nan, None})
        county_map.columns = [f'county_id_{i}' for i in range(1, 5)]
        return county_map

    df = pd.concat([df, find_counties(df)], axis=1)

    is_complete_track = ((df.ns == 1) & (df.sn == 1)) | ((df.ns > 1) & (df.sn == 0))
    is_segment = df.sn == 1
    is_continuation = df.sg == -9

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

    ## save full track tornadoes
    tor_df = df[is_complete_track & ~is_continuation].replace({np.nan: None})
    tor_df['id'] = range(1, len(tor_df) + 1)

    tor_records_df = tor_df[list(columns.values()) + ['id']]
    tor_records_df.columns = list(columns.keys()) + ['id']
    tor_records = tor_records_df.to_dict(orient='records')

    session.bulk_save_objects([Tornado(**rec) for rec in tor_records])

    ## save tornado segments
    seg_df = df[is_segment & ~is_continuation].replace({np.nan: None})
    # associate with parent tor id
    seg_df = seg_df.merge(tor_df[['yr', 'om', 'id']], how='left').rename(columns={'id': 'tornado_id'})
    seg_df['id'] = range(1, len(seg_df) + 1)

    if seg_df.tornado_id.isnull().any():
        # there are orphan segments or something wrong with the DB. Time to correct the data
        raise ValueError('Segment mismatch with tornado! Re-evaluate the data')

    seg_records_df = seg_df[list(columns.values()) + ['id', 'tornado_id']]
    seg_records_df.columns = list(columns.keys()) + ['id', 'tornado_id']
    seg_records = seg_records_df.to_dict(orient='records')
    seg_records = [TornadoSegment(**rec) for rec in seg_records]

    # associate counties - this eventually has to be a slow loop, unfortunately
    continuation_df = df[is_continuation].replace({np.nan: None})
    continuation_df = continuation_df.merge(seg_df[['yr', 'om', 'st', 'id']], how='left')

    if continuation_df.id.isnull().any():
        raise ValueError('Continuation county record mismatch with county! Re-evaluate the data')

    # this ensures counties are inserted in order
    continuation_df = continuation_df.sort_values(by='datetime')

    county_order_tracker = {}
    seg_county_records = []

    def extract_counties(src_df):
        county_id_cols = [f'county_id_{i}' for i in range(1, 5)]
        county_df = src_df[['id'] + county_id_cols]

        for _, county_row in county_df.iterrows():
            seg_id = county_row.id
            for _, county_id in county_row.loc[county_df.columns != 'id'].items():
                if county_id is not None:
                    county_order = county_order_tracker.get(seg_id, 0) + 1
                    seg_record = TornadoSegmentCounty(
                        tornado_segment_id=seg_id,
                        county_id=county_id,
                        county_order=county_order
                    )
                    seg_county_records.append(seg_record)
                    county_order_tracker[seg_id] = county_order

    extract_counties(seg_df)
    extract_counties(continuation_df)

    session.bulk_save_objects(seg_records + seg_county_records)


def seed_hail(session, county_ref):
    records = _generate_point_records(county_ref, files.SPC_HAIL)
    session.bulk_save_objects([Hail(id=idx, **rec) for idx, rec in enumerate(records, start=1)])


def seed_wind(session, county_ref):
    records = _generate_point_records(county_ref, files.SPC_WIND)
    session.bulk_save_objects([Wind(id=idx, **rec) for idx, rec in enumerate(records, start=1)])


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
    subset = subset.merge(county_ref, left_on=['stf', 'f1'],
                          right_on=['state_fips', 'county_fips'], how='left')
    subset = subset[list(columns.values()) + ['county_id']]
    subset.columns = list(columns.keys()) + ['county_id']

    return subset.replace({np.nan: None}).to_dict(orient='records')
