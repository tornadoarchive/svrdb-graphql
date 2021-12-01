import os

import pandas as pd
import numpy as np

from svrdb.models import (
    County, Base, engine, Hail, Wind, get_session, Tornado,
    TornadoSegment, TornadoSegmentCounty
)


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
    df = _correct_tor_records(df)

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

    # associate counties - this has to be a slow loop, unfortunately
    continuation_df = df[is_continuation].replace({np.nan: None})
    continuation_df = continuation_df.merge(seg_df, on=['yr', 'om', 'st'],
                                            suffixes=[None, '_orig'], how='left')

    seg_county_records = []
    for _, row in seg_df.iterrows():
        seg_id = row.id
        county_ids = [
            row[id_col] for id_col in [f'county_id_{i}' for i in range(1, 5)]
            if row[id_col] is not None
        ]

        if seg_id in continuation_df.id:
            # most times there will be only one continuation row, but making sure we
            # account for instances there are multiple
            for _, continuation_row in continuation_df[continuation_df.id == seg_id].iterrows():
                county_ids += [
                    continuation_row[id_col] for id_col in [f'county_id_{i}' for i in range(1, 5)]
                    if continuation_row[id_col] is not None
                ]

        for idx, cty_id in enumerate(county_ids, start=1):
            seg_record = TornadoSegmentCounty(
                tornado_segment_id=seg_id,
                county_id=cty_id,
                county_order=idx
            )
            seg_county_records.append(seg_record)

    session.bulk_save_objects(seg_records + seg_county_records)


def _correct_tor_records(df):
    df = df.copy()

    # county segment entered as an extra tornado in the FL Panhandle on 3/15/01
    # see https://www.ncdc.noaa.gov/stormevents/eventdetails.jsp?id=5238186 and
    # https://www.ncdc.noaa.gov/stormevents/eventdetails.jsp?id=5238187
    df.drop(df[(df.om == 56) & (df.date_time == '2001-03-15 03:40:00')].index, inplace=True)

    # dup om's on legitimate separate tornadoes -- this breaks my join condition
    df.loc[(df.om == 506) & (df.date_time == '2002-04-11 16:35:00'), 'om'] = df[df.yr == 2002].om.max() + 1
    df.loc[(df.om == 252) & (df.date_time == '2010-05-10 15:03:00'), 'om'] = df[df.yr == 2010].om.max() + 1

    # duplicate tornado in May 2015
    try:
        df.drop(df[(df.om == 610626) & (df.yr == 2015)].index[1], inplace=True)
    except IndexError:
        # if no dup exists because subsequent SPC files were fixed, we're good
        pass

    # mislabeled/duplicate om's -- found by @tsupinie
    df.loc[(df.om == 9999) & (df.yr == 1995) & (df.st == 'IA'), 'om'] = 9998
    df.loc[(df.om == 576455) & (df.yr == 2015) & (df.st == 'NE'), 'om'] = 576454
    df.loc[(df.om == 265) & (df.yr == 1953) & (df.st == 'IA'), 'om'] = 263
    df.loc[(df.om == 456) & (df.yr == 1961) & (df.st == 'SD'), 'om'] = 454

    # missing state segment from a KS-NE tornado in Mar 1993
    # see https://www.ncdc.noaa.gov/stormevents/eventdetails.jsp?id=10326096 and
    # https://www.ncdc.noaa.gov/stormevents/eventdetails.jsp?id=10334215
    pd.options.mode.chained_assignment = None  # ignore warnings
    ne_segment_1993 = df[(df.om == 74) & (df.yr == 1993)]
    ne_segment_1993['st'] = 'NE'
    ne_segment_1993['date_time'] = pd.Timestamp('1993-03-28 17:22:00')
    ne_segment_1993[['stf', 'f1']] = [31, 65]
    ne_segment_1993['len'] = 0.25
    ne_segment_coord = [40.02, -99.92]
    ne_segment_1993[['slat', 'slon']] = ne_segment_coord
    ne_segment_1993[['elat', 'elon']] = ne_segment_coord
    ne_segment_1993[['ns', 'sn', 'sg']] = [2, 1, 2]

    par_tornado_1993 = df[(df.om == 74) & (df.yr == 1993)]
    par_tornado_1993[['ns', 'sn', 'sg']] = [2, 0, 1]

    df = pd.concat([
        df,
        par_tornado_1993,
        ne_segment_1993
    ], ignore_index=True)  # this avoids pandas.errors.InvalidIndexError

    # fix fips/counties, credit goes to @tsupinie for these fixes
    fips_fixes = {
        (46, 131): 71,  # Washabaugh County, SD merged with Jackson County, SD
        (12, 25): 86,  # Dade County, FL renamed Miami-Dade County, FL
        (13, 597): 197,  # Typo on the FIPS code for Marion County, GA?
        (51, 39): 37,  # Typo on the FIPS code for Charlotte County, VA?
        (27, 2): 3,  # Typo on the FIPS code for Anoka County, MN?
        (51, 123): 800,  # Suffolk City, VA replaced Nansemond County, VA
        (46, 1): 3,  # Typo on the FIPS code for Aurora County, SD?
        (29, 677): 77,  # Typo on the FIPS code for Greene County, MO?
        (21, 22): 33,  # Typo on the FIPS code for Caldwell County, KY?
        (42, 159): 15,  # Typo on the FIPS code for Bradford County, PA?
        (72, 8): 5,  # Typo on the FIPS code for Aguadilla, PR?
        (46, 113): 102,  # Shannon County, SD became Ogalala Lakota County
        (2, 155): 50,  # Old code for Bethel Census Area?
        (2, 181): 13  # Old code for Aleutians East Borough?
    }

    for fips, replacement in fips_fixes.items():
        stf, ctf = fips
        for county_col in ('f1', 'f2', 'f3', 'f4'):
            df.loc[(df.stf == stf) & (df[county_col] == ctf), county_col] = replacement

    df.loc[(df.yr == 1966) & (df.om.isin([13, 14])), 'f1'] = [83, 81]

    return df


def seed_hail(session, county_ref):
    file = _get_data_file('1955-2019_hail.csv')
    records = _generate_point_records(county_ref, file)
    session.bulk_save_objects([Hail(id=idx, **rec) for idx, rec in enumerate(records, start=1)])


def seed_wind(session, county_ref):
    file = _get_data_file('1955-2019_wind.csv')
    records = _generate_point_records(county_ref, file)
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


def _get_data_file(filename):
    return os.path.join(os.path.dirname(__file__), 'data', filename)


if __name__ == '__main__':
    seed()
