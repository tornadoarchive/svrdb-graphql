import pandas as pd


def correct_tor_records(df):
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

    # fix null island
    df.loc[(df.elat < 10) & (df.sg > 0), 'elat'] = df.slat
    df.loc[(df.elon > -10) & (df.sg > 0), 'elon'] = df.slon

    # fix unmatched continuation records
    df.loc[(df.st == 'IA') & (df.date_time == '1953-06-07 21:15:00') & (df.om == 265) & (df.sg == -9),
           'om'] = 263
    df.loc[(df.st == 'SD') & (df.date_time == '1961-06-21 14:30:00') & (df.om == 456) & (df.sg == -9),
           'om'] = 454

    # this tornado was labeled as a continuation segment when it was a full track
    # see https://www.ncdc.noaa.gov/stormevents/eventdetails.jsp?id=291657 and
    # https://www.ncdc.noaa.gov/stormevents/eventdetails.jsp?id=291659
    df.loc[(df.st == 'LA') & (df.date_time == '2011-04-26 23:56:00') & (df.sg == -9),
           ['sn', 'sg']] = [1, 1]

    return df