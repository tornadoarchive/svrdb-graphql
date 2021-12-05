import os

from decouple import config

datadir = config('DATA_FILE_DIR')


class _Files:
    SPC_TOR = os.path.join(datadir, config('SPC_TOR_FILE'))
    SPC_WIND = os.path.join(datadir, config('SPC_WIND_FILE'))
    SPC_HAIL = os.path.join(datadir, config('SPC_HAIL_FILE'))
    US_COUNTIES = os.path.join(datadir, config('US_COUNTY_FILE'))


files = _Files
