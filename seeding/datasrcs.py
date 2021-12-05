import os

from decouple import config


def get_datadir():
    # TODO: this will only work locally mounting data dir to ../data
    # How will this work in non-local environments?
    current_dir = os.path.dirname(__file__)
    parent_dir = os.path.dirname(current_dir)
    return os.path.join(parent_dir, 'data')


class _Files:
    SPC_TOR = os.path.join(get_datadir(), config('SPC_TOR_FILE'))
    SPC_WIND = os.path.join(get_datadir(), config('SPC_WIND_FILE'))
    SPC_HAIL = os.path.join(get_datadir(), config('SPC_HAIL_FILE'))
    US_COUNTIES = os.path.join(get_datadir(), config('US_COUNTY_FILE'))


files = _Files
