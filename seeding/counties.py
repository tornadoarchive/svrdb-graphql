import pandas as pd

from svrdb.models import County
from seeding.datasrcs import files


def seed_counties(session):
    county_df = pd.read_csv(files.US_COUNTIES,
                            names=['state', 'state_fips', 'county_fips', 'county'],
                            index_col=False)

    records = county_df.to_dict(orient='records')
    db_counties = [County(id=i, **rec) for i, rec in enumerate(records, start=1)]
    session.bulk_save_objects(db_counties)

    # return the df for in-memory lookup on county id's
    county_df['county_id'] = [cty.id for cty in db_counties]
    return county_df
