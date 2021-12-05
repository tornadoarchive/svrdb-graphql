from decouple import config

from seeding.counties import seed_counties
from seeding.spc import seed_tornadoes, seed_hail, seed_wind
from svrdb.models import get_session, Base, engine


def seed(to_seed, recreate_tables=True):
    if to_seed not in ('tornado', 'all'):
        print(f'SEED_DB argument: {to_seed} not `tornado` or `all`, skip seeding')
        return

    if recreate_tables:
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

    with get_session() as session:
        county_ref = seed_counties(session)
        seed_tornadoes(session, county_ref)

        if to_seed == 'all':
            seed_hail(session, county_ref)
            seed_wind(session, county_ref)

        session.commit()


if __name__ == '__main__':
    seed(config('SEED_DB', default='none'))
