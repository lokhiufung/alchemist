import os

from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv('.env')

from alchemist.timescaledb.models.base import Base


TIMESCALEDB_URI = os.getenv('TIMESCALEDB_URI')


def main():
    engine = create_engine(TIMESCALEDB_URI)

    Base.metadata.drop_all(engine)
    
    Base.metadata.create_all(engine)


if __name__ == '__main__':
    main()

