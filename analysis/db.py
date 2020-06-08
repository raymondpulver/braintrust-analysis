from sqlalchemy import create_engine
import configparser

config = configparser.ConfigParser()
config.read('db.ini')
engine = create_engine(config['alembic']['sqlalchemy.url'])

