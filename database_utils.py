import pandas as pd
from sqlalchemy import create_engine
import yaml

if __name__ == "__main__":
    print('Run main.py first!')

class DatabaseConnector:

    def read_db_creds(self):
        with open('db_credentials.yaml') as f:
            dct = yaml.load(f, Loader = yaml.FullLoader)
            return dct
        
    def init_db_engine(self):
        cred = DatabaseConnector.read_db_creds(self)
        DBAPI = 'psycopg2'
        DATABASE_TYPE = 'postgresql'
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{cred['RDS_USER']}:{cred['RDS_PASSWORD']}@{cred['RDS_HOST']}:{cred['RDS_PORT']}/{cred['RDS_DATABASE']}")
        return engine
    
    def upload_to_db(self, df, t_name):
        cred = DatabaseConnector.read_db_creds(self)
        DBAPI = 'psycopg2'
        DATABASE_TYPE = 'postgresql'
        eng = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{cred['USER']}:{cred['PASSWORD']}@{cred['HOST']}:{cred['PORT']}/{cred['DATABASE']}")
        with eng.connect() as connection:
            df.to_sql(t_name, connection, if_exists='replace', index=False)