from database_utils import DatabaseConnector
from sqlalchemy import create_engine
from sqlalchemy import text

if __name__ == "__main__":
    print('Run main.py first!')

dbc = DatabaseConnector()

class SQL_queries:

    def QnA(self):
        
        cred = dbc.read_db_creds()
        DBAPI = 'psycopg2'
        DATABASE_TYPE = 'postgresql'
        eng = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{cred['USER']}:{cred['PASSWORD']}@{cred['HOST']}:{cred['PORT']}/{cred['DATABASE']}")
        with eng.execution_options(isolation_level='AUTOCOMMIT').connect() as connection:
            
            print('How many stores does the business have and in which countries?\n')
            connection.execute(text('''SELECT country_code, COUNT(country_code) AS total_no_stores FROM dim_store_details
                                       GROUP BY country_code
                                       ORDER BY total_no_stores DESC'''))
            
            print('\nWhich locations currently have the most stores?\n')
            connection.execute(text('''SELECT locality, COUNT(locality) AS total_no_stores
                                       FROM dim_store_details
                                       GROUP BY locality
                                       ORDER BY total_no_stores DESC
                                       LIMIT 7'''))

            print('\nWhich months produced the largest amount of sales?\n')
            connection.execute(text(''''''))

            print('\nHow many sales are coming from online?\n')
            connection.execute(text(''''''))

            print('\nWhat percentage of sales come through each type of store?\n')
            connection.execute(text(''''''))

            print('\nWhich month in each year produced the highest cost of sales?\n')
            connection.execute(text(''''''))

            print('\nWhat is our staff headcount?\n')
            connection.execute(text(''''''))

            print('\nWhich German store type is selling the most?\n')
            connection.execute(text(''''''))

            print('\nHow quickly is the company making sales?\n')
            connection.execute(text(''''''))