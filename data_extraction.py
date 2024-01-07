# Import Necessary Packages
import boto3
from database_utils import DatabaseConnector
import pandas as pd
import requests
from sqlalchemy import text
import tabula

# Check if this script is the main entry point
if __name__ == "__main__":
    print('Run main.py first!')

# Creating Instance for the DatabaseConnector Class
dbc = DatabaseConnector()

class DataExtractor:

    # Listing all tables in the given Database
    def list_db_tables(self):
       
        # Initializing database engine
        eng = dbc.init_db_engine()
        # Set isolation level to AUTOCOMMIT
        eng.execution_options(isolation_level='AUTOCOMMIT').connect()
        with eng.connect() as connection:

            # Execute SQL query to get table names
            result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';"))
            # Extract table names from the result into a list
            ls = list(result)
            ls_2 = [0 ,0 ,0]
            for index in range(0, 3):
                ls_2[index] = ls[index][0]
            return ls_2
        
    # Read data from a specified RDS table
    def read_rds_table(self, dbc_o, t_name):

        # Initialize database engine
        eng = dbc_o.init_db_engine()
        # Set isolation level to AUTOCOMMIT
        eng.execution_options(isolation_level='AUTOCOMMIT').connect()
        if t_name in DataExtractor.list_db_tables(self):
            # Read and return data from the specified table
            data = pd.read_sql_table(t_name, eng)
            return data
    
    # Retrieve data from a PDF file using tabula
    def retrieve_pdf_data(self, pdf_data):
        df = tabula.read_pdf(pdf_data, pages = 'all')
        return df
    
    # Get the number of stores from the given endpoint
    def list_number_of_stores(self, num_st_ep, h_dict):
        response = requests.get(num_st_ep, headers = h_dict)
        dict_1 = response.json()
        return dict_1['number_stores']
    
    # Retrieve stores data from a specified endpoint
    def retrieve_stores_data(self, st_ep, h_dict):
        response = requests.get(st_ep, headers= h_dict)
        return response.json()
    
    # Extract data from an S3 bucket
    def extract_from_s3(self, address):

        # Initialize S3 client
        s3 = boto3.client('s3')
        address = address.strip()
        # Extract bucket name and key from the address
        splitted = address.split("//")[1]
        bucket_name = splitted.split("/")[0]
        key = splitted.split("/")[-1]
        if '.' in bucket_name:
            bucket_name = bucket_name.split(".")[0]
        # Download file from S3
        s3.download_file(bucket_name, key, key)
        # Read data based on file type (csv or json)
        if 'csv' in key:
            data = pd.read_csv(key)
        else:
            data = pd.read_json(key)
        return data