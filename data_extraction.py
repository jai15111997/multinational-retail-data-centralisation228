import boto3
from database_utils import DatabaseConnector
import pandas as pd
import requests
from sqlalchemy import text
import tabula

if __name__ == "__main__":
    print('Run main.py first!')

dbc = DatabaseConnector()

class DataExtractor:
    def list_db_tables(self):
        eng = dbc.init_db_engine()
        eng.execution_options(isolation_level='AUTOCOMMIT').connect()
        with eng.connect() as connection:
            result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';"))
            ls = list(result)
            ls_2 = [0 ,0 ,0]
            for index in range(0, 3):
                ls_2[index] = ls[index][0]
            return ls_2
        
    def read_rds_table(self, dbc_o, t_name):
        eng = dbc_o.init_db_engine()
        eng.execution_options(isolation_level='AUTOCOMMIT').connect()
        if t_name in DataExtractor.list_db_tables(self):
            data = pd.read_sql_table(t_name, eng)
            return data
    
    def retrieve_pdf_data(self, pdf_data):
        df = tabula.read_pdf(pdf_data, pages = 'all')
        return df
    
    def list_number_of_stores(self, num_st_ep, h_dict):
        response = requests.get(num_st_ep, headers = h_dict)
        dict_1 = response.json()
        return dict_1['number_stores']
    
    def retrieve_stores_data(self, st_ep, h_dict):
        response = requests.get(st_ep, headers= h_dict)
        return response.json()
    
    def extract_from_s3(self, address):
        s3 = boto3.client('s3')
        address = address.strip()
        splitted = address.split("//")[1]
        bucket_name = splitted.split("/")[0]
        key = splitted.split("/")[-1]
        if '.' in bucket_name:
            bucket_name = bucket_name.split(".")[0]
        s3.download_file(bucket_name, key, key)
        if 'csv' in key:
            data = pd.read_csv(key)
        else:
            data = pd.read_json(key)
        return data