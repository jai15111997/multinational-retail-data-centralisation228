from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
import pandas as pd
import numpy as np
from dateutil.parser import parse
import boto3
import re

dbc = DatabaseConnector()
d_ext = DataExtractor()
d_cl = DataCleaning()
pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
store_ep = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
no_st_ep = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
s3_address = 's3://data-handling-public/products.csv'
s3_d_event = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
ls = d_ext.list_db_tables()

'''
data = d_ext.read_rds_table(dbc, 'legacy_users')
data.dropna(subset = ['first_name', 'last_name', 'user_uuid','date_of_birth','country','country_code','join_date'], inplace = True)

data['first_name'] = data['first_name'].replace('[0-9]', '', regex=True)
data['last_name'] = data['last_name'].replace('[0-9]', '', regex=True)
data['company'] = data['company'].replace('[0-9]', '', regex=True)
data['email_address'] = np.where((data['email_address'].str.contains('@')) & (data['email_address'].str.contains('.')), data['email_address'], np.nan)
data['address'] = data['address'].replace('NULL', '', regex=True)
data['country'] = data['country'].replace('[0-9]', '', regex=True)
data['country_code'] = np.where((data['country_code'].str.len() == 2) & data['country_code'].str.contains('[A-Z]'), data['country_code'], np.nan)
data['phone_number'] = np.where((data['phone_number']).str.contains('[a-zA-Z]'), np.nan, data['phone_number'])
data['phone_number'] = data['phone_number'].replace({r'\+44': '0', r'\(': '', r'\)': '', r'-': '', r' ': '', r'\.': ''}, regex=True)
is_valid_format = data['user_uuid'].str.match(r'^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$')
data[~is_valid_format] = np.nan

data['first_name'] = data['first_name'].astype('string')
data['last_name'] = data['last_name'].astype('string')
data['date_of_birth'] = pd.to_datetime(data['date_of_birth'], errors='coerce')
data['company'] = data['company'].astype('string')
data['email_address'] = data['email_address'].astype('string')
data['address'] = data['address'].astype('string')
data['country'] = data['country'].astype('string')
data['country_code'] = data['country_code'].astype('string')
data['phone_number'] = data['phone_number'].astype('Int64')
data['join_date'] = pd.to_datetime(data['join_date'], errors='coerce')
data['user_uuid'] = data['user_uuid'].astype('string')

data.reset_index(drop = True, inplace = True)

#print(data.dtypes)

'''

'''
card_details = d_ext.retrieve_pdf_data(pdf_path)
card_df = pd.concat(card_details, ignore_index=True)
card_df.dropna(inplace = True)

card_df['card_number'] = card_df['card_number'].apply(lambda x: int(x) if str(x).isdigit() else np.nan)
card_df['expiry_date'] = card_df['expiry_date'].apply(lambda x: '01/'+ x)
def custom_parse(date_str):
    try:
        return parse(date_str)
    except ValueError:
        return pd.NaT
card_df['expiry_date'] = card_df['expiry_date'].apply(custom_parse)
card_df['expiry_date'] = pd.to_datetime(card_df['expiry_date'], format= '%d-%m-%Y', errors='coerce')
card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], format= '%Y-%m-%d', errors='coerce')

card_df['card_number'] = card_df['card_number'].astype('Int64')
card_df['card_provider'] = card_df['card_provider'].astype('string')

card_df.reset_index(drop = True, inplace = True)

print(card_df.dtypes)
'''
'''
no_st_dict = d_ext.list_number_of_stores(no_st_ep, header)
all_stores_df = pd.DataFrame()
for count in range(0, no_st_dict):
    store_ep_n = store_ep + str(count)
    st_data = d_ext.retrieve_stores_data(store_ep_n, header)
    all_stores_df = all_stores_df._append(st_data, ignore_index = True)
all_stores_df.set_index('index', inplace = True)
'''

'''
all_stores_df = pd.read_csv('store_data.csv')
all_stores_df.drop(['lat'], axis = 1, inplace = True)
all_stores_df.dropna(inplace = True)
all_stores_df['address'] = np.where((all_stores_df['address'] == 'N/A') | (all_stores_df['address'] == 'NULL'), '', all_stores_df['address'])
all_stores_df['locality'] = np.where(all_stores_df['locality'].str.contains('[0-9]') | (all_stores_df['locality'] == 'N/A') | (all_stores_df['locality'] == 'NULL'), '', all_stores_df['locality'])
all_stores_df['store_code'] = np.where(all_stores_df['store_code'].str.contains('-'), all_stores_df['store_code'], '')

def custom_parse(date_str):
    try:
        return parse(date_str)
    except ValueError:
        return pd.NaT
    
all_stores_df['opening_date'] = all_stores_df['opening_date'].apply(custom_parse)
all_stores_df['opening_date'] = pd.to_datetime(all_stores_df['opening_date'], infer_datetime_format=True, errors='coerce')
all_stores_df['store_type'] = all_stores_df['store_type'].apply(lambda x: x if x in ['Super Store', 'Web Portal', 'Local', 'Outlet', 'Mall Kiosk'] else '') 
all_stores_df['country_code'] = all_stores_df['country_code'].apply(lambda x: x if x in ['US', 'GB', 'DE'] else '') 
all_stores_df['continent'] = all_stores_df['continent'].replace('ee' ,'')
all_stores_df['continent'] = all_stores_df['continent'].apply(lambda x: x if x in ['Europe', 'America'] else '')

all_stores_df['address'] = all_stores_df['address'].astype('string')
all_stores_df['longitude'] = pd.to_numeric(all_stores_df['longitude'], errors = 'coerce')
all_stores_df['locality'] = all_stores_df['locality'].astype('string')
all_stores_df['store_code'] = all_stores_df['store_code'].astype('string')
all_stores_df['staff_numbers'] = pd.to_numeric(all_stores_df['staff_numbers'], errors = 'coerce')
all_stores_df['store_type'] = all_stores_df['store_type'].astype('string')
all_stores_df['latitude'] = pd.to_numeric(all_stores_df['latitude'], errors = 'coerce')
all_stores_df['country_code'] = all_stores_df['country_code'].astype('string')
all_stores_df['continent'] = all_stores_df['continent'].astype('string')



all_stores_df.reset_index(drop = True, inplace = True)

print(all_stores_df.dtypes)
#print(all_stores_df['continent'].head(65))
'''
'''
s3_data = pd.read_csv('products.csv')

s3_data['weight'] = s3_data['weight'].str.replace('kg', '')

def convert(data):
    data_s = str(data)
    num_ls = [int(num) for num in re.findall(r'\d+', data_s)]
    data_m = np.prod(num_ls) / 1000
    return data_m
s3_data['weight'] = s3_data['weight'].apply(lambda x: convert(x) if 'g' in str(x) or 'ml' in str(x) or 'x' in str(x) else x)
s3_data['weight'] = pd.to_numeric(s3_data['weight'], errors = 'coerce')

print(s3_data['weight'].head())
'''

'''
s3_data.dropna(subset = ['product_name', 'uuid'], inplace = True)
s3_data.set_index('Unnamed: 0', inplace= True)
s3_data['product_name'] = s3_data['product_name'].str.replace('NULL', '')
s3_data['product_price'] = s3_data['product_price'].str.replace('£', '')
s3_data['category'] = s3_data['category'].apply(lambda x: x if x in ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty', 'food-and-drink', 'diy'] else '')
is_valid_format = s3_data['uuid'].str.match(r'^[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}$')
s3_data[~is_valid_format] = ''
s3_data['removed'] = s3_data['removed'].str.replace('avaliable', 'available')
s3_data['removed'] = s3_data['removed'].apply(lambda x: x if x in ['Still_available', 'Removed'] else '')
code_format = s3_data['product_code'].str.match(r'^[a-zA-Z0-9]{2}-[a-zA-Z0-9]{8}$')
s3_data[~code_format] = ''

s3_data['product_name'] = s3_data['product_name'].astype('string')
s3_data['product_price'] = pd.to_numeric(s3_data['product_price'], errors = 'coerce')
s3_data['category'] = s3_data['category'].astype('string')
s3_data['EAN'] = pd.to_numeric(s3_data['EAN'], errors = 'coerce')
s3_data['date_added'] = pd.to_datetime(s3_data['date_added'], errors='coerce')
s3_data['uuid'] = s3_data['uuid'].astype('string')
s3_data['removed'] = s3_data['removed'].astype('string')
s3_data['product_code'] = s3_data['product_code'].astype('string')

s3_data = s3_data.reset_index(drop=True)
print(s3_data.dtypes)
#print(code_format)
print(s3_data.head(12))
'''

'''
data = d_ext.read_rds_table(dbc, 'orders_table')
data = data.drop(['first_name', 'last_name', '1', 'level_0'], axis = 1)
data.set_index('index', inplace= True)

data['date_uuid'] = np.where(data['date_uuid'].str.match(r'^[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}$'), data['date_uuid'], '')
data['user_uuid'] = np.where(data['user_uuid'].str.match(r'^[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}$'), data['user_uuid'], '')
data['store_code'] = np.where(data['store_code'].str.match(r'^[a-zA-Z0-9]{2}-[a-zA-Z0-9]{8}$') | data['store_code'].str.match(r'^[a-zA-Z0-9]{3}-[a-zA-Z0-9]{8}$'), data['store_code'], '')
data['product_code'] = np.where(data['product_code'].str.match(r'^[a-zA-Z0-9]{2}-[a-zA-Z0-9]{8}$') | data['product_code'].str.match(r'^[a-zA-Z0-9]{3}-[a-zA-Z0-9]{8}$'), data['product_code'], '')

data['date_uuid'] = data['date_uuid'].astype('string')
data['user_uuid'] = data['user_uuid'].astype('string')
data['store_code'] = data['store_code'].astype('string')
data['product_code'] = data['product_code'].astype('string')

data = data.reset_index(drop=True)
print(data['product_code'].head())
print(data['store_code'].head())
print(data['date_uuid'].head())
print(data['user_uuid'].head())
'''



#s3_d_data = d_ext.extract_from_s3(s3_d_event)
s3_d_data = pd.read_json('date_details.json')
s3_d_data['timestamp'] = pd.to_datetime(s3_d_data['timestamp'], format = '%H:%M:%S', errors = 'coerce').dt.time
s3_d_data['month'] = pd.to_numeric(s3_d_data['month'], errors = 'coerce')
s3_d_data['year'] = pd.to_numeric(s3_d_data['year'], errors = 'coerce')
s3_d_data['day'] = pd.to_numeric(s3_d_data['day'], errors = 'coerce')
s3_d_data['time_period'] = s3_d_data['time_period'].apply(lambda x: x if x in ['Evening', 'Morning', 'Late_Hours', 'Midday'] else '')
s3_d_data['date_uuid'] = np.where(s3_d_data['date_uuid'].str.match(r'^[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}$'), s3_d_data['date_uuid'], '')

s3_d_data['time_period'] = s3_d_data['time_period'].astype('string')
s3_d_data['date_uuid'] = s3_d_data['date_uuid'].astype('string')

s3_d_data = s3_d_data.reset_index(drop=True)

print(s3_d_data.dtypes)
print(s3_d_data['date_uuid'].head())
