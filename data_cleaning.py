# Import Necessary Packages
import datetime as datetime
from dateutil.parser import parse
from database_utils import DatabaseConnector
import numpy as np
import pandas as pd
import re

# Check if this script is the main entry point
if __name__ == "__main__":
    print('Run main.py first!')

# Creating Instance for the DatabaseConnector Class
dbc = DatabaseConnector()

class DataCleaning:

   # Clean user data
   def clean_user_data(self, data):
      # Drop rows with missing values in specified columns
      data.dropna(subset = ['first_name', 'last_name', 'user_uuid','date_of_birth','country','country_code','join_date'], inplace = True)

      # Cleaning operations on string columns
      data['first_name'] = data['first_name'].replace('[0-9]', '', regex=True)
      data['last_name'] = data['last_name'].replace('[0-9]', '', regex=True)
      data['company'] = data['company'].replace('[0-9]', '', regex=True)
      data['email_address'] = np.where((data['email_address'].str.contains('@')) & (data['email_address'].str.contains('.')), data['email_address'], np.nan)
      data['address'] = data['address'].replace('NULL', '', regex=True)
      data['country'] = data['country'].replace('[0-9]', '', regex=True)
      data['country_code'] = np.where((data['country_code'].str.len() == 2) & data['country_code'].str.contains('[A-Z]'), data['country_code'], np.nan)
      data['phone_number'] = np.where((data['phone_number']).str.contains('[a-zA-Z]'), np.nan, data['phone_number'])
      data['phone_number'] = data['phone_number'].replace({r'\+44': '0', r'\(': '', r'\)': '', r'-': '', r' ': '', r'\.': ''}, regex=True)
      data['user_uuid'] = np.where(data['user_uuid'].str.match(r'^[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}$'), data['user_uuid'], np.nan)
      data.dropna(subset = ['user_uuid'], inplace = True)

      # Convert data types
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

      # Reset index
      data.reset_index(drop = True, inplace = True)
      return data    
 
   # Clean card data
   def clean_card_data(self, card_df):
      
      # Remove rows with invalid 'expiry_date'
      card_df = card_df.drop(card_df[card_df['expiry_date'].apply(lambda x: (len(x) > 5) or len(x) == 4)].index.tolist())
      card_df['expiry_date'] = card_df['expiry_date'].apply(lambda x: '01/'+ str(x))
      # Cleaning operations on numeric columns
      card_df['card_number'] = card_df['card_number'].astype('string')
      card_df['card_number'] = card_df['card_number'].apply(lambda x: x.strip('?') if '?' in x else x)
      card_df['card_number'] = pd.to_numeric(card_df['card_number'], errors = 'coerce')
      
      # Custom parsing for 'expiry_date'
      def custom_parse(date_str):
         try:
            return parse(date_str)
         except ValueError:
            return pd.NaT
      card_df['expiry_date'] = card_df['expiry_date'].apply(custom_parse)
      card_df['expiry_date'] = pd.to_datetime(card_df['expiry_date'], format= '%d-%m-%Y', errors='coerce').dt.strftime('%m-%d-%Y')
      card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], format= '%Y-%m-%d', errors='coerce')
      card_df.dropna(subset = ['card_number'], inplace = True)
      card_df['card_provider'] = card_df['card_provider'].astype('string')
      
      # Reset index
      card_df.reset_index(drop = True, inplace = True)
      return card_df
   
   # Clean store data
   def called_clean_store_data(self, all_stores_df):
      
      # Drop specified column
      all_stores_df.drop(['lat'], axis = 1, inplace = True)
      # Cleaning operations on string columns
      all_stores_df['address'] = np.where((all_stores_df['address'] == 'N/A') | (all_stores_df['address'] == 'NULL'), np.nan, all_stores_df['address'])
      all_stores_df['locality'] = np.where(all_stores_df['locality'].str.contains('[0-9]') | (all_stores_df['locality'] == 'N/A') | (all_stores_df['locality'] == 'NULL'), np.nan, all_stores_df['locality'])
      all_stores_df['store_code'] = np.where(all_stores_df['store_code'].str.contains('-'), all_stores_df['store_code'], np.nan)
      all_stores_df.dropna(subset = ['store_code'], inplace = True)

      # Custom parsing for 'opening_date'
      def custom_parse(date_str):
         try:
            return parse(date_str)
         except ValueError:
            return pd.NaT
         
      all_stores_df['opening_date'] = all_stores_df['opening_date'].apply(custom_parse)
      all_stores_df['opening_date'] = pd.to_datetime(all_stores_df['opening_date'], infer_datetime_format=True, errors='coerce')
      all_stores_df['store_type'] = all_stores_df['store_type'].apply(lambda x: x if x in ['Super Store', 'Web Portal', 'Local', 'Outlet', 'Mall Kiosk'] else np.nan) 
      all_stores_df['country_code'] = all_stores_df['country_code'].apply(lambda x: x if x in ['US', 'GB', 'DE'] else np.nan)
      all_stores_df['continent'] = all_stores_df['continent'].apply(lambda x: x.strip('ee') if 'ee' in x else x)
      all_stores_df['continent'] = all_stores_df['continent'].apply(lambda x: x if x in ['Europe', 'America'] else np.nan)
      err_correction_mapping = {'30e': '30', '80R': "80", 'A97': "97", '3n9': "39", 'J78':'78', 'N/A': np.nan, None: np.nan}
      # Correcting Errors in Data
      for column in ['staff_numbers', 'latitude', 'longitude']:
            all_stores_df[column] = all_stores_df[column].replace(err_correction_mapping)
      # Dropping Null Values
      all_stores_df.dropna(subset = ['country_code'], inplace = True)
      
      # Convert data types
      all_stores_df['address'] = all_stores_df['address'].astype('string')
      all_stores_df['longitude'] = pd.to_numeric(all_stores_df['longitude'], errors = 'coerce')
      all_stores_df['locality'] = all_stores_df['locality'].astype('string')
      all_stores_df['store_code'] = all_stores_df['store_code'].astype('string')
      all_stores_df['staff_numbers'] = pd.to_numeric(all_stores_df['staff_numbers'], errors = 'coerce')
      all_stores_df['store_type'] = all_stores_df['store_type'].astype('string')
      all_stores_df['latitude'] = pd.to_numeric(all_stores_df['latitude'], errors = 'coerce')
      all_stores_df['country_code'] = all_stores_df['country_code'].astype('string')
      all_stores_df['continent'] = all_stores_df['continent'].astype('string')
      
      # Reset index
      all_stores_df.reset_index(drop = True, inplace = True)
      return all_stores_df

   # Convert product weights
   def convert_product_weights(self, s3_data_w):
      # Extract numeric values and remove 'kg' from values already in kilograms
      s3_data_w['weight'] = s3_data_w['weight'].str.replace('kg', '')
      
      # Converting 'g' or 'ml' to 'kg'
      def convert(data):
         data_s = str(data)
         num_ls = [int(num) for num in re.findall(r'\d+', data_s)]
         data_m = np.prod(num_ls) / 1000
         return data_m
      
      # Also converting data like '8 x 100g' to 'kg'
      s3_data_w['weight'] = s3_data_w['weight'].apply(lambda x: convert(x) if 'g' in str(x) or 'ml' in str(x) or 'x' in str(x) else x)
      
      # Convert Data Types
      s3_data_w['weight'] = pd.to_numeric(s3_data_w['weight'], errors = 'coerce')
      return s3_data_w

   # Clean products data
   def clean_products_data(self, s3_data):

      # Drop rows with missing values in specified columns
      s3_data.dropna(subset = ['product_name', 'uuid'], inplace = True)
      s3_data.set_index('Unnamed: 0', inplace= True)
      
      # Cleaning operations on string columns
      s3_data['product_name'] = s3_data['product_name'].str.replace('NULL', '')
      s3_data['product_price'] = s3_data['product_price'].str.replace('£', '')
      s3_data['category'] = s3_data['category'].apply(lambda x: x if x in ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty', 'food-and-drink', 'diy'] else np.nan)
      s3_data['uuid'] = np.where(s3_data['uuid'].str.match(r'^[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}$'), s3_data['uuid'], np.nan)
      # Correcting spelling errors
      s3_data['removed'] = s3_data['removed'].str.replace('avaliable', 'available')
      s3_data['removed'] = s3_data['removed'].apply(lambda x: x if x in ['Still_available', 'Removed'] else np.nan)
      s3_data['product_code'] = np.where(s3_data['product_code'].str.contains('-'), s3_data['product_code'], np.nan)
      s3_data.dropna(subset = ['product_code'], inplace = True)

      # Convert data types
      s3_data['product_name'] = s3_data['product_name'].astype('string')
      s3_data['product_price'] = pd.to_numeric(s3_data['product_price'], errors = 'coerce')
      s3_data['category'] = s3_data['category'].astype('string')
      s3_data['EAN'] = pd.to_numeric(s3_data['EAN'], errors = 'coerce')
      s3_data['date_added'] = pd.to_datetime(s3_data['date_added'], errors='coerce')
      s3_data['uuid'] = s3_data['uuid'].astype('string')
      s3_data['removed'] = s3_data['removed'].astype('string')
      s3_data['product_code'] = s3_data['product_code'].astype('string')

      # Reset index
      s3_data = s3_data.reset_index(drop=True)
      return s3_data

   # Clean orders data
   def clean_orders_data(self, data):

      # Drop specified columns
      data = data.drop(['first_name', 'last_name', '1', 'level_0'], axis = 1)
      data.set_index('index', inplace= True)

      # Cleaning operations on string columns
      data['date_uuid'] = np.where(data['date_uuid'].str.contains('-'), data['date_uuid'], np.nan)
      data['user_uuid'] = np.where(data['user_uuid'].str.contains('-'), data['user_uuid'], np.nan)
      data['store_code'] = np.where(data['store_code'].str.contains('-'), data['store_code'], np.nan)
      data['product_code'] = np.where(data['product_code'].str.contains('-'), data['product_code'], np.nan)
      # Dropping Variables for easy Foreign Key Generation
      data.dropna(subset = ['card_number', 'date_uuid', 'product_code', 'store_code', 'user_uuid'], inplace = True)

      # Convert data types
      data['date_uuid'] = data['date_uuid'].astype('string')
      data['user_uuid'] = data['user_uuid'].astype('string')
      data['store_code'] = data['store_code'].astype('string')
      data['product_code'] = data['product_code'].astype('string')

      # Reset index
      data = data.reset_index(drop=True)
      return data

   # Clean events data
   def clean_events_data(self, s3_d_data):

      # Convert columns to appropriate data types
      s3_d_data['timestamp'] = pd.to_datetime(s3_d_data['timestamp'], format = '%H:%M:%S', errors = 'coerce').dt.time
      s3_d_data['month'] = pd.to_numeric(s3_d_data['month'], errors = 'coerce')
      s3_d_data['year'] = pd.to_numeric(s3_d_data['year'], errors = 'coerce')
      s3_d_data['day'] = pd.to_numeric(s3_d_data['day'], errors = 'coerce')
      s3_d_data['time_period'] = s3_d_data['time_period'].apply(lambda x: x if x in ['Evening', 'Morning', 'Late_Hours', 'Midday'] else np.nan)
      s3_d_data['date_uuid'] = np.where(s3_d_data['date_uuid'].str.contains('-'), s3_d_data['date_uuid'], np.nan)
      s3_d_data.dropna(inplace = True)

      # Convert data types
      s3_d_data['time_period'] = s3_d_data['time_period'].astype('string')
      s3_d_data['date_uuid'] = s3_d_data['date_uuid'].astype('string')

      # Reset index
      s3_d_data = s3_d_data.reset_index(drop=True)
      return s3_d_data