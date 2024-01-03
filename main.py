from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
import pandas as pd

dbc = DatabaseConnector()
d_ext = DataExtractor()
d_cl = DataCleaning()
pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
store_ep = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
no_st_ep = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
s3_address = 's3://data-handling-public/products.csv'
s3_d_event = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

card_details = d_ext.retrieve_pdf_data(pdf_path)
card_df = pd.concat(card_details, ignore_index=True)
card_refined = d_cl.clean_card_data(card_df)
dbc.upload_to_db(card_refined, 'dim_card_details')

ls = d_ext.list_db_tables()
for table in ls:

    if table == 'legacy_users':
        data = d_ext.read_rds_table(dbc, table)
        ref_data = d_cl.clean_user_data(data)
        dbc.upload_to_db(ref_data, 'dim_users')
    
    elif table == 'orders_table':
        data = d_ext.read_rds_table(dbc, table)
        ref_data = d_cl.clean_orders_data(data)
        dbc.upload_to_db(ref_data, 'orders_table')

no_st_dict = d_ext.list_number_of_stores(no_st_ep, header)
all_stores_df = pd.DataFrame()
for count in range(0, no_st_dict):
    store_ep_n = store_ep + str(count)
    st_data = d_ext.retrieve_stores_data(store_ep_n, header)
    all_stores_df = all_stores_df._append(st_data, ignore_index = True)
all_stores_df.set_index('index', inplace = True)

ref_data = d_cl.called_clean_store_data(all_stores_df)
dbc.upload_to_db(ref_data, 'dim_store_details')

s3_data = d_ext.extract_from_s3(s3_address)
s3_ref = d_cl.convert_product_weights(s3_data)
s3_clean = d_cl.clean_products_data(s3_ref)
dbc.upload_to_db(s3_clean, 'dim_products')

s3_d_data = d_ext.extract_from_s3(s3_d_event)
s3_d_clean = d_cl.clean_events_data(s3_d_data)
dbc.upload_to_db(s3_clean, 'dim_date_times')