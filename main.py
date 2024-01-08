# Importing necessary modules
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from SQL_Operations import SQL_datatype_change
from SQL_Queries import SQL_queries
import pandas as pd

# Initialise instances of classes
dbc = DatabaseConnector()
d_ext = DataExtractor()
d_cl = DataCleaning()
sql_dtype = SQL_datatype_change()
sql_q_a = SQL_queries()

# Set up Variables
pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
store_ep = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
no_st_ep = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
s3_address = 's3://data-handling-public/products.csv'
s3_d_event = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

#Extract and clean card details from PDF
card_details = d_ext.retrieve_pdf_data(pdf_path)
card_df = pd.concat(card_details, ignore_index=True)
card_refined = d_cl.clean_card_data(card_df)
# Upload cleaned data to 'dim_card_details' table
dbc.upload_to_db(card_refined, 'dim_card_details')

# Extract and clean user data from the 'legacy_users' table in the database

ls = d_ext.list_db_tables()
for table in ls:

    if table == 'legacy_users':
        data = d_ext.read_rds_table(dbc, table)
        ref_data = d_cl.clean_user_data(data)
        # Upload the cleaned data to the 'dim_users' table
        dbc.upload_to_db(ref_data, 'dim_users')
    
    # Extract and clean order data from the 'orders_table' table in the database
    elif table == 'orders_table':
        data = d_ext.read_rds_table(dbc, table)
        ref_data = d_cl.clean_orders_data(data)
        # Upload the cleaned data to the 'orders_table' table
        dbc.upload_to_db(ref_data, 'orders_table')

# Extract store details from the API as a list
no_st_dict = d_ext.list_number_of_stores(no_st_ep, header)
# Creating Dataframe
all_stores_df = pd.DataFrame()
for count in range(0, no_st_dict):
    store_ep_n = store_ep + str(count)
    st_data = d_ext.retrieve_stores_data(store_ep_n, header)
    # Combining all records into one dataframe
    all_stores_df = all_stores_df._append(st_data, ignore_index = True)
all_stores_df.set_index('index', inplace = True)

# Cleaning the store data
ref_data = d_cl.called_clean_store_data(all_stores_df)

# Upload cleaned data to the 'dim_store_details' table
dbc.upload_to_db(ref_data, 'dim_store_details')

# Extract and clean product data from S3
s3_data = d_ext.extract_from_s3(s3_address)
# Converting 'ml' and 'g' to 'kg'
s3_ref = d_cl.convert_product_weights(s3_data)
# Cleaning products data
s3_clean = d_cl.clean_products_data(s3_ref)
# Upload cleaned data to 'dim_products' table
dbc.upload_to_db(s3_clean, 'dim_products')

# Extract and clean event data from S3
s3_d_data = d_ext.extract_from_s3(s3_d_event)
s3_d_clean = d_cl.clean_events_data(s3_d_data)
# Upload cleaned data to 'dim_date_times' table
dbc.upload_to_db(s3_d_clean, 'dim_date_times')

# Perform SQL operations to change data types in the database
sql_dtype.dtype_change()

# Execute SQL queries for data analysis
sql_q_a.QnA()