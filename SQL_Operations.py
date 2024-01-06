from database_utils import DatabaseConnector
from sqlalchemy import create_engine
from sqlalchemy import text

if __name__ == "__main__":
    print('Run main.py first!')

dbc = DatabaseConnector()

class SQL_datatype_change:

    def dtype_change(self):
        
        cred = dbc.read_db_creds()
        DBAPI = 'psycopg2'
        DATABASE_TYPE = 'postgresql'
        eng = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{cred['USER']}:{cred['PASSWORD']}@{cred['HOST']}:{cred['PORT']}/{cred['DATABASE']}")
        with eng.execution_options(isolation_level='AUTOCOMMIT').connect() as connection:

            connection.execute(text('''ALTER TABLE orders_table ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;
                                    ALTER TABLE orders_table ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;
                                    ALTER TABLE orders_table ALTER COLUMN card_number TYPE varchar(19);
                                    ALTER TABLE orders_table ALTER COLUMN store_code TYPE varchar(12);
                                    ALTER TABLE orders_table ALTER COLUMN product_code TYPE varchar(12);
                                    ALTER TABLE orders_table ALTER COLUMN product_quantity TYPE smallint;'''))

            connection.execute(text('''ALTER TABLE dim_users ALTER COLUMN first_name TYPE varchar(255);
                                    ALTER TABLE dim_users ALTER COLUMN last_name TYPE varchar(255);
                                    ALTER TABLE dim_users ALTER COLUMN date_of_birth TYPE date;
                                    ALTER TABLE dim_users ALTER COLUMN country_code TYPE varchar(2);
                                    ALTER TABLE dim_users ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;
                                    ALTER TABLE dim_users ALTER COLUMN join_date TYPE date;'''))

            connection.execute(text('''ALTER TABLE dim_store_details ALTER COLUMN longitude TYPE float;
                                    ALTER TABLE dim_store_details ALTER COLUMN locality TYPE varchar(255);
                                    ALTER TABLE dim_store_details ALTER COLUMN store_code TYPE varchar(12);
                                    ALTER TABLE dim_store_details ALTER COLUMN staff_numbers TYPE smallint;
                                    ALTER TABLE dim_store_details ALTER COLUMN opening_date TYPE date;
                                    ALTER TABLE dim_store_details ALTER COLUMN store_type TYPE varchar(255);
                                    ALTER TABLE dim_store_details ALTER COLUMN latitude TYPE float;
                                    ALTER TABLE dim_store_details ALTER COLUMN country_code TYPE varchar(2);
                                    ALTER TABLE dim_store_details ALTER COLUMN continent TYPE varchar(255);'''))

            connection.execute(text('''ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR(14);
                                    UPDATE dim_products
                                    SET weight_class =
                                    CASE
                                            WHEN weight < 2 THEN 'Light'
                                            WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
                                            WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
                                            WHEN weight >= 140 THEN 'Truck_Required'
                                        END;'''))
            
            connection.execute(text('''ALTER TABLE dim_products ALTER COLUMN product_price TYPE float;
                                    ALTER TABLE dim_products ALTER COLUMN weight TYPE float;
                                    ALTER TABLE dim_products ALTER COLUMN "EAN" TYPE varchar(22);
                                    ALTER TABLE dim_products ALTER COLUMN product_code TYPE varchar(12);
                                    ALTER TABLE dim_products ALTER COLUMN date_added TYPE date;
                                    ALTER TABLE dim_products ALTER COLUMN uuid TYPE uuid USING uuid::uuid;
                                    ALTER TABLE dim_products RENAME COLUMN removed TO still_available;
                                    ALTER TABLE dim_products ALTER COLUMN still_available TYPE bool USING
                                    CASE WHEN still_available = 'Still_available' THEN TRUE ELSE FALSE END;
                                    ALTER TABLE dim_products ALTER COLUMN weight_class TYPE varchar(14);'''))
            
            connection.execute(text('''ALTER TABLE dim_date_times ALTER COLUMN month TYPE varchar(2);
                                    ALTER TABLE dim_date_times ALTER COLUMN year TYPE varchar(4);
                                    ALTER TABLE dim_date_times ALTER COLUMN day TYPE varchar(2);
                                    ALTER TABLE dim_date_times ALTER COLUMN time_period TYPE varchar(10);
                                    ALTER TABLE dim_date_times ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;'''))
            
            connection.execute(text('''ALTER TABLE dim_card_details ALTER COLUMN card_number TYPE varchar(22);
                                    ALTER TABLE dim_card_details ALTER COLUMN expiry_date TYPE varchar(19);
                                    ALTER TABLE dim_card_details ALTER COLUMN date_payment_confirmed TYPE date USING date_payment_confirmed::date;'''))

            connection.execute(text('''ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);
                                    ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);
                                    ALTER TABLE dim_products ADD PRIMARY KEY (product_code);
                                    ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code);
                                    ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);'''))
            
            connection.execute(text('''ALTER TABLE orders_table ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);
                                    ALTER TABLE orders_table ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);
                                    ALTER TABLE orders_table ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code);
                                    ALTER TABLE orders_table ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
                                    ALTER TABLE orders_table ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);'''))