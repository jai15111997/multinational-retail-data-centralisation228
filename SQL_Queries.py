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
            q1 = connection.execute(text('''SELECT country_code, COUNT(country_code) AS total_no_stores FROM dim_store_details
                                            GROUP BY country_code
                                            ORDER BY total_no_stores DESC'''))
            results = q1.fetchall()
            for row in results:
                print(row)

            print('\nWhich locations currently have the most stores?\n')
            q2 =connection.execute(text('''SELECT locality, COUNT(locality) AS total_no_stores
                                           FROM dim_store_details
                                           GROUP BY locality
                                           ORDER BY total_no_stores DESC
                                           LIMIT 7'''))
            results = q2.fetchall()
            for row in results:
                print(row)

            print('\nWhich months produced the largest amount of sales?\n')
            q3 =connection.execute(text('''SELECT ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::numeric, 2) AS total_sales, dim_date_times.month
                                           FROM orders_table
                                           INNER JOIN dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
                                           INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
                                           GROUP BY dim_date_times.month
                                           ORDER BY total_sales DESC
                                           LIMIT 6;'''))
            results = q3.fetchall()
            for row in results:
                print(row)

            print('\nHow many sales are coming from online?\n')
            q4 =connection.execute(text('''SELECT COUNT(orders_table.product_code) AS number_of_sales, SUM(orders_table.product_quantity) AS product_quantity_count,
                                           CASE 
                                           WHEN dim_store_details.store_type = 'Web Portal' THEN 'Web'
                                           ELSE 'Offline'
                                           END as location
                                           FROM orders_table
                                           INNER JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
                                           INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
                                           GROUP BY location
                                           ORDER BY number_of_sales;'''))
            results = q4.fetchall()
            for row in results:
                print(row)

            print('\nWhat percentage of sales come through each type of store?\n')
            q5 =connection.execute(text('''SELECT dim_store_details.store_type, ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::numeric, 2) AS total_sales,
                                           ROUND(((SUM(dim_products.product_price * orders_table.product_quantity) * 100) / SUM(SUM(dim_products.product_price * orders_table.product_quantity)) OVER ())::numeric, 2) AS "percentage_total(%)"
                                           FROM orders_table
                                           INNER JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
                                           INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
                                           GROUP BY dim_store_details.store_type
                                           ORDER BY total_sales DESC;'''))
            
            results = q5.fetchall()
            for row in results:
                print(row)

            print('\nWhich month in each year produced the highest cost of sales?\n')
            q6 =connection.execute(text('''SELECT ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::numeric, 2) AS total_sales, dim_date_times.year, dim_date_times.month
                                           FROM orders_table
                                           INNER JOIN dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
                                           INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
                                           GROUP BY dim_date_times.year, dim_date_times.month
                                           ORDER BY total_sales DESC
                                           LIMIT 10;'''))
            results = q6.fetchall()
            for row in results:
                print(row)

            print('\nWhat is our staff headcount?\n')
            q7 =connection.execute(text('''SELECT SUM(staff_numbers) AS total_staff_numbers, country_code
                                           FROM dim_store_details
                                           GROUP BY country_code
                                           ORDER BY total_staff_numbers DESC;'''))
            results = q7.fetchall()
            for row in results:
                print(row)

            print('\nWhich German store type is selling the most?\n')
            q8 =connection.execute(text('''SELECT ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::numeric, 2) AS total_sales, dim_store_details.store_type, dim_store_details.country_code 
                                           FROM orders_table
                                           INNER JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
                                           INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
                                           WHERE dim_store_details.country_code = 'DE'
                                           GROUP BY dim_store_details.store_type, dim_store_details.country_code
                                           ORDER BY total_sales;'''))
            results = q8.fetchall()
            for row in results:
                print(row)

            print('\nHow quickly is the company making sales?\n')
            q9 =connection.execute(text('''WITH comb_date AS 
                                           (
                                           SELECT year,
                                           CAST(CONCAT( year,'-', month, '-', day, ' ', dim_date_times.timestamp) AS timestamp) AS full_date
                                           FROM dim_date_times 
                                           INNER JOIN orders_table ON dim_date_times.date_uuid = orders_table.date_uuid
                                           ORDER BY full_date
                                           )
                                           SELECT year,
                                           (
                                           CONCAT('hours: ', CAST(ROUND(AVG(EXTRACT(HOUR FROM t_diff))) AS text), 
                                            ', minutes: ', CAST(ROUND(AVG(EXTRACT(MINUTE FROM t_diff))) AS text), 
                                            ', seconds: ', CAST(ROUND(AVG(EXTRACT(SECOND FROM t_diff))) AS text))
                                           ) AS actual_time_taken
                                           FROM
                                           (
                                           SELECT year, full_date, LEAD(full_date) OVER (PARTITION BY year) AS next_date,
                                           LEAD (full_date) OVER (PARTITION BY year) - full_date AS t_diff
                                           FROM comb_date
                                           )
                                           GROUP BY year
                                           ORDER BY actual_time_taken DESC
                                           LIMIT 5;'''))
            results = q9.fetchall()
            for row in results:
                print(row)