# Import Necessary Packages
from database_utils import DatabaseConnector
from sqlalchemy import create_engine
from sqlalchemy import text

# Check if this script is the main entry point
if __name__ == "__main__":
    print('Run main.py first!')

# Creating Instance for the DatabaseConnector Class
dbc = DatabaseConnector()

class SQL_queries:

    def QnA(self):
        
        # Read database credentials
        cred = dbc.read_db_creds()
        DBAPI = 'psycopg2'
        DATABASE_TYPE = 'postgresql'
        # Creating Engine
        eng = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{cred['USER']}:{cred['PASSWORD']}@{cred['HOST']}:{cred['PORT']}/{cred['DATABASE']}")
        # Connect to the database and execute SQL queries
        with eng.execution_options(isolation_level='AUTOCOMMIT').connect() as connection:
            
            # Query 1
            print('How many stores does the business have and in which countries?\n')
            q1 = connection.execute(text('''SELECT country_code, COUNT(country_code) AS total_no_stores FROM dim_store_details
                                            WHERE address IS NOT NULL
                                            GROUP BY country_code
                                            ORDER BY total_no_stores DESC'''))
            results = q1.fetchall()
            for row in results:
                print(row)

            # Query 2
            print('\nWhich locations currently have the most stores?\n')
            q2 =connection.execute(text('''SELECT locality, COUNT(locality) AS total_no_stores
                                           FROM dim_store_details
                                           GROUP BY locality
                                           ORDER BY total_no_stores DESC
                                           LIMIT 7'''))
            # Fetching Query Results
            results = q2.fetchall()
            # Printing Each Row
            for row in results:
                print(row)

            # Query 3
            print('\nWhich months produced the largest amount of sales?\n')
            q3 =connection.execute(text('''SELECT ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::numeric, 2) AS total_sales, dim_date_times.month
                                           FROM orders_table
                                           INNER JOIN dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
                                           INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
                                           GROUP BY dim_date_times.month
                                           ORDER BY total_sales DESC
                                           LIMIT 6;'''))
            # Fetching Query Results
            results = q3.fetchall()
            # Printing Each Row
            for row in results:
                print(row)

            # Query 4
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
            # Fetching Query Results
            results = q4.fetchall()
            # Printing Each Row
            for row in results:
                print(row)

            # Query 5
            print('\nWhat percentage of sales come through each type of store?\n')
            q5 =connection.execute(text('''SELECT dim_store_details.store_type, ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::numeric, 2) AS total_sales,
                                           ROUND(((SUM(dim_products.product_price * orders_table.product_quantity) * 100) / SUM(SUM(dim_products.product_price * orders_table.product_quantity)) OVER ())::numeric, 2) AS "percentage_total(%)"
                                           FROM orders_table
                                           INNER JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
                                           INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
                                           GROUP BY dim_store_details.store_type
                                           ORDER BY total_sales DESC;'''))
            # Fetching Query Results
            results = q5.fetchall()
            # Printing Each Row
            for row in results:
                print(row)

            # Query 6
            print('\nWhich month in each year produced the highest cost of sales?\n')
            q6 =connection.execute(text('''SELECT ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::numeric, 2) AS total_sales, dim_date_times.year, dim_date_times.month
                                           FROM orders_table
                                           INNER JOIN dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
                                           INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
                                           GROUP BY dim_date_times.year, dim_date_times.month
                                           ORDER BY total_sales DESC
                                           LIMIT 10;'''))
            # Fetching Query Results
            results = q6.fetchall()
            # Printing Each Row
            for row in results:
                print(row)

            # Query 7
            print('\nWhat is our staff headcount?\n')
            q7 =connection.execute(text('''SELECT SUM(staff_numbers) AS total_staff_numbers, country_code
                                           FROM dim_store_details
                                           GROUP BY country_code
                                           ORDER BY total_staff_numbers DESC;'''))
            # Fetching Query Results
            results = q7.fetchall()
            # Printing Each Row
            for row in results:
                print(row)

            # Query 8
            print('\nWhich German store type is selling the most?\n')
            q8 =connection.execute(text('''SELECT ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::numeric, 2) AS total_sales, dim_store_details.store_type, dim_store_details.country_code 
                                           FROM orders_table
                                           INNER JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
                                           INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
                                           WHERE dim_store_details.country_code = 'DE'
                                           GROUP BY dim_store_details.store_type, dim_store_details.country_code
                                           ORDER BY total_sales;'''))
            # Fetching Query Results
            results = q8.fetchall()
            # Printing Each Row
            for row in results:
                print(row)

            # Query 9
            print('\nHow quickly is the company making sales?\n')
            q9 =connection.execute(text('''SELECT year, 
                                           CONCAT('hours: ', CAST(ROUND(EXTRACT(HOUR FROM t_diff)) AS text), 
                                           ', minutes: ', CAST(ROUND(EXTRACT(MINUTE FROM t_diff)) AS text), 
                                           ', seconds: ', CAST(ROUND(EXTRACT(SECOND FROM t_diff)) AS text),
                                           ', milliseconds: ', CAST(ROUND(EXTRACT(MILLISECONDS FROM t_diff)) AS text)) AS time_difference
                                           FROM
                                           (
                                           WITH comb_date AS 
                                           (
                                           SELECT year, CAST(CONCAT(year, '-', month, '-', day, ' ', dim_date_times.timestamp) AS timestamp) AS full_date
                                           FROM 
                                           dim_date_times 
                                           ORDER BY 
                                           full_date
                                           ),
                                           comb_date_next AS 
                                           (
                                           SELECT year, CAST(CONCAT(year, '-', month, '-', day, ' ', dim_date_times.timestamp) AS timestamp) AS f_date,
                                           LEAD(CAST(CONCAT(year, '-', month, '-', day, ' ', dim_date_times.timestamp) AS timestamp)) OVER (ORDER BY year, month, day) AS lead_full_date
                                           FROM dim_date_times  
                                           ORDER BY f_date
                                           )
                                           SELECT comb_date.year, AVG(comb_date_next.lead_full_date - comb_date.full_date) AS t_diff
                                           FROM comb_date
                                           INNER JOIN comb_date_next ON comb_date.full_date = comb_date_next.f_date
                                           GROUP BY comb_date.year
                                           ORDER BY t_diff DESC
                                           LIMIT 5);'''))
            # Fetching Query Results
            results = q9.fetchall()
            # Printing Each Row
            for row in results:
                print(row)