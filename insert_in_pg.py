from psycopg2 import connect, extras, Error
import get_all_config as conf
import pandas as pd
from numpy import int32

# Define connection parameters using config file
db_params = conf.get_db_config_by_role('write')
# Define file read path
read_path = conf.get_filepath_by_mode('read')
# Read file from path
df = pd.read_parquet(read_path['filepath'], engine="fastparquet")
# cast data type
df['Amount'].astype(int32)

rows = df.count()
#transform data read from file into tuples for fast inserting to DB
tup =  [tuple(x) for x in df.to_numpy()]

try:
    # Establishing a connection to the database
    connection = connect(**db_params)
    print("Connection to database established successfully.")

    # Creating a cursor object to interact with the database
    cursor = connection.cursor()

    # Truncate target table
    cursor.execute('truncate table t_stg_fin_raw')
    connection.commit()
    
    #prepare insert query
    sql_insert_query = """ 
    INSERT INTO t_stg_fin_raw (accountcode, amount, Emp_name, customer_id) VALUES %s
    """
    # insert into target table
    extras.execute_values(cursor, sql_insert_query, tup)

        # Commit the changes
    connection.commit()
    print(rows + " Records inserted successfully.")

except (Exception, Error) as error:
    print(f"Error connecting to the database: {error}")

finally:
    if connection:
        cursor.close()
        connection.close() #close db connection
        print("Database connection closed.")
