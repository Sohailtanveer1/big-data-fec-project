import pandas as pd
import mysql.connector
from mysql.connector import Error
from config_manager import load_config
from Logger import logger

def get_data_from_sql(table_name):

    # Load MySQL configurations
    configs = load_config()
    hostname = configs["mysql_creds"]['hostname']
    database = configs["mysql_creds"]['database']
    port = configs["mysql_creds"]['port']
    username = configs["mysql_creds"]['username']
    password = configs["mysql_creds"]['password']

    try:
        logger.info("Connection to MySQL Server Started")
        connection = mysql.connector.connect(host=hostname, database=database, user=username, password=password, port=port)
        
        if connection.is_connected():
            logger.info("Querying MySQL table started")
            
            # Fetch all records
            query = f"SELECT * FROM {table_name};"
            df = pd.read_sql(query, con=connection)

            logger.info("Querying MySQL table finished")
            return df

    except Error as e:
        logger.error(f"Connection to MySQL or Querying MySQL table failed {str(e)}")

    finally:
        if connection is not None and connection.is_connected():
            connection.close()
            logger.info("MySQL connection is closed.")

