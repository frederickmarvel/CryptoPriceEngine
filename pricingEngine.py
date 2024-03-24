import os
import requests
import schedule
import time
import pymysql
from pymysql.err import MySQLError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

# Global variable to store max server_time
max_server_txid = 0  # Initialize as 0 instead of None

# Function to get the max server_time from the database
def init_max_txid():
    global max_server_txid
    try:
        conn = pymysql.connect(host=db_host, db=db_name, user=db_user, password=db_password, cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(txid) AS txid FROM btc_idr_price")
        result = cursor.fetchone()
        max_server_txid = result['txid'] if result['txid'] is not None else 0
        cursor.close()
        conn.close()
    except MySQLError as e:
        print(f"Error: {e}")
        max_server_txid = 0  # Ensure max_server_time is an integer even on error

# Function to fetch, process, and insert data
def fetch_and_update():
    global max_server_txid
    response = requests.get("https://indodax.com/api/trades/btcidr")
    data = response.json()
    data.reverse()  # Reorder the array to ascending

    # Filter data
    new_data = [d for d in data if int(d['tid']) > max_server_txid]

    if not new_data:
        return

    try:
        conn = pymysql.connect(host=db_host, db=db_name, user=db_user, password=db_password)
        cursor = conn.cursor()
        
        # Prepare bulk insert query
        insert_query = "INSERT INTO btc_idr_price (price, volume, txid, server_time,source) VALUES (%s, %s, %s, %s, %s)"
        values = [(d['price'], d['amount'],d['tid'], d['date'] ,'indodax') for d in new_data]

        # Execute bulk insert
        cursor.executemany(insert_query, values)
        conn.commit()

        # Update max_server_time in memory
        max_server_txid = int(new_data[-1]['tid'])

        cursor.close()
        conn.close()
    except MySQLError as e:
        print(f"Error: {e}")

# Initialize max server_time
init_max_txid()

# Schedule the fetch_and_update function to run every 10 seconds
schedule.every(10).seconds.do(fetch_and_update)

# Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(1)
