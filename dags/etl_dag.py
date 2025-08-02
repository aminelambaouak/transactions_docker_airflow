"""
ETL Pipeline DAG for Transaction Data

This DAG performs the following tasks:
1. Creates the transactions table in PostgreSQL
2. Extracts data from Mockaroo API using PySpark
3. Loads the cleaned data into PostgreSQL

The pipeline runs daily and processes transaction data for analysis.
"""

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from pendulum import datetime
from datetime import timedelta
import logging

# Define the basic parameters of the DAG
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={
        "owner": "DataTeam", 
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["etl", "transactions", "postgresql", "pyspark"],
)
def transaction_etl_pipeline():
    """Transaction ETL Pipeline DAG"""
    
    @task
    def validate_environment():
        """Validate that all required environment variables are set"""
        import os
        
        required_vars = ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'API_KEY']
        missing_vars = []
        
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        logging.info("All required environment variables are set")
        return True
    
    # Task 1: Validate environment
    validate_env = validate_environment()
    
    # Task 2: Create database table
    create_table_task = BashOperator(
        task_id='create_table',
        bash_command='cd /opt/airflow/etl && python create_table.py',
        depends_on_past=False,
    )
    
    # Task 3: Extract data from API
    extract_data_task = BashOperator(
        task_id='extract_data',
        bash_command='cd /opt/airflow/etl && python extract.py',
        depends_on_past=False,
    )
    
    # Task 4: Load data into PostgreSQL
    load_data_task = BashOperator(
        task_id='load_data',
        bash_command="""cd /opt/airflow/etl && python -c "
import psycopg2
import csv
import os
import logging
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO)

# Load environment
load_dotenv('/opt/airflow/.env')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = os.getenv('DB_PORT', '5432')

# Connect and load data
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, 
    host=DB_HOST, port=DB_PORT
)
cur = conn.cursor()

# Clear existing data
cur.execute('TRUNCATE TABLE transactions;')
logging.info('Cleared existing data from transactions table')

# Load new data
with open('/opt/airflow/clean_data.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        cur.execute('''
            INSERT INTO transactions 
            (transaction_id, user_id, product_id, product_category, quantity, 
             price_per_unit, payment_method, transaction_date, shipping_country, 
             is_returned, total_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            row['transaction_id'], int(row['user_id']), row['product_id'],
            row['product_category'], int(row['quantity']), float(row['price_per_unit']),
            row['payment_method'], row['transaction_date'], row['shipping_country'],
            row['is_returned'].lower() == 'true', float(row['total_price'])
        ))

conn.commit()
cur.close()
conn.close()
logging.info('Data loaded successfully into transactions table')
" """,
        depends_on_past=False,
    )
    
    @task
    def data_quality_check():
        """Perform basic data quality checks"""
        import psycopg2
        import os
        from dotenv import load_dotenv
        
        # Load environment
        load_dotenv('/opt/airflow/.env')
        DB_NAME = os.getenv('DB_NAME')
        DB_USER = os.getenv('DB_USER')
        DB_PASSWORD = os.getenv('DB_PASSWORD')
        DB_HOST = os.getenv('DB_HOST', 'postgres')
        DB_PORT = os.getenv('DB_PORT', '5432')
        
        # Connect to database
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            host=DB_HOST, port=DB_PORT
        )
        cur = conn.cursor()
        
        # Check record count
        cur.execute("SELECT COUNT(*) FROM transactions;")
        count = cur.fetchone()[0]
        
        if count == 0:
            raise ValueError("No data found in transactions table")
        
        logging.info(f"Data quality check passed: {count} records in transactions table")
        
        # Check for null transaction_ids
        cur.execute("SELECT COUNT(*) FROM transactions WHERE transaction_id IS NULL;")
        null_ids = cur.fetchone()[0]
        
        if null_ids > 0:
            logging.warning(f"Found {null_ids} records with null transaction_ids")
        
        cur.close()
        conn.close()
        
        return {"record_count": count, "null_transaction_ids": null_ids}
    
    # Task 5: Data quality check
    quality_check = data_quality_check()
    
    # Define task dependencies
    validate_env >> create_table_task >> extract_data_task >> load_data_task >> quality_check

# Instantiate the DAG
transaction_etl_pipeline()