import logging
import requests
import shutil
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from dotenv import load_dotenv

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("/opt/airflow/logs/spark_app.log"),
            logging.StreamHandler()
        ]
    )

def load_environment():
    """Load environment variables"""
    # Try to load .env file
    env_path = '/opt/airflow/.env'
    if os.path.exists(env_path):
        load_dotenv(env_path)
        logging.info(f"Loaded .env from {env_path}")
    else:
        logging.info("No .env file found, using system environment variables")
    
    # Access variables
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    API_KEY = os.getenv("API_KEY")
    
    logging.info(f"DB_NAME: {DB_NAME}")
    logging.info(f"DB_USER: {DB_USER}")
    logging.info(f"API_KEY: {'*' * len(API_KEY) if API_KEY else 'Not set'}")
    
    return DB_NAME, DB_USER, DB_PASSWORD, API_KEY

def get_schema():
    """Define the schema for the data"""
    return StructType([
        StructField('transaction_id', StringType(), True),
        StructField('user_id', IntegerType(), True),
        StructField('product_id', StringType(), True),
        StructField('product_category', StringType(), True),
        StructField('quantity', IntegerType(), True),
        StructField('price_per_unit', DoubleType(), True),
        StructField('payment_method', StringType(), True),
        StructField('transaction_date', StringType(), True),
        StructField('shipping_country', StringType(), True),
        StructField('is_returned', BooleanType(), True),
        StructField('total_price', DoubleType(), True)
    ])

def main():
    """Main extraction function"""
    setup_logging()
    logging.info("Starting data extraction process")
    
    # Load environment variables
    DB_NAME, DB_USER, DB_PASSWORD, API_KEY = load_environment()
    
    if not API_KEY:
        logging.error("API_KEY not found in environment variables")
        return False
    
    URL = f"https://my.api.mockaroo.com/trans?key={API_KEY}"
    SCHEMA = get_schema()
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName('TransactionExtraction') \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Fetch data from API
        logging.info(f"Fetching data from: {URL}")
        response = requests.get(URL, timeout=30)
        response.raise_for_status()
        logging.info("Data fetched successfully from API")
        
        # Create DataFrame from JSON response
        json_rdd = spark.sparkContext.parallelize([response.text])
        df = spark.read.schema(SCHEMA).json(json_rdd)
        
        # Add total_price with 2 decimals
        df = df.withColumn('total_price', round(col('price_per_unit') * col('quantity'), 2))
        
        # Log dataframe info
        logging.info(f"DataFrame created with {df.count()} rows")
        df.printSchema()
        
        # Save to CSV in temp directory
        temp_dir = "/opt/airflow/temp_csv"
        df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(temp_dir)
        
        # Move CSV file to final location
        csv_files = [f for f in os.listdir(temp_dir) if f.endswith('.csv')]
        if csv_files:
            csv_file = csv_files[0]
            source_path = os.path.join(temp_dir, csv_file)
            dest_path = "/opt/airflow/clean_data.csv"
            
            shutil.move(source_path, dest_path)
            shutil.rmtree(temp_dir)
            
            logging.info(f"CSV saved to {dest_path}")
            
            # Show sample data
            logging.info("Sample of extracted data:")
            df.show(5, truncate=False)
            
            return True
        else:
            logging.error("No CSV file found in temp directory")
            return False
            
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error during extraction: {e}")
        return False
    finally:
        spark.stop()
        logging.info("Spark session stopped")

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)