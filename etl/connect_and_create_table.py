import psycopg2
import logging
import os
from dotenv import load_dotenv

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("/opt/airflow/logs/create_table.log"),
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
    
    # Get database configuration
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST", "postgres")
    DB_PORT = os.getenv("DB_PORT", "5432")
    
    logging.info(f"DB_NAME: {DB_NAME}")
    logging.info(f"DB_USER: {DB_USER}")
    logging.info(f"DB_HOST: {DB_HOST}")
    logging.info(f"DB_PORT: {DB_PORT}")
    
    return DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT

def connect_and_create_table():
    """Connect to database and create transactions table"""
    setup_logging()
    logging.info("Starting database table creation process")
    
    # Load environment variables
    DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT = load_environment()
    
    # Validate required environment variables
    if not all([DB_NAME, DB_USER, DB_PASSWORD]):
        logging.error("Missing required database environment variables")
        return False
    
    try:
        logging.info("Attempting to connect to the database...")
        
        # Connect to database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            connect_timeout=30
        )
        
        # Set autocommit to True for DDL operations
        conn.autocommit = True
        cur = conn.cursor()
        
        logging.info("Successfully connected to the database")
        
        # Create table query
        create_query = """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR(20) PRIMARY KEY,
            user_id INTEGER,
            product_id VARCHAR(20),
            product_category VARCHAR(100),
            quantity INTEGER,
            price_per_unit DECIMAL(10,2),
            payment_method VARCHAR(50),
            transaction_date DATE,
            shipping_country VARCHAR(50),
            is_returned BOOLEAN,
            total_price DECIMAL(10,2)
        );
        """
        
        logging.info("Creating table 'transactions' if it doesn't exist...")
        cur.execute(create_query)
        logging.info("Table 'transactions' created successfully")
        
        # Verify table creation by checking if it exists
        check_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = 'transactions';
        """
        
        cur.execute(check_query)
        result = cur.fetchone()
        
        if result:
            logging.info("Table 'transactions' verified in database")
            
            # Get table structure
            structure_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'transactions' 
            ORDER BY ordinal_position;
            """
            
            cur.execute(structure_query)
            columns = cur.fetchall()
            
            logging.info("Table structure:")
            for column in columns:
                logging.info(f"  {column[0]}: {column[1]} (nullable: {column[2]})")
                
        else:
            logging.error("Table 'transactions' was not created successfully")
            return False
        
        # Close connections
        cur.close()
        conn.close()
        logging.info("Database connection closed successfully")
        
        return True
        
    except psycopg2.OperationalError as e:
        logging.error(f"Database connection error: {e}")
        return False
    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return False

if __name__ == "__main__":
    success = connect_and_create_table()
    if not success:
        exit(1)