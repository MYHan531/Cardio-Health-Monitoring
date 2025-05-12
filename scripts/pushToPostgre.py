import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def validate_dataframe(df: pd.DataFrame) -> None:
    """
    Validate the DataFrame before pushing to PostgreSQL.
    
    Args:
        df: DataFrame to validate
    """
    if df.empty:
        raise ValueError("DataFrame is empty")
    
    logger.info(f"DataFrame shape: {df.shape}")
    logger.info(f"Columns: {df.columns.tolist()}")
    logger.info(f"Sample data:\n{df.head()}")
    
    # Check for null values
    null_counts = df.isnull().sum()
    if null_counts.any():
        logger.warning(f"Null values found:\n{null_counts[null_counts > 0]}")

def push_to_postgresql():
    try:
        # Get database credentials from environment variables
        DB_USER = os.getenv('DB_USER')
        DB_PASSWORD = os.getenv('DB_PASSWORD')
        DB_HOST = os.getenv('DB_HOST')
        DB_PORT = os.getenv('DB_PORT')
        DB_NAME = os.getenv('DB_NAME')

        # Validate environment variables
        required_vars = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_NAME']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

        # Construct the database URL
        database_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        
        # Create SQLAlchemy engine
        engine = create_engine(database_url)
        
        # Read the data file
        data_path = Path('../data/raw/apple_health_records.parquet')
        if not data_path.exists():
            raise FileNotFoundError(f"Data file not found: {data_path}")
            
        logger.info(f"Reading data from: {data_path}")
        df = pd.read_parquet(data_path)
        
        # Validate the data
        validate_dataframe(df)
        
        # Push to PostgreSQL
        table_name = 'cardio_data'
        logger.info(f"Pushing data to PostgreSQL table '{table_name}'...")
        
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            schema='public',
            method='multi',  # Use multi-insert for better performance
            chunksize=10000  # Process in chunks to manage memory
        )
        
        logger.info(f"Successfully pushed {len(df)} rows to PostgreSQL table '{table_name}'")
        
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    push_to_postgresql()
