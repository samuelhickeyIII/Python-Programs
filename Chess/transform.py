import chess.pgn as pgn
import hashlib
import multiprocessing as mp
import pandas as pd
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Union

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, BooleanType, FloatType
from pyspark.sql.functions import col, lit, current_timestamp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("./logs/chess_transformation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("chess_transformer")

def create_spark_session():
    """Create and return a Spark session configured for Iceberg"""
    from spark_utils import create_spark_session as utils_create_spark_session
    return utils_create_spark_session(app_name="Chess Data Transformer")

def transform_pgn_to_df(file_path) -> Optional[pd.DataFrame]:
    """
    Transform a PGN file into a pandas DataFrame with structured game data.
    
    Parameters
    ----------
    file_path : str or Path
        Path to the PGN file
        
    Returns
    -------
    pd.DataFrame or None
        DataFrame containing structured game data, or None if no games found
    """
    games = []
    try:
        with open(file_path) as f:
            while game := pgn.read_game(f):
                if game:
                    headers = {k.upper(): v for k, v in list(game.headers.items())}
                    games.append({
                        'EVENT':            headers.get('EVENT', None),
                        'SITE':             headers.get('SITE', None),
                        'START_DATE':       headers.get('DATE', None),
                        'ROUND':            headers.get('ROUND', None),
                        'WHITE':            headers.get('WHITE', None),
                        'BLACK':            headers.get('BLACK', None),
                        'RESULT':           headers.get('RESULT', None),
                        'CURRENT_POSITION': headers.get('CURRENTPOSITION', None),
                        'TIMEZONE':         headers.get('TIMEZONE', None),
                        'ECO':              headers.get('ECO', None),
                        'ECO_URL':          headers.get('ECOURL', None),
                        'UTC_DATE':         headers.get('UTCDATE', None),
                        'UTC_TIME':         headers.get('UTCTIME', None),
                        'WHITE_ELO':        int(headers.get('WHITEELO', None)),
                        'BLACK_ELO':        int(headers.get('BLACKELO', None)),
                        'TIME_CONTROL':     headers.get('TIMECONTROL', None),
                        'TERMINATION':      headers.get('TERMINATION', None),
                        'START_TIME':       headers.get('STARTTIME', None),
                        'END_DATE':         headers.get('ENDDATE', None),
                        'END_TIME':         headers.get('ENDTIME', None),
                        'LINK':             headers.get('LINK', None),
                        'MAINLINE':         str(game.mainline()),
                        'TOURNAMENT':       headers.get('TOURNAMENT', None),
                        'VARIANT':          headers.get('VARIANT', None),
                        'FEN':              headers.get('FEN', None),
                        'SETUP':            headers.get('SETUP', None),
                        'MATCH':            headers.get('MATCH', None)
                    })
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        return None
        
    if len(games) < 1:
        return None
        
    result = pd.DataFrame.from_dict(games)
    
    # Process date and time fields
    for attr in ['START', 'END', 'UTC']:  
        date, time = attr + '_DATE', attr + '_TIME'
        if date in result.columns and result[date].notna().any():
            if time in result.columns and result[time].notna().any():
                try:
                    result[time] = pd.to_datetime(
                        result[date].str.cat(result[time], sep=' '), 
                        format='%Y.%m.%d %H:%M:%S'
                    )
                except Exception as e:
                    logger.warning(f"Could not convert {time} column: {str(e)}")

            try:
                result[date] = pd.to_datetime(result[date], format='%Y.%m.%d').dt.date
            except Exception as e:
                logger.warning(f"Could not convert {date} column: {str(e)}")
    
    # Generate a unique hash for each game
    result["GAME_HASH"] = result.apply(
        lambda x: hashlib.sha256(
            str(tuple(x)).encode('utf-8'),
            usedforsecurity=False
        ).hexdigest(),
        axis=1
    )
    
    return result

def create_staging_schema():
    """Create and return the schema for the staging table"""
    return StructType([
        StructField("EVENT", StringType(), True),
        StructField("SITE", StringType(), True),
        StructField("START_DATE", DateType(), True),
        StructField("ROUND", StringType(), True),
        StructField("WHITE", StringType(), True),
        StructField("BLACK", StringType(), True),
        StructField("RESULT", StringType(), True),
        StructField("CURRENT_POSITION", StringType(), True),
        StructField("TIMEZONE", StringType(), True),
        StructField("ECO", StringType(), True),
        StructField("ECO_URL", StringType(), True),
        StructField("UTC_DATE", DateType(), True),
        StructField("UTC_TIME", TimestampType(), True),
        StructField("WHITE_ELO", IntegerType(), True),
        StructField("BLACK_ELO", IntegerType(), True),
        StructField("TIME_CONTROL", StringType(), True),
        StructField("TERMINATION", StringType(), True),
        StructField("START_TIME", TimestampType(), True),
        StructField("END_DATE", DateType(), True),
        StructField("END_TIME", TimestampType(), True),
        StructField("LINK", StringType(), True),
        StructField("MAINLINE", StringType(), True),
        StructField("TOURNAMENT", StringType(), True),
        StructField("VARIANT", StringType(), True),
        StructField("FEN", StringType(), True),
        StructField("SETUP", StringType(), True),
        StructField("MATCH", StringType(), True),
        StructField("GAME_HASH", StringType(), False)
    ])

def create_staging_table(spark, database_name="chess"):
    """
    Create the staging table as an Iceberg table if it doesn't exist.
    
    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    database_name : str
        Name of the database to create the staging table in
    """
    try:
        # Create database if it doesn't exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS local.{database_name}")
        
        # Create an empty dataframe with the staging schema
        schema = create_staging_schema()
        empty_rdd = spark.sparkContext.emptyRDD()
        df = spark.createDataFrame(empty_rdd, schema)
        
        # Create the staging table
        df.writeTo(f"local.{database_name}.staging") \
          .using("iceberg") \
          .createOrReplace()
        
        logger.info(f"Created staging table 'local.{database_name}.staging'")
    except Exception as e:
        logger.error(f"Error creating staging table: {str(e)}")

def load_to_staging(df, spark, database_name="chess", batch_size=1000):
    """
    Load a DataFrame into the staging Iceberg table.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing game data
    spark : SparkSession
        Active Spark session
    database_name : str
        Name of the database
    batch_size : int
        Size of batches for insertion
        
    Returns
    -------
    bool
        True if successful, False otherwise
    """
    if df is None or df.empty:
        return False
        
    try:
        # Convert pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(df, schema=create_staging_schema())
        
        # Write to the staging table using merge to handle duplicates
        spark_df.writeTo(f"local.{database_name}.staging") \
               .using("iceberg") \
               .append()
            
        logger.info(f"Loaded {len(df)} records to staging")
        return True
    except Exception as e:
        logger.error(f"Error loading to staging: {str(e)}")
        return False


def process_pgn_files(file_paths, database_name="chess", batch_size=1000):
    """
    Process PGN files and load them into staging or transform for data vault.
    
    Parameters
    ----------
    file_paths : list
        List of paths to PGN files
    database_name : str
        Database name for Iceberg tables
    mode : str
        Processing mode: 'staging' or 'direct'
    batch_size : int
        Batch size for database operations
        
    Returns
    -------
    dict
        Statistics about the processing
    """
    stats = {
        'files_processed': 0,
        'games_processed': 0,
        'failures': 0
    }
    
    # Create Spark session
    spark = create_spark_session()
    
    # Process files
    for file_path in file_paths:
        try:
            # Transform PGN to DataFrame
            df = transform_pgn_to_df(file_path)
            
            if df is not None:
                games_count = len(df)
                
                # Load to staging
                success = load_to_staging(df, spark, database_name, batch_size=batch_size)
                if success:
                    stats['games_processed'] += games_count
                else:
                    stats['failures'] += 1
                
                logger.info(f"Processed {file_path} - {games_count} games")
            
            stats['files_processed'] += 1
            
            # Log progress periodically
            if stats['files_processed'] % 100 == 0:
                logger.info(
                    f"Progress: Files processed: {stats['files_processed']}, "
                    f"Games: {stats['games_processed']}, Failures: {stats['failures']}"
                )
                
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            stats['failures'] += 1
    
    # Stop Spark session
    spark.stop()
    
    logger.info(
        f"Processing complete. Files: {stats['files_processed']}, "
        f"Games: {stats['games_processed']}, Failures: {stats['failures']}"
    )
    
    return stats

def main(data_dir='./data', mode='staging', num_workers=8, database_name="chess"):
    """
    Main function to run the transformation process.
    
    Parameters
    ----------
    data_dir : str
        Directory containing PGN files
    mode : str
        Processing mode: 'staging' or 'direct'
    num_workers : int
        Number of worker processes for parallel processing
    database_name : str
        Name of the database to use
    """
    # Find all PGN files
    data_folder = Path(data_dir)
    file_paths = list(data_folder.glob('**/*.pgn'))
    file_paths = file_paths[:len(file_paths)//10]  # Limit to 10% of files for testing
    logger.info(f"Found {len(file_paths)} PGN files to process")
    
    # Split files among workers
    file_chunks = [file_paths[i::num_workers] for i in range(num_workers)]

    # Create staging table if needed
    spark = create_spark_session()
    create_staging_table(spark, database_name)
    spark.stop()
    
    # Create a process pool
    with mp.Pool(num_workers) as pool:
        # Start processes
        results = [
            pool.apply_async(
                process_pgn_files, 
                (chunk, database_name)
            ) 
            for chunk in file_chunks
        ]
        
        # Collect results
        stats = {
            'files_processed': 0,
            'games_processed': 0,
            'failures': 0
        }
        
        for result in results:
            worker_stats = result.get()
            stats['files_processed'] += worker_stats['files_processed']
            stats['games_processed'] += worker_stats['games_processed']
            stats['failures'] += worker_stats['failures']
    
    logger.info(
        f"All processing complete. Files: {stats['files_processed']}, "
        f"Games: {stats['games_processed']}, Failures: {stats['failures']}"
    )

if __name__ == "__main__":
    main(data_dir='./data', mode='staging')