from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, BooleanType, FloatType
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime
import logging
import pandas as pd
import os
from pathlib import Path

# Ensure directories exist
def ensure_directories():
    """Create necessary directories if they don't exist."""
    Path("./logs").mkdir(parents=True, exist_ok=True)

# Configure logging
ensure_directories()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("./logs/chess_data_vault.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("chess_data_vault")

def create_spark_session():
    """Create and return a Spark session configured for Iceberg"""
    from spark_utils import create_spark_session as utils_create_spark_session
    return utils_create_spark_session(app_name="Chess Data Vault")

# Define schemas for Data Vault 2.0 components

# Hub schemas (business entity identifiers)
hub_player_schema = StructType([
    StructField("hub_player_key", IntegerType(), False),
    StructField("player_id", StringType(), False),
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

hub_game_schema = StructType([
    StructField("hub_game_key", IntegerType(), False),
    StructField("game_id", StringType(), False),
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

hub_move_schema = StructType([
    StructField("hub_move_key", IntegerType(), False),
    StructField("move_id", StringType(), False),
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

hub_position_schema = StructType([
    StructField("hub_position_key", IntegerType(), False),
    StructField("position_id", StringType(), False),
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

# Add new Hub for events
hub_event_schema = StructType([
    StructField("hub_event_key", IntegerType(), False),
    StructField("event_id", StringType(), False),  # Business key
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

# Add new Hub for chess openings
hub_opening_schema = StructType([
    StructField("hub_opening_key", IntegerType(), False),
    StructField("opening_id", StringType(), False),  # Business key
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

# Link schemas (relationships between business entities)
link_game_player_schema = StructType([
    StructField("link_game_player_key", IntegerType(), False),
    StructField("hub_game_key", IntegerType(), True),
    StructField("hub_player_key", IntegerType(), True),
    StructField("player_color", StringType(), True),
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

link_game_move_schema = StructType([
    StructField("link_game_move_key", IntegerType(), False),
    StructField("hub_game_key", IntegerType(), True),
    StructField("hub_move_key", IntegerType(), True),
    StructField("move_number", IntegerType(), True),
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

link_move_position_schema = StructType([
    StructField("link_move_position_key", IntegerType(), False),
    StructField("hub_move_key", IntegerType(), True),
    StructField("hub_position_key", IntegerType(), True),
    StructField("is_position_after", BooleanType(), True),
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

# Add hierarchical link between events and games
link_event_game_schema = StructType([
    StructField("link_event_game_key", IntegerType(), False),
    StructField("hub_event_key", IntegerType(), True),
    StructField("hub_game_key", IntegerType(), True),
    StructField("hierarchy_level", IntegerType(), True),  # Can be used for rounds or stages
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

# Link between games and openings
link_game_opening_schema = StructType([
    StructField("link_game_opening_key", IntegerType(), False),
    StructField("hub_game_key", IntegerType(), True),
    StructField("hub_opening_key", IntegerType(), True),
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

# Satellite schemas (descriptive attributes)
sat_player_schema = StructType([
    StructField("sat_player_key", IntegerType(), False),
    StructField("hub_player_key", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("rating", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("effective_date", TimestampType(), True),
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

sat_game_schema = StructType([
    StructField("sat_game_key", IntegerType(), False),
    StructField("hub_game_key", IntegerType(), True),
    StructField("event", StringType(), True),
    StructField("site", StringType(), True),
    StructField("date_played", TimestampType(), True),
    StructField("round", StringType(), True),
    StructField("result", StringType(), True),
    StructField("time_control", StringType(), True),
    StructField("effective_date", TimestampType(), True),
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True),
    StructField("variant", StringType(), True),
    StructField("setup", BooleanType(), True),
    StructField("initial_fen", StringType(), True),
    StructField("current_position", StringType(), True), 
    StructField("eco", StringType(), True),
    StructField("eco_url", StringType(), True),
    StructField("termination_type", StringType(), True),
    StructField("game_url", StringType(), True)
])

sat_move_schema = StructType([
    StructField("sat_move_key", IntegerType(), False),
    StructField("hub_move_key", IntegerType(), True),
    StructField("algebraic_notation", StringType(), True),
    StructField("time_spent", FloatType(), True),
    StructField("evaluation", FloatType(), True),
    StructField("is_check", BooleanType(), True),
    StructField("is_capture", BooleanType(), True),
    StructField("piece_moved", StringType(), True),
    StructField("effective_date", TimestampType(), True),
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

sat_position_schema = StructType([
    StructField("sat_position_key", IntegerType(), False),
    StructField("hub_position_key", IntegerType(), True),
    StructField("fen_notation", StringType(), True),
    StructField("evaluation", FloatType(), True),
    StructField("is_check", BooleanType(), True),
    StructField("material_balance", FloatType(), True),
    StructField("effective_date", TimestampType(), True),
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

# Add satellite for event details
sat_event_schema = StructType([
    StructField("sat_event_key", IntegerType(), False),
    StructField("hub_event_key", IntegerType(), True),
    StructField("event_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("start_date", TimestampType(), True),
    StructField("end_date", TimestampType(), True),
    StructField("event_type", StringType(), True),  # Tournament, Match, etc.
    StructField("event_format", StringType(), True),  # Swiss, Round Robin, etc.
    StructField("effective_date", TimestampType(), True),
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

# Satellite for opening details
sat_opening_schema = StructType([
    StructField("sat_opening_key", IntegerType(), False),
    StructField("hub_opening_key", IntegerType(), True),
    StructField("eco_code", StringType(), True),
    StructField("opening_name", StringType(), True),
    StructField("eco_url", StringType(), True),
    StructField("effective_date", TimestampType(), True),
    StructField("load_date", TimestampType(), True),
    StructField("record_source", StringType(), True)
])

def create_iceberg_tables(spark, database_name="chess"):
    """Create all Iceberg tables for the Data Vault 2.0 chess model"""
    
    # Create database if it doesn't exist
    # spark.sql(f"CREATE DATABASE IF NOT EXISTS local.{database_name}")
    
    # Create empty DataFrames with the defined schemas
    empty_rdd = spark.sparkContext.emptyRDD()
    
    # Create and write Hub tables
    tables_to_create = [
        # Table name, schema, partition fields
        ("hub_player", hub_player_schema, []),
        ("hub_game", hub_game_schema, []),
        ("hub_move", hub_move_schema, []),
        ("hub_position", hub_position_schema, []),
        ("hub_event", hub_event_schema, []),  # New hub for events
        ("hub_opening", hub_opening_schema, []),  # New hub for openings
        
        # Link tables - partition game_move by game for query efficiency
        ("link_game_player", link_game_player_schema, []),
        ("link_game_move", link_game_move_schema, ["hub_game_key"]),
        ("link_move_position", link_move_position_schema, []),
        ("link_event_game", link_event_game_schema, ["hub_event_key"]),  # New hierarchical link
        ("link_game_opening", link_game_opening_schema, []),  # New link for game and opening
        
        # Satellite tables - partition by time dimensions
        ("sat_player", sat_player_schema, ["effective_date"]),
        ("sat_game", sat_game_schema, ["date_played"]),
        ("sat_move", sat_move_schema, []),
        ("sat_position", sat_position_schema, []),
        ("sat_event", sat_event_schema, ["start_date"]),  # New satellite for event details
        ("sat_opening", sat_opening_schema, [])  # New satellite for opening details
    ]
    
    for table_name, schema, partition_fields in tables_to_create:
        df = spark.createDataFrame(empty_rdd, schema)
        writer = df.writeTo(f"local.{database_name}.{table_name}").using("iceberg")
        
        # Add partitioning if specified
        if partition_fields:
            writer = writer.partitionedBy(*partition_fields)
        
        # Create the table
        writer.createOrReplace()
        
        print(f"Created table: local.{database_name}.{table_name}")
    
    print(f"Data Vault 2.0 chess model created successfully as Iceberg tables in database '{database_name}'.")

def get_staging_data(spark, database_name="chess", query_limit=None):
    """
    Retrieve data from the staging table using Spark SQL.
    
    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    database_name : str
        Name of the database
    query_limit : int, optional
        Limit the number of rows returned
    
    Returns
    -------
    pyspark.sql.DataFrame
        Spark DataFrame containing staging data
    """
    query = f"SELECT * FROM local.{database_name}.staging"
    if query_limit:
        query += f" LIMIT {query_limit}"
    
    logger.info(f"Retrieving staging data with query: {query}")
    
    staging_df = spark.sql(query)
    
    logger.info(f"Retrieved {staging_df.count()} rows from staging")
    return staging_df

def transform_staging_to_vault(spark, staging_df):
    """
    Transform staging data into Data Vault entities.
    
    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    staging_df : pyspark.sql.DataFrame
        DataFrame containing staging data
    
    Returns
    -------
    dict
        Dictionary of DataFrames for each Data Vault entity
    """
    if staging_df.isEmpty():
        return {}
    
    # Current timestamp for load dates
    current_ts = current_timestamp()
    record_source = lit("staging_import")
    
    # Initialize result dictionary
    vault_dfs = {}
    
    # Create hub_player DataFrame
    white_players = staging_df.select(
        col("WHITE").alias("player_id"),
        current_ts.alias("load_date"),
        record_source.alias("record_source")
    ).distinct()
    
    black_players = staging_df.select(
        col("BLACK").alias("player_id"),
        current_ts.alias("load_date"),
        record_source.alias("record_source")
    ).distinct()
    
    players_df = white_players.union(black_players).distinct()
    vault_dfs["hub_player"] = players_df
    
    # Create hub_game DataFrame
    games_df = staging_df.select(
        col("GAME_HASH").alias("game_id"),
        current_ts.alias("load_date"),
        record_source.alias("record_source")
    ).distinct()
    vault_dfs["hub_game"] = games_df
    
    # Create hub_event DataFrame
    events_df = staging_df.select(
        col("EVENT").alias("event_id"),
        current_ts.alias("load_date"),
        record_source.alias("record_source")
    ).filter(col("EVENT").isNotNull()).distinct()
    vault_dfs["hub_event"] = events_df
    
    # Create hub_opening DataFrame 
    openings_df = staging_df.select(
        col("ECO").alias("opening_id"),
        current_ts.alias("load_date"),
        record_source.alias("record_source")
    ).filter(col("ECO").isNotNull()).distinct()
    vault_dfs["hub_opening"] = openings_df
    
    # Create various links and satellites
    # This would be expanded to include all the entities
    
    return vault_dfs

def load_to_vault(spark, vault_dfs, database_name="chess"):
    """
    Load transformed data into the Data Vault model.
    
    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    vault_dfs : dict
        Dictionary of DataFrames for each Data Vault entity
    database_name : str
        Name of the Iceberg database
    
    Returns
    -------
    bool
        True if successful, False otherwise
    """
    try:
        for table_name, df in vault_dfs.items():
            # Add appropriate surrogate keys if needed
            if table_name.startswith("hub_"):
                key_col = f"{table_name}_key"
                if key_col not in df.columns:
                    # Use row_number to generate surrogate keys
                    df = df.withColumn(key_col, spark.sql("SELECT row_number() OVER (ORDER BY 1)"))
            
            # Write to Iceberg table
            df.writeTo(f"local.{database_name}.{table_name}") \
              .using("iceberg") \
              .append()
            
            logger.info(f"Loaded {df.count()} rows to {table_name}")
            
        return True
    except Exception as e:
        logger.error(f"Error loading to vault: {str(e)}")
        return False

def direct_load_from_pgn(spark, pgn_data, database_name="chess"):
    """
    Load data directly from PGN format into the Data Vault model.
    
    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    pgn_data : list or pandas.DataFrame
        PGN data already transformed into a tabular format
    database_name : str
        Name of the Iceberg database
    
    Returns
    -------
    bool
        True if successful, False otherwise
    """
    try:
        # Convert pandas DataFrame to Spark DataFrame if needed
        if isinstance(pgn_data, pd.DataFrame):
            pgn_df = spark.createDataFrame(pgn_data)
        else:
            # Assume it's already a Spark DataFrame
            pgn_df = pgn_data
        
        # Transform to Data Vault format
        vault_dfs = transform_staging_to_vault(spark, pgn_df)
        
        # Load to vault
        success = load_to_vault(spark, vault_dfs, database_name)
        
        return success
    except Exception as e:
        logger.error(f"Error in direct load from PGN: {str(e)}")
        return False

def main(mode="from_staging", database_name="chess"):
    """
    Main function to run the Data Vault loading process.
    
    Parameters
    ----------
    mode : str
        "from_staging" or "direct" to indicate data source
    database_name : str
        Name of the Iceberg database
    """
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Create Data Vault tables if they don't exist
        create_iceberg_tables(spark, database_name)
        
        # if mode == "from_staging":
        #     # Get staging data
        #     staging_df = get_staging_data(spark, database_name)
            
        #     # Transform to Data Vault format
        #     vault_dfs = transform_staging_to_vault(spark, staging_df)
            
        #     # Load to vault
        #     success = load_to_vault(spark, vault_dfs, database_name)
            
        #     if success:
        #         logger.info("Successfully loaded data from staging to Data Vault")
        #     else:
        #         logger.error("Failed to load data from staging to Data Vault")
        # else:
        #     logger.info("Direct loading mode requires calling direct_load_from_pgn function")
            
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main(mode="from_staging")
