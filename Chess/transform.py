import chess.pgn as pgn
import hashlib
import logging
import os
import json
import pandas as pd


from operator import add
from pathlib import Path
from typing import Optional


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
        return []
        
    if len(games) < 1:
        return []
# TODO: from here, return games, split the rest into separate function
    result = pd.DataFrame.from_dict(games)
    
    # Process date and time fields
    for attr in ['START', 'END', 'UTC']:  
        date, time = attr + '_DATE', attr + '_TIME'
        if date in result.columns and result[date].notna().any():
            if time in result.columns and result[time].notna().any():
                try:
                    result[time] = result[date].str.cat(result[time], sep=' ')
                except Exception as e:
                    logger.warning(f"Could not convert {time} column: {str(e)}")

            try:
                # Convert date to ISO format
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
    
    return result.to_dict(orient='records')


def load_staging_state(state_file='./config/staging_state.json'):
    """Load the staging state from a file."""
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading staging state: {str(e)}")
    return {}

def save_staging_state(state, state_file='./config/staging_state.json'):
    """Save the staging state to a file."""
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, 'w') as f:
            json.dump(state, f)
    except Exception as e:
        logger.error(f"Error saving staging state: {str(e)}")


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
    
    file_paths = [str(file_path) for file_path in data_folder.glob('**/*.pgn')]
    logger.info(f"Found {len(file_paths)} PGN files to process")

    # load staging state and dead letter state
    # TODO: add a check to see if the file exists before loading, similar to extract.py
    staging_state = load_staging_state()
    dead_letter_state = load_staging_state('./config/dead_letter_state.json')
    
    # create a Spark session
    spark = create_spark_session()

    # Process batches of 500 files at a time
    batch_number = 0
    while file_paths:
        batch = file_paths[:500]
        file_paths = file_paths[500:]

        batch_number += 1
        staging_state[batch_number] = {
            "file_paths": batch,
            "status": "processing"
        }
        logger.info(f"Processing batch {batch_number}...")
        try:
            batch_df = spark.createDataFrame(
                spark.sparkContext.parallelize(batch)
                    .map(transform_pgn_to_df)
                    .reduce(add),
                schema=spark.table('local.chess.staging').schema
            )
            batch_df.write.mode("append").saveAsTable(f"local.chess.staging")
            logger.info(f"Batch {batch_number} processed successfully")
            staging_state[batch_number]["status"] = "completed"
            batch_df.unpersist()
        except Exception as e:
            logger.error(f"Error processing batch {batch_number}: {str(e)}")
            staging_state[batch_number]["status"] = "failed"
            dead_letter_state[batch_number] = {
                "file_paths": batch,
                "error": str(e)
            }
        finally:
            save_staging_state(staging_state)
            save_staging_state(dead_letter_state, './config/dead_letter_state.json')
            logger.info(f"Staging state saved for batch {batch_number}")

    

if __name__ == "__main__":
    main(data_dir='./data', mode='staging')