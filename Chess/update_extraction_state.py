import os
import json
import logging
from datetime import datetime
from pathlib import Path
import re

# Create necessary directories
def ensure_directories():
    """Create necessary directories if they don't exist."""
    Path("./logs").mkdir(parents=True, exist_ok=True)
    Path("./config").mkdir(parents=True, exist_ok=True)

# Configure logging
def configure_logging():
    """Configure logging to write to the logs directory."""
    log_file = "./logs/extraction_state_update.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger("extraction_state_updater")

def load_extraction_state(state_file='./config/extraction_state.json'):
    """Load the extraction state from a file or return empty dict if not exists"""
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading extraction state: {str(e)}")
            return {}
    return {}

def save_extraction_state(state, state_file='./config/extraction_state.json'):
    """Save the extraction state to a file"""
    try:
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
        logger.info(f"Saved extraction state to {state_file}")
    except Exception as e:
        logger.error(f"Error saving extraction state: {str(e)}")

def update_extraction_state_from_files(data_dir='./data', state_file='./config/extraction_state.json'):
    """
    Update extraction state by scanning all PGN files in data directory
    
    Parameters
    ----------

    data_dir : str
        Directory containing PGN files
    state_file : str
        Path to the extraction state file
    
    Returns
    -------
    dict
        Updated extraction state
    """
    # Setup
    ensure_directories()
    logger = configure_logging()
    logger.info("Starting extraction state update")
    
    # Load current extraction state
    extraction_state = load_extraction_state(state_file)
    
    # Get all PGN files in the data directory and subdirectories
    data_folder = Path(data_dir)
    pgn_files = list(data_folder.glob('**/*.pgn'))
    
    logger.info(f"Found {len(pgn_files)} PGN files in {data_dir}")
    
    # Regular expression to extract username, year, month from filename
    # Expected format: username-year-month.pgn
    pattern = r'(.+)-(\d{4})-(\d{1,2})\.pgn$'
    
    file_count = 0
    user_count = 0
    
    # Process each PGN file
    for file_path in pgn_files:
        file_name = file_path.name
        match = re.match(pattern, file_name)
        
        if match:
            username, year, month = match.groups()
            archive_id = f"{year}-{month}"
            
            # Initialize username entry if not exists
            if username not in extraction_state:
                extraction_state[username] = {"archives_processed": []}
                user_count += 1
            
            # Add archive_id if not already in the list
            if archive_id not in extraction_state[username]["archives_processed"]:
                extraction_state[username]["archives_processed"].append(archive_id)
                file_count += 1
        else:
            logger.warning(f"Skipping file with unrecognized format: {file_name}")
    
    # Save updated state
    save_extraction_state(extraction_state, state_file)
    
    logger.info(f"Updated extraction state with {file_count} new archives for {user_count} users")
    return extraction_state

if __name__ == "__main__":
    logger.info("Starting extraction state update")
    
    # Update extraction state from files in data directory
    update_extraction_state_from_files(
        data_dir='./data', 
        state_file='./config/extraction_state.json'
    )
    
    logger.info("Extraction state update completed")
