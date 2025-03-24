import os
import queue
import threading
import logging
import json
from datetime import datetime
from pathlib import Path

from chessdotcom import (
    Client,
    get_player_games_by_month_pgn,
    get_player_game_archives,
    get_titled_players
)

# Ensure directories exist
def ensure_directories(data_dir='./data'):
    """Create necessary directories if they don't exist."""
    Path("./logs").mkdir(parents=True, exist_ok=True)
    Path("./config").mkdir(parents=True, exist_ok=True)
    Path(data_dir).mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("./logs/chess_extraction.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("chess_extractor")

def configure_client():
    """
    Configure the chess.com API client with appropriate headers and rate limits.
    """
    Client.request_config["headers"]["User-Agent"] = (
        "CS student research regarding the application of RNNs, Transformers, and Search"
        "Contact me at hickeys@iu.edu"
    )
    Client.rate_limit_handler.tries = 2
    Client.rate_limit_handler.tts = 4

def get_usernames(title_list=None):
    """
    Retrieve usernames of titled chess players from chess.com API.
    
    Parameters
    ----------
    title_list : list, optional
        List of chess titles to fetch. Default is all common titles.
    
    Returns
    -------
    set
        A set of unique usernames of chess players with titles.
    """
    if title_list is None:
        title_list = ['GM', 'IM', 'CM', 'FM', 'WIM', 'WGM', 'WCM', 'WFM']
    
    usernames = []
    for title in title_list:
        try:
            title_players = get_titled_players(title).players
            logger.info(f"Retrieved {len(title_players)} players with title {title}")
            usernames += title_players
        except Exception as e:
            logger.error(f"Error retrieving {title} players: {str(e)}")
    
    return set(usernames)

def worker(q, usernames, extraction_state, data_dir='./data'):
    """
    Process chess games for each username from the queue.
    
    Parameters
    ----------
    q : queue.Queue
        Queue containing usernames to process
    usernames : set
        Set of all usernames for progress calculation
    extraction_state : dict
        Dictionary to track extraction state
    data_dir : str
        Base directory for storing PGN files
    """
    while not q.empty():
        username = q.get()
        user_dir = f'{data_dir}/{username}'
        os.makedirs(user_dir, exist_ok=True)
        
        try:
            archives_response = get_player_game_archives(username)
            archives = archives_response.json['archives']
            
            # Track this username's archives in the state
            if username not in extraction_state:
                extraction_state[username] = {'archives_processed': []}
            
            for archive_url in archives:
                archive_parts = archive_url.split('/')
                year, month = archive_parts[-2], archive_parts[-1]
                file_path = f'{user_dir}/{username}-{year}-{month}.pgn'
                
                archive_id = f"{year}-{month}"
                
                # Skip if already processed
                if archive_id in extraction_state[username]['archives_processed'] and os.path.exists(file_path):
                    continue
                
                try:
                    with open(file_path, 'w') as f:
                        pgn_data = get_player_games_by_month_pgn(username, year, month).text
                        f.write(pgn_data)
                        
                    # Update state after successful extraction
                    extraction_state[username]['archives_processed'].append(archive_id)
                    
                    # Save state periodically
                    if len(extraction_state[username]['archives_processed']) % 5 == 0:
                        save_extraction_state(extraction_state)
                        
                    logger.info(f"Extracted games for {username} - {year}-{month}")
                except Exception as e:
                    logger.error(f"Error extracting games for {username} - {year}-{month}: {str(e)}")
            
            # Display progress
            progress = (1 - round(q.qsize() / len(usernames), 5)) * 100
            logger.info(f"Progress: {progress:.2f}% | Queue remaining: {q.qsize()}")
            
        except Exception as e:
            logger.error(f"Error processing user {username}: {str(e)}")
        
        q.task_done()

def initialize_queue(usernames):
    """Create and populate a queue with usernames."""
    q = queue.Queue()
    for username in usernames:
        q.put(username)
    return q

def load_extraction_state(state_file='./config/extraction_state.json'):
    """Load the extraction state from a file."""
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading extraction state: {str(e)}")
    return {}

def save_extraction_state(state, state_file='./config/extraction_state.json'):
    """Save the extraction state to a file."""
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, 'w') as f:
            json.dump(state, f)
    except Exception as e:
        logger.error(f"Error saving extraction state: {str(e)}")

def extraction(data_dir='./data', thread_count=4, title_list=None, resume=True):
    """
    Main function that orchestrates the chess game data extraction process.
    
    Parameters
    ----------
    data_dir : str
        Directory to store extracted PGN files
    thread_count : int
        Number of parallel download threads
    title_list : list, optional
        List of chess titles to fetch
    resume : bool
        Whether to resume extraction from previous state
    
    Returns
    -------
    dict
        The final extraction state
    """
    # Create necessary directories
    ensure_directories(data_dir)
    
    # Configure the API client
    configure_client()
    
    # Load previous extraction state if resuming
    extraction_state = load_extraction_state() if resume else {}
    
    # Get usernames to process
    usernames = get_usernames(title_list)
    logger.info(f"Retrieved {len(usernames)} unique usernames to process")
    
    # Initialize and populate the queue
    q = initialize_queue(usernames)
    
    # Start worker threads
    threads = []
    for i in range(thread_count):
        t = threading.Thread(
            target=worker, 
            args=(q, usernames, extraction_state, data_dir), 
            daemon=True
        )
        threads.append(t)
        t.start()
    
    # Wait for all tasks to complete
    q.join()
    
    # Save final state
    save_extraction_state(extraction_state)
    
    logger.info("Extraction completed successfully")
    return extraction_state

if __name__ == "__main__":
    extraction(data_dir='./data')