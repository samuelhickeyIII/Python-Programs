import os
import logging
from pathlib import Path

def ensure_directories():
    """Create necessary directories for the project structure."""
    dirs = [
        "./logs",
        "./config",
        "./data"
    ]
    
    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)

def configure_logger(name, log_file):
    """Configure and return a logger with file and console handlers."""
    # Ensure the logs directory exists
    ensure_directories()
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Create handlers
    file_handler = logging.FileHandler(f"./logs/{log_file}")
    console_handler = logging.StreamHandler()
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def get_config_path(filename):
    """Get full path for a config file and ensure directory exists."""
    config_dir = "./config"
    Path(config_dir).mkdir(parents=True, exist_ok=True)
    return os.path.join(config_dir, filename)

def get_data_path(path_segments=None):
    """Get full path for data files/directories and ensure directory exists."""
    data_dir = "./data"
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    
    if path_segments:
        if isinstance(path_segments, str):
            path_segments = [path_segments]
        return os.path.join(data_dir, *path_segments)
    
    return data_dir
