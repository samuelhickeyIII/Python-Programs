import argparse
import logging
import os
from datetime import datetime
from pathlib import Path

# Import our modules
from extract import extraction
from transform import process_pgn_files
from data_vault_model import main as vault_main, direct_load_from_pgn

# Import the new utilities
from spark_utils import create_spark_session

# Ensure directories exist
def ensure_directories():
    """Create necessary directories if they don't exist."""
    Path("./logs").mkdir(parents=True, exist_ok=True)
    Path("./config").mkdir(parents=True, exist_ok=True)
    Path("./data").mkdir(parents=True, exist_ok=True)

# Configure logging
ensure_directories()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("./logs/chess_etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("chess_etl_orchestrator")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Chess data ETL orchestrator")
    
    # General arguments
    parser.add_argument(
        "--mode", 
        choices=["full", "extract_only", "transform_only", "load_only"], 
        default="full",
        help="ETL mode to run"
    )
    
    parser.add_argument(
        "--data-dir", 
        default="./Chess/data",
        help="Directory for PGN files"
    )
    
    # Extract-specific arguments
    parser.add_argument(
        "--title-list", 
        nargs="+", 
        default=None,
        help="List of chess titles to extract (e.g. GM IM)"
    )
    
    parser.add_argument(
        "--thread-count", 
        type=int, 
        default=4,
        help="Number of threads for extraction"
    )
    
    parser.add_argument(
        "--num-workers", 
        type=int, 
        default=4,
        help="Number of workers for transformation"
    )
    
    # Database configuration
    parser.add_argument("--db-name", default="chess", 
                        help="Database name for Iceberg tables")
    
    # Add argument for Iceberg dependency setup
    parser.add_argument(
        "--setup-deps",
        action="store_true",
        help="Run setup for Iceberg dependencies before processing"
    )
    
    return parser.parse_args()

def run_extraction(args):
    """Run the extraction phase."""
    logger.info("Starting extraction phase")
    
    os.makedirs(args.data_dir, exist_ok=True)
    
    extraction_state = extraction(
        data_dir=args.data_dir,
        thread_count=args.thread_count,
        title_list=args.title_list,
        resume=True
    )
    
    logger.info(f"Extraction phase completed. Processed {len(extraction_state)} users")
    return extraction_state

def setup_dependencies():
    """Setup Iceberg dependencies if needed"""
    try:
        from setup_iceberg import setup_iceberg_dependencies
        jars = setup_iceberg_dependencies()
        if jars:
            logger.info(f"Iceberg dependencies setup completed. Found {len(jars)} JAR files.")
            return True
        else:
            logger.error("Failed to setup Iceberg dependencies.")
            return False
    except ImportError:
        logger.error("setup_iceberg module not found. Please run setup_iceberg.py first.")
        return False

def run_transformation(args):
    """Run the transformation phase."""
    logger.info("Starting transformation phase")
    
    # Create Spark session using the utility
    spark = create_spark_session(app_name="Chess Data Transformer")
    
    # Get all PGN files
    data_folder = Path(args.data_dir)
    file_paths = list(data_folder.glob('**/*.pgn'))
    logger.info(f"Found {len(file_paths)} PGN files to process")
    
    # Split files among workers
    file_chunks = [file_paths[i::args.num_workers] for i in range(args.num_workers)]
    
    # Use multiprocessing to process files
    with mp.Pool(args.num_workers) as pool:
        results = [
            pool.apply_async(
                process_pgn_files, 
                (chunk, args.db_name)
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
    
    # Stop Spark session
    spark.stop()
    
    logger.info(
        f"Transformation phase completed. Files: {stats['files_processed']}, "
        f"Games: {stats['games_processed']}, Failures: {stats['failures']}"
    )

def run_loading(args):
    """Run the loading phase."""
    logger.info("Starting loading phase")
    
    vault_main(
        mode="from_staging",
        database_name=args.db_name
    )
    
    logger.info("Loading phase completed")

def main():
    """Main function to run the ETL process."""
    args = parse_args()
    
    start_time = datetime.now()
    logger.info(f"Starting ETL process at {start_time}")
    
    try:
        # Run dependency setup if requested
        if args.setup_deps:
            if not setup_dependencies():
                logger.error("Failed to setup dependencies. Exiting.")
                return
        
        # Run all or selected phases based on mode
        if args.mode in ["full", "extract_only"]:
            run_extraction(args)
            if args.mode == "extract_only":
                return
        
        if args.mode in ["full", "transform_only"]:
            run_transformation(args)
            if args.mode == "transform_only":
                return
        
        if args.mode in ["full", "load_only"]:
            run_loading(args)
    
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        raise
    
    finally:
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"ETL process completed at {end_time}. Duration: {duration}")

if __name__ == "__main__":
    main()
