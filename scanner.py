##############################################################################
# scanner.py
##############################################################################
import argparse
import concurrent.futures
import os
import sys
import signal
import multiprocessing
import time
from typing import List, Callable
import fnmatch  # for matching file patterns
from datetime import datetime
from dataclasses import dataclass

from source.logging_modules import CustomLogger
from source.database_modules import DatabaseConnection, DatabaseManager
from source.hash_modules import HashCalculator
from source.fingerprint_modules import VideoFingerprinter
from source.yolo_modules import YoloProvider
from source.scanner_modules import Scanner

# ------------------------------------------------------------------------
# GLOBALS
# ------------------------------------------------------------------------
global_executor = None   # For shutting down the ProcessPoolExecutor
stop_flag = False        # For single-process immediate stop

def handle_sigint(sig, frame):
    """
    Handle keyboard interrupt (SIGINT) by shutting down the executor gracefully
    and setting stop_flag, so single-process can also exit quickly.
    """
    logger = CustomLogger(__name__).get_logger()
    logger.warning("Keyboard interrupt in main detected.")

    # 1) Set stop_flag so single-process scanning can stop
    global stop_flag
    stop_flag = True

    # 2) If multi-process is running, shut down the pool
    if global_executor:
        logger.warning("Shutting down process pool. This may take a moment...")
        global_executor.shutdown(wait=False, cancel_futures=True)
    
    logger.info("Graceful shutdown initiated. Exiting...")
    sys.exit(1)

def parse_arguments():
    """
    Parse command-line arguments. 
    Usage example:
      python scanner.py --workers 4
    If --workers is 0 or omitted, we use single-process mode.
    """
    parser = argparse.ArgumentParser(description="Scanner for media files, single or multi-process.")
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Number of worker processes. If 0 (default), run single-process."
    )
    parser.add_argument(
        "--display-stats", 
        action="store_true",
        default=False,
        help="Display pre-scan statistics before starting the scan"
    )
    parser.add_argument(
        "--reset-table", 
        action="store_true",
        default=False,
        help="Reset (drop and recreate) the database table before starting"
    )
    return parser.parse_args()


def reset_table(db_credentials, table_name):
    """
    Reset (drop and recreate) the specified table.
    """
    logger = CustomLogger(__name__).get_logger()
    logger.info(f"Resetting table {table_name}...")
    
    try:
        # Create a new connection for this operation
        db_connection = DatabaseConnection(
            db_server_ip=db_credentials["server_ip"],
            db_server_port=db_credentials["server_port"],
            db_name=db_credentials["db_name"],
            db_user=db_credentials["db_user"],
            db_password=db_credentials["db_password"]
        )
        db_manager = DatabaseManager()
        
        # Reset the table (drop and recreate)
        db_manager.reset_table(db_connection.connection, table_name)
        logger.info(f"Table {table_name} reset successfully.")
        
        # Close the connection
        db_connection.close()
        return True
    except Exception as e:
        logger.error(f"Failed to reset table {table_name}: {e}")
        return False


def run_scan_and_load_single_process(
    directories: list[list[str]],
    table_name: str,
    db_connection: DatabaseConnection,
    db_manager: DatabaseManager,
    scanner: Scanner
):
    """
    Single-process scan that processes the directories in serial.
    We rely on the global 'stop_flag' to stop the loop if Ctrl+C is pressed.
    """
    if not directories:
        scanner.logger.error("No directories specified to scan.")
        return
    
    # Flatten the nested list of directories for single-process mode
    flat_directories = []
    for group in directories:
        flat_directories.extend(group)
    
    scanner.logger.info(f"Processing {len(flat_directories)} directories in single-process mode")
    
    # Set the stop_flag reference in scanner
    scanner.stop_flag_ref = lambda: stop_flag
    
    # Call the normal 'process_directories' which in turn calls 'scan_and_load'.
    # 'scan_and_load' will check stop_flag each iteration.
    scanner.process_directories(table_name, db_connection, db_manager, flat_directories)


def process_directory_group(
    group_directories: list[str],
    db_credentials: dict,
    table_name: str,
    yolo_model_path: str
):
    """
    Process an entire group of directories in a single process.
    This function runs in its own process and handles all directories in the group.
    """
    logger = CustomLogger(__name__).get_logger()
    logger.info(f"Starting process for group with {len(group_directories)} directories")
    
    # Set up signal handler for this process
    signal.signal(signal.SIGINT, lambda sig, frame: sys.exit(0))
    
    # Set up database connection for this process
    try:
        db_connection = DatabaseConnection(
            db_server_ip=db_credentials["server_ip"],
            db_server_port=db_credentials["server_port"],
            db_name=db_credentials["db_name"],
            db_user=db_credentials["db_user"],
            db_password=db_credentials["db_password"]
        )
        db_manager = DatabaseManager()
        
        # Create the table in this process if it doesn't exist
        logger.info(f"Ensuring table {table_name} exists...")
        db_manager.create_table(db_connection.connection, table_name)
        
        # Create scanning components
        hash_calculator = HashCalculator()
        video_fingerprinter = VideoFingerprinter()
        yolo_provider = YoloProvider(yolo_model_path, iou=0.5, conf=0.5, device="auto")
        
        # Create scanner with its own stop flag
        local_stop_flag = False
        def check_stop_flag():
            return local_stop_flag
        
        scanner = Scanner(
            logger=logger,
            hash_calculator=hash_calculator,
            yolo_provider=yolo_provider,
            video_fingerprinter=video_fingerprinter,
            stop_flag_ref=check_stop_flag
        )
        
        # Process each directory in the group sequentially
        processed_count = 0
        try:
            for directory in group_directories:
                try:
                    logger.info(f"Processing directory: {directory}")
                    scanner.scan_and_load(table_name, db_connection, db_manager, directory)
                    processed_count += 1
                    logger.info(f"Completed directory {processed_count}/{len(group_directories)} in group")
                except Exception as e:
                    logger.error(f"Error processing directory {directory}: {e}")
                
                # Check if we should exit
                if local_stop_flag:
                    logger.info("Stop flag detected, exiting group processing")
                    break
        except KeyboardInterrupt:
            logger.warning("Keyboard interrupt detected in child process")
            local_stop_flag = True
        finally:
            # Clean up
            db_connection.close()
            logger.info(f"Completed processing group with {processed_count}/{len(group_directories)} directories")
            return processed_count
            
    except Exception as e:
        logger.error(f"Error in process_directory_group: {e}", exc_info=True)
        if 'db_connection' in locals():
            db_connection.close()
        return 0


def run_scan_and_load_multiprocess(
    directories: list[list[str]],
    table_name: str,
    db_credentials: dict,
    max_workers: int,
    yolo_model_path: str
):
    """
    Multi-process version where each GROUP of directories is handled by a separate worker.
    """
    global global_executor
    logger = CustomLogger(__name__).get_logger()
    
    if not directories:
        logger.error("No directory groups specified to scan.")
        return
    
    # Count total directories for progress reporting
    total_groups = len(directories)
    total_directories = sum(len(group) for group in directories)
    
    logger.info(f"Running multi-process scan with up to {max_workers} workers.")
    logger.info(f"Total: {total_groups} groups containing {total_directories} directories")
    
    # Create a multiprocessing context explicitly
    mp_context = multiprocessing.get_context('spawn')
    
    try:
        with concurrent.futures.ProcessPoolExecutor(
                max_workers=max_workers,
                mp_context=mp_context
            ) as executor:
            
            global_executor = executor
            
            # Submit one task per GROUP
            futures = []
            for group in directories:
                # Skip empty groups
                if not group:
                    continue
                    
                # Each call to process_directory_group processes all directories in that group
                futures.append(executor.submit(
                    process_directory_group, 
                    group,  # Pass the entire group of directories
                    db_credentials, 
                    table_name, 
                    yolo_model_path
                ))
            
            completed_groups = 0
            completed_directories = 0
            
            try:
                for future in concurrent.futures.as_completed(futures):
                    try:
                        # Get the count of directories processed by this group
                        dirs_processed = future.result()
                        completed_groups += 1
                        completed_directories += dirs_processed
                        
                        logger.info(
                            f"Progress: {completed_groups}/{total_groups} groups completed, "
                            f"{completed_directories}/{total_directories} directories processed"
                        )
                    except Exception as e:
                        logger.error(f"Error in worker process: {e}", exc_info=True)
            except KeyboardInterrupt:
                logger.warning("Keyboard interrupt detected in main. Shutting down workers...")
                executor.shutdown(wait=False, cancel_futures=True)
                raise
            
            global_executor = None
            
    except KeyboardInterrupt:
        logger.warning("Scan interrupted by user. Exiting...")
        return
        
    logger.info("All multi-process directory scans complete.")


def populate_directories(base_directory: str) -> List[str]:
    """
    Scan the base directory and return a list of all subdirectories.
    
    Args:
        base_directory (str): The root directory to start scanning from
    
    Returns:
        list: A list of full paths to all subdirectories
    """
    # Ensure the base directory exists
    if not os.path.isdir(base_directory):
        print(f"Error: {base_directory} is not a valid directory.")
        return []

    # List to store all discovered directories
    directories = []

    # Walk through the directory tree
    for root, dirs, _ in os.walk(base_directory):
        # Add each directory's full path to the list
        for dir_name in dirs:
            full_path = os.path.join(root, dir_name)
            directories.append(full_path)

    return directories


def main():
    """
    Main function that either runs single-process or multi-process scanning 
    based on the --workers argument.
    """
    # Set up signal handler for keyboard interrupts
    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)
    
    args = parse_arguments()
    logger = CustomLogger(__name__).get_logger()

    # Database credentials. 
    db_credentials = {
        "server_ip": "172.16.8.31",
        "server_port": "1433",
        "db_name": "media_db",
        "db_user": "sa",
        "db_password": "Abcd!5678"
    }

    # SOURCE_DIRECTORIES definition omitted for brevity
    SOURCE_DIRECTORIES = [
        [
            "/mnt/nas3/projects/assets/스크래퍼/인스타/강예진",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/강혜원",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/고준희",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/곽리아",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/구진희",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/권유주",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/기타",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김다륜",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김미진",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김민희",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김보라",
        ],
        [
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김수연",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김신애",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김연재",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김연정",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김예진",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김주희",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김지원",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김진솔",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김진희",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김채연",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김하나",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김해나",
        ],
        [
            "/mnt/nas3/projects/assets/스크래퍼/인스타/김희진",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/나나세",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/니니니",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/데이니",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/도민서",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/레이양",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/레이카",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/마주리",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/마코토",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/박다현",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/박미란",
        ],
        [
            "/mnt/nas3/projects/assets/스크래퍼/인스타/박민영",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/박수현",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/박정윤",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/박지나",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/박지영",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/박클린",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/박혜빈",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/반서진",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/백설희",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/벨라윤",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/비워니",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/사쿠라",
        ],
        [
            "/mnt/nas3/projects/assets/스크래퍼/인스타/삼칠삼",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/서혜린",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/세레나",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/손현경",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/송단아",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/송은주",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/쇼핑몰",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/수빈이",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/수안비",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/스테판",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/스텔라",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/시카밍",
        ],
        [
            "/mnt/nas3/projects/assets/스크래퍼/인스타/신민아",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/신유나",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/신재은",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/신지현",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/신해빈",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/아사키",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/아이야",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/아카리",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/야수연",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/양혜원",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/언더웨어",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/오뽀슬",
        ],
        [
            "/mnt/nas3/projects/assets/스크래퍼/인스타/오영주",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/왕씨씨",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/외국녀",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/우마이",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/유나겸",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/윤빛나",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/윤지수",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/율리왕",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이가은",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이경이",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이릴리",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이산리",
        ],
        [
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이성혜",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이세린",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이세빈",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이소현",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이수니",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이승진",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이시은",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이아영",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이연",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이예리",
        ],
        [
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이유정",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이유주",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이재이",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이주미",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이주아",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이지원",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이채은",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이태이",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/이한이",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/인터넷",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/일본녀",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/임세리",
        ],
        [
            "/mnt/nas3/projects/assets/스크래퍼/인스타/임지나",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/장아영",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/정소민",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/정우주",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/정지윤",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/제시카",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/제이진",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/조민영",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/조승민",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/조윤혜",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/주아령",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/진아흔",
        ],
        [
            "/mnt/nas3/projects/assets/스크래퍼/인스타/차아정",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/차은영",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/채수빈",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/천미경",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/천영은",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/첼로주",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/최소미",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/최여진",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/최윤희",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/최은경",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/최자영",
        ],
        [
            "/mnt/nas3/projects/assets/스크래퍼/인스타/최진",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/치아키",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/테레사",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/하니야",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/한민영",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/한채영",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/한효빈",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/홍모영",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/홍세라",
            "/mnt/nas3/projects/assets/스크래퍼/인스타/황바비",
        ],
    ]


    DEFAULT_TABLE_NAME = "tbl_fetcher"
    YOLO_MODEL_PATH = os.path.abspath("model/yolov8x.pt")

    try:
        # Reset table if requested
        if args.reset_table:
            if not reset_table(db_credentials, DEFAULT_TABLE_NAME):
                logger.error("Table reset failed. Exiting.")
                return
            logger.info("Table reset completed successfully.")

        # Create the table in the main process
        init_db_connection = DatabaseConnection(
            db_server_ip=db_credentials["server_ip"],
            db_server_port=db_credentials["server_port"],
            db_name=db_credentials["db_name"],
            db_user=db_credentials["db_user"],
            db_password=db_credentials["db_password"]
        )
        init_db_manager = DatabaseManager()
        init_db_manager.create_table(init_db_connection.connection, DEFAULT_TABLE_NAME)
        logger.info(f"Initialized table {DEFAULT_TABLE_NAME} in database.")
        init_db_connection.close()

    except Exception as e:
        logger.error(f"Failed to initialize database table: {e}")
        return

    try:
        # Create scanning modules
        hash_calculator = HashCalculator()
        video_fingerprinter = VideoFingerprinter()
        yolo_provider = YoloProvider(YOLO_MODEL_PATH, iou=0.5, conf=0.5, device="auto")

        # Single Scanner for single-process mode
        scanner = Scanner(
            logger=logger,
            hash_calculator=hash_calculator,
            yolo_provider=yolo_provider,
            video_fingerprinter=video_fingerprinter,
            stop_flag_ref=lambda: stop_flag
        )

        # If you want a pre-scan stats
        if args.display_stats:
            # Flatten for stats
            flat_dirs = []
            for group in SOURCE_DIRECTORIES:
                flat_dirs.extend(group)
            scanner.log_pre_scan_stats(flat_dirs)
            logger.info("Displayed pre-scan stats. Exiting as requested.")
            return

        # Single-process or multi-process
        if args.workers == 0:
            logger.info("Running in SINGLE-PROCESS mode.")
            db_connection = DatabaseConnection(
                db_server_ip=db_credentials["server_ip"],
                db_server_port=db_credentials["server_port"],
                db_name=db_credentials["db_name"],
                db_user=db_credentials["db_user"],
                db_password=db_credentials["db_password"]
            )
            db_manager = DatabaseManager()

            # Run single-process scanning
            run_scan_and_load_single_process(
                directories=SOURCE_DIRECTORIES,
                table_name=DEFAULT_TABLE_NAME,
                db_connection=db_connection,
                db_manager=db_manager,
                scanner=scanner
            )

            db_connection.close()
            logger.info("Single-process scan completed successfully.")
        else:
            logger.info(f"Running in MULTI-PROCESS mode with {args.workers} workers.")
            run_scan_and_load_multiprocess(
                directories=SOURCE_DIRECTORIES,
                table_name=DEFAULT_TABLE_NAME,
                db_credentials=db_credentials,
                max_workers=args.workers,
                yolo_model_path=YOLO_MODEL_PATH
            )
            logger.info("Multi-process scan completed successfully.")

    except KeyboardInterrupt:
        logger.warning("Scan interrupted by user in main.")
    except Exception as e:
        logger.error(f"Error in main process: {e}", exc_info=True)


if __name__ == "__main__":
    try:
        multiprocessing.set_start_method("spawn")
    except RuntimeError:
        # Already set, ignore
        pass

    # We set the same handle_sigint for SIGTERM if we like
    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    try:
        main()
    except KeyboardInterrupt:
        logger = CustomLogger(__name__).get_logger()
        logger.warning("Process interrupted by user. Exiting...")
        sys.exit(0)