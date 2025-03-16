#!/usr/bin/env python3

import os
import sys
import signal
import argparse
from concurrent.futures import ThreadPoolExecutor

from source.logging_modules import CustomLogger
from source.db_modules import DatabaseConnection, DatabaseManager
from source.copier_modules import Copier

# Constants for destination directories
VIDEO_DESTINATION = "/mnt/nas3/TEST/뷰티영상/"
IMAGE_DESTINATION = "/mnt/nas3/TEST/뷰티사진/"

# Global stop flag
stop_flag = False

def handle_sigint(sig, frame):
    """
    Handle keyboard interrupt (SIGINT) by signaling the Copier to stop gracefully.
    """
    global stop_flag
    logger = CustomLogger(__name__).get_logger()
    logger.warning("Keyboard interrupt caught. Stopping copy operation...")
    stop_flag = True

def parse_arguments():
    """
    Parse command line arguments for the copier application.
    """
    parser = argparse.ArgumentParser(
        description="Multimedia file copy utility with optional human detection (via YOLO) and dedupe support."
    )
    parser.add_argument(
        "--directory-depth", 
        type=int, 
        default=0,
        help="Directory depth to preserve when copying (0 => flat structure)"
    )
    parser.add_argument(
        "--threads", 
        type=int, 
        default=14,
        help="Number of threads for parallel copying"
    )
    parser.add_argument(
        "--human-only", 
        action="store_true", 
        help="Only copy images where YOLO detects a person"
    )
    parser.add_argument(
        "--dedupe", 
        choices=["none", "dhash", "phash", "twophase"], 
        default="none",
        help="Which deduplication source table to use (default: none)"
    )
    return parser.parse_args()

def main():
    """
    Main entry point for the copier script.
    """
    # 1) Set up logger and handle Ctrl+C signals
    logger = CustomLogger(__name__).get_logger()
    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    # 2) Parse arguments
    args = parse_arguments()
    logger.info("Starting copy application with arguments:")
    logger.info(f"  directory_depth={args.directory_depth}")
    logger.info(f"  threads={args.threads}")
    logger.info(f"  human_only={args.human_only}")
    logger.info(f"  dedupe={args.dedupe}")

    # 3) Database connection
    db_credentials = {
        "db_server_ip": "172.16.8.31",
        "db_server_port": "1433",
        "db_name": "media_db",
        "db_user": "sa",
        "db_password": "Abcd!5678"
    }
    try:
        db_connection = DatabaseConnection(**db_credentials)
        db_manager = DatabaseManager()
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        sys.exit(1)

    # 5) Create Copier with these dependencies
    copier = Copier(
        logger=logger,
        db_manager=db_manager,
        db_connection=db_connection.connection,
        video_destination=VIDEO_DESTINATION,
        image_destination=IMAGE_DESTINATION,
        directory_depth=args.directory_depth,
        human_only=args.human_only,
        dedupe_option=args.dedupe
    )

    # 6) Run copy process
    try:
        copier.run(
            thread_count=args.threads,
            stop_flag_ref=lambda: stop_flag
        )
        logger.info("Copy operation completed successfully.")
    except Exception as e:
        logger.error(f"Unexpected error while copying files: {e}", exc_info=True)

    # 7) Cleanup
    db_connection.close()
    logger.info("Copier script finished.")

if __name__ == "__main__":
    main()