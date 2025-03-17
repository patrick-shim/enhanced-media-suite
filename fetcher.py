#!/usr/bin/env python3

import os
import sys
import pytz
import time
import random
import argparse
import traceback
import re
from datetime import datetime
from instaloader import Instaloader
from argparse import Namespace

# Source modules
from source.logging_modules import CustomLogger
from source.database_modules import DatabaseConnection, DatabaseManager
from source.hash_modules import HashCalculator
from source.fingerprint_modules import VideoFingerprinter
from source.yolo_modules import YoloProvider, get_yolo_provider
from source.fetcher_modules import InstagramFetcher, RateController

def parse_args(default_table_name: str, default_download_directory):
    """
    Parse and return CLI arguments using subcommands for different modes.
    """
    parser = argparse.ArgumentParser(description="Download Instagram profile's posts and store metadata in SQL.")
    
    # Common arguments for all modes
    parser.add_argument("--table-name", type=str, default=f"{default_table_name}", help="DB table name")
    parser.add_argument("--skip-database", action="store_true", default=False, help="Skip record insertion to database")
    parser.add_argument("--reset-table", action="store_true", default=False, help="Reset the media table in DB")
    
    # Add YOLO provider arguments - simplified
    parser.add_argument("--use-remote-yolo", action="store_true", default=True, help="Use remote YOLO API instead of local model")
    
    # Create subparsers for different modes
    subparsers = parser.add_subparsers(dest="mode", required=True, help="Operation mode")
    
    # 1. Instagram download mode
    instagram_parser = subparsers.add_parser("download", help="Download media from Instagram")
    instagram_parser.add_argument("--login", required=True, help="Instagram login username")
    instagram_parser.add_argument("--profile", required=True, help="Target Instagram profile to download from")
    instagram_parser.add_argument("--relogin", action="store_true", default=False, help="Re-login to Instagram by deleting the session file")
    instagram_parser.add_argument("--save-to", type=str, default=None, help="Directory to save downloaded media (default: profile name)")
    instagram_parser.add_argument("--resume", action="store_true", default=True, help="Resume from last post (default: True)")
    instagram_parser.add_argument("--limit", type=int, default=None, help="Limit the number of posts to download")
    
    # 2. Reverse scan mode
    reverse_parser = subparsers.add_parser("scan", help="Scan local directory and process files")
    reverse_parser.add_argument("path", type=str, help="Path to the directory to scan")
    reverse_parser.add_argument("--no-reset-table", action="store_true", default=False, help="Do not reset the table before scanning (default: False)")
    reverse_parser.add_argument("--no-stats", action="store_true", default=False, help="Do not display statistics before scanning (default: False)")
    
    args = parser.parse_args()
    return args

def init_instaloader(
    user_agent: str,
    resume_enabled: bool,
    max_retries: int,
    logger: CustomLogger,
    default_download_directory: str
) -> Instaloader:
    """
    Create and return a configured Instaloader instance.
    """
    L = Instaloader(
        rate_controller=lambda ctx: RateController(ctx),
        max_connection_attempts=max_retries,
        dirname_pattern=f"{default_download_directory}/{{target}}",
        filename_pattern="{date:%Y%m%d_%H%M%S}_{shortcode}",
        save_metadata=True,
        compress_json=True,
        download_video_thumbnails=False,
        post_metadata_txt_pattern="",
        download_geotags=False,
        quiet=True,
        user_agent=user_agent
    )

    L.fast_update=resume_enabled
    L.download_comments=False
    return L

def pick_user_agent(timezone_str: str, current_time_struct: time.struct_time = None) -> str:
    """
    Pick a user agent string based on time-of-day.
    """
    mobile_agents = [
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/134.0.6998.33 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 18_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 10;K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Mobile Safari/537.36"
    ]
    desktop_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/131.0.2903.86"
    ]
    tz = pytz.timezone(timezone_str)
    if current_time_struct is None:
        now = datetime.now(tz)
        hour = now.hour
    else:
        hour = current_time_struct.tm_hour

    if 5 <= hour < 12:
        return random.choice(mobile_agents)
    elif 12 <= hour < 17:
        return random.choice(desktop_agents)
    else:
        return random.choice(mobile_agents)

def main():
    # Constants
    MAX_RETRIES = 3  # For Instaloader connection attempts
    DEFAULT_TABLE_NAME = "tbl_fetcher"
    DEFAULT_DOWNLOAD_DIRECTORY = '/mnt/nas3/projects/assets/스크래퍼/인스타'
    
    # YOLO remote defaults
    REMOTE_YOLO_URL = "http://172.16.8.45:8000"
    REMOTE_YOLO_RETRIES = 3
    REMOTE_YOLO_RETRY_DELAY = 2
    REMOTE_YOLO_FALLBACK_ENABLED = True

    logger = CustomLogger(__name__).get_logger()
    args = parse_args(DEFAULT_TABLE_NAME, DEFAULT_DOWNLOAD_DIRECTORY)

    # Setup dependencies
    hash_calculator = HashCalculator()
    video_fingerprinter = VideoFingerprinter()

    yolo_provider = get_yolo_provider(
        use_remote=args.use_remote_yolo,
        remote_url=REMOTE_YOLO_URL,
        max_retries=REMOTE_YOLO_RETRIES,
        retry_delay=REMOTE_YOLO_RETRY_DELAY,
        enable_fallback=REMOTE_YOLO_FALLBACK_ENABLED,
        model_path="model/yolov8x.pt",
        iou=0.5,
        conf=0.5,
        device="auto"
)
    
    # Database connection
    if args.skip_database:
        logger.info("[bright_black][Main]🏠[/bright_black][bold #FFA500]Skipping database operations[/bold #FFA500]")
        db_connection = None
        db_manager = None
    else:
        # Create DB connection
        db_connection = DatabaseConnection(
            db_server_ip="172.16.8.31",
            db_server_port="1433",
            db_name="media_db",
            db_user="sa",
            db_password="Abcd!5678"
        )
        try:
            connection = db_connection.connection
            db_manager = DatabaseManager()
            db_manager.create_table(connection, args.table_name)
        except Exception as e:
            logger.error(f"[bright_black][Main]🏠[/bright_black][red] Failed to create table[/red]: {e}")
            sys.exit(1)

    # Create fetcher
    instagram_fetcher = InstagramFetcher(
        logger=logger,
        hash_calculator=hash_calculator,
        yolo_provider=yolo_provider,
        video_fingerprinter=video_fingerprinter,
        skip_database=args.skip_database
    )

    # Possibly reset table
    if args.reset_table:
        instagram_fetcher.reset_table(args.table_name, db_connection, db_manager)

    try:
        if args.mode == "scan":
            # Reverse scan mode
            if not os.path.isdir(args.path):
                logger.error(f"[bright_black][Main]🏠[/bright_black][bold red] Scan directory does not exist[/bold red]: {args.path}")
                sys.exit(1)
                
            instagram_fetcher.reverse_scan(
                args.table_name,
                db_connection,
                db_manager,
                args.path,
                reset_table=not args.no_reset_table,
                display_stats=not args.no_stats
            )
            
        elif args.mode == "download":
            # Instagram download mode
            save_to = args.save_to if args.save_to else args.profile
            download_directory = os.path.join(DEFAULT_DOWNLOAD_DIRECTORY, save_to.rstrip('/'))
                
            if not os.path.isdir(download_directory):
                try:
                    os.makedirs(download_directory, exist_ok=True)
                except Exception as ex:
                    logger.error(f"Failed to create download directory '{download_directory}': {ex}")
                    sys.exit(1)
            
            # Prepare user-agent and Instaloader
            tz_str = "Asia/Seoul"
            current_time_struct = time.localtime()
            user_agent = pick_user_agent(tz_str, current_time_struct)
            
            L = init_instaloader(
                user_agent=user_agent,
                resume_enabled=args.resume,
                max_retries=MAX_RETRIES,
                logger=logger,
                default_download_directory=DEFAULT_DOWNLOAD_DIRECTORY
            )
            
            # Instagram login
            logger.info(f"[bright_black][Main]🏠[/bright_black][bold magenta] Logging in as [/bold magenta]🔐[bold red]{args.login}...[/bold red]")
            if not instagram_fetcher.instagram_login(args.login, L, args.relogin):
                logger.error(f"[bright_black][Main]🏠[/bright_black][bold red] Instagram login failed as [/bold red]⛔{args.login}. Exiting.")
                sys.exit(1)
                
            # Download posts
            instagram_fetcher.process_posts(
                db_connection,
                db_manager,
                args.table_name,
                L,
                args.profile,
                download_directory,
                save_to,
                limit=args.limit
            )
    
    except KeyboardInterrupt:
        logger.warning("Execution interrupted by user (Ctrl+C). Exiting gracefully.")
    except Exception as e:
        logger.error(f"Fatal error: {e}\n{traceback.format_exc()}")
    finally:
        # Clean up
        if db_connection:
            db_connection.close()
            
    logger.info(f"[bright_black][Main]🏠[/bright_black] 🎉🎈🎉🍾🎊  All done!!! 🎊 🍾🎈🎉🎉")

if __name__ == "__main__":
    main()