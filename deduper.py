#!/usr/bin/env python3

import sys
import argparse
import pyodbc

from source.logging_modules import CustomLogger
from source.db_modules import DatabaseConnection, DatabaseManager
from source.deduper_modules_db import Deduper
def parse_args():
    parser = argparse.ArgumentParser(
        description="Deduplicate images per directory, marking representatives."
    )
    parser.add_argument(
        "--source-table", 
        default="tbl_scanner", 
        help="Source table name (default: tbl_scanner)"
    )
    parser.add_argument(
        "--target-table",
        help="Optional target table name (default: auto-generated based on method)"
    )
    parser.add_argument(
        "--method", 
        default="both",
        choices=["single", "twophase", "both"],
        help="Deduplication method (default: both)"
    )
    parser.add_argument(
        "--phash-threshold", 
        type=int, 
        default=3,
        help="Hamming distance threshold for pHash (default: 3)"
    )
    parser.add_argument(
        "--dhash-threshold", 
        type=int, 
        default=5,
        help="Hamming distance threshold for dHash (default: 5)"
    )
    return parser.parse_args()

def main():
    args = parse_args()
    SOURCE_TABLE = args.source_table
    logger = CustomLogger(__name__).get_logger()

    conn_params = {
        'db_server_ip': '172.16.8.31',
        'db_server_port': '1433',
        'db_name': 'media_db',
        'db_user': 'sa',
        'db_password': 'Abcd!5678'
    }

    logger.info(f"[bright_black][Deduper]📸[/bright_black] Starting deduplication on table: [bold cyan]{SOURCE_TABLE}[/bold cyan]")
    logger.info(f"[bright_black][Deduper]📸[/bright_black] Method: [bold cyan]{args.method}[/bold cyan]")
    logger.info(f"[bright_black][Deduper]📸[/bright_black] pHash threshold: [bold cyan]{args.phash_threshold}[/bold cyan]")
    logger.info(f"[bright_black][Deduper]📸[/bright_black] dHash threshold: [bold cyan]{args.dhash_threshold}[/bold cyan]")

    # 1) Create DB connection + manager
    db_conn = DatabaseConnection(**conn_params)
    connection = db_conn.connection
    db_manager = DatabaseManager()

    # 2) Create the Deduper (uses pre-calculated hashes in DB)
    deduper = Deduper(db_manager)

    # 3) Run the requested deduplication method(s)
    if args.method in ["single", "both"]:
        logger.info(f"[bright_black][Deduper]📸[/bright_black][bold magenta] Starting single-phase per-directory deduplication (pHash)...[/bold magenta]")
        target_table = args.target_table if args.target_table and args.method == "single" else None
        target = deduper.dedupe_by_hash(
            connection=connection, 
            source_table=SOURCE_TABLE, 
            method='phash', 
            distance_threshold=args.phash_threshold
        )
        logger.info(f"[bright_black][Deduper]📸[/bright_black][bold green] Created table: {target}[/bold green]")
        logger.info(f"[bright_black][Deduper]📸[/bright_black] You can now query for representatives with:")
        logger.info(f"[bright_black][Deduper]📸[/bright_black][blue]  SELECT * FROM [dbo].[{target}] WHERE is_representative = 1[/blue]")
        
        # Example query to summarize representatives by directory
        logger.info(f"[bright_black][Deduper]📸[/bright_black] Summary query by directory:")
        logger.info(f"""[bright_black][Deduper]📸[/bright_black][blue]  
            SELECT file_directory, 
                   COUNT(*) as total_files,
                   SUM(CASE WHEN is_representative = 1 THEN 1 ELSE 0 END) as representatives
            FROM [dbo].[{target}]
            GROUP BY file_directory
            ORDER BY file_directory;
        [/blue]""")

    if args.method in ["twophase", "both"]:
        logger.info(f"[bright_black][Deduper]📸[/bright_black][bold magenta] Starting two-phase per-directory deduplication (dHash -> pHash)...[/bold magenta]")
        target_table = args.target_table if args.target_table and args.method == "twophase" else None
        target2 = deduper.dedupe_two_phase(
            connection=connection, 
            source_table=SOURCE_TABLE, 
            threshold_dhash=args.dhash_threshold, 
            threshold_phash=args.phash_threshold
        )
        logger.info(f"[bright_black][Deduper]📸[/bright_black][bold green] Created table: {target2}[/bold green]")
        logger.info(f"[bright_black][Deduper]📸[/bright_black] You can now query for representatives with:")
        logger.info(f"[bright_black][Deduper]📸[/bright_black][blue]  SELECT * FROM [dbo].[{target2}] WHERE is_representative = 1[/blue]")
        
        # Example query to summarize representatives by directory
        logger.info(f"[bright_black][Deduper]📸[/bright_black] Summary query by directory:")
        logger.info(f"""[bright_black][Deduper]📸[/bright_black][blue]  
            SELECT file_directory, 
                   COUNT(*) as total_files,
                   SUM(CASE WHEN is_representative = 1 THEN 1 ELSE 0 END) as representatives
            FROM [dbo].[{target2}]
            GROUP BY file_directory
            ORDER BY file_directory;
        [/blue]""")

    # Close the DB connection
    db_conn.close()
    logger.info(f"[bright_black][Deduper]📸[/bright_black][bold green] 🎉🎈🎉🍾🎊  Deduplication completed successfully! 🎊 🍾🎈🎉🎉[/bold green]")

if __name__ == "__main__":
    main()