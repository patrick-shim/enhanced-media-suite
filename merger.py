#!/usr/bin/env python3

import os
import sys
import signal
import argparse
import json
import time
import hashlib
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import sqlite3
from pathlib import Path

from source.logging_modules import CustomLogger
from source.merger_modules import Merger
from source.yolo_modules import YoloProvider

stop_flag = False
progress_file = "merger_progress.json"
hash_db_file = "file_hashes.db"

def handle_sigint(sig, frame):
    """
    Set a global stop flag on Ctrl+C for graceful shutdown.
    """
    global stop_flag
    logger = CustomLogger(__name__).get_logger()
    logger.warning("Keyboard interrupt detected. Stopping the merger gracefully...")
    logger.warning("Progress will be saved. You can resume later.")
    stop_flag = True

def parse_arguments():
    """
    Parse command line arguments for the merger script.
    """
    parser = argparse.ArgumentParser(
        description="Merge media files from source directories into separate image and video destinations."
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        required=True,
        help="One or more source directories to merge from"
    )
    parser.add_argument(
        "--image-dest",
        default="/mnt/nas3/Photos",
        help="Destination directory for image files (default: /mnt/nas3/Photos)"
    )
    parser.add_argument(
        "--video-dest",
        default="/mnt/nas3/Videos",
        help="Destination directory for video files (default: /mnt/nas3/Videos)"
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Number of threads for parallel merging (default: 4)"
    )
    parser.add_argument(
        "--human-only",
        action="store_true",
        help="Only copy images that contain humans (requires YOLO model)"
    )
    parser.add_argument(
        "--yolo-model",
        default="model/yolov8x.pt",
        help="Path to YOLO model file (default: model/yolov8x.pt)"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last saved progress"
    )
    parser.add_argument(
        "--rebuild-hash-db",
        action="store_true",
        help="Rebuild the hash database for destination directories"
    )
    return parser.parse_args()

def save_progress(processed_files):
    """
    Save the list of processed files to enable resuming later.
    """
    with open(progress_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'processed_files': list(processed_files)
        }, f)

def load_progress():
    """
    Load the list of previously processed files.
    """
    if not os.path.exists(progress_file):
        return set()
    
    try:
        with open(progress_file, 'r') as f:
            data = json.load(f)
            return set(data.get('processed_files', []))
    except Exception as e:
        logger = CustomLogger(__name__).get_logger()
        logger.error(f"Error loading progress file: {e}")
        return set()

class HashDatabase:
    """
    A database for storing and retrieving file hashes to speed up duplicate detection.
    """
    def __init__(self, db_path=hash_db_file):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.logger = CustomLogger(__name__).get_logger()
        self.init_db()
        
    def init_db(self):
        """Initialize the database connection and create tables if they don't exist."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            
            # Create tables if they don't exist
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS file_hashes (
                    path TEXT PRIMARY KEY,
                    hash TEXT,
                    size INTEGER,
                    last_modified REAL,
                    priority INTEGER
                )
            ''')
            
            # Create index on hash for faster lookups
            self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_hash ON file_hashes(hash)
            ''')
            
            self.conn.commit()
        except Exception as e:
            self.logger.error(f"Error initializing hash database: {e}")
            if self.conn:
                self.conn.close()
            raise
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
    
    def add_file(self, path, file_hash, size, last_modified, priority):
        """Add or update a file hash in the database."""
        try:
            self.cursor.execute(
                "INSERT OR REPLACE INTO file_hashes VALUES (?, ?, ?, ?, ?)",
                (path, file_hash, size, last_modified, priority)
            )
            self.conn.commit()
        except Exception as e:
            self.logger.error(f"Error adding file to hash database: {e}")
            self.conn.rollback()
    
    def get_files_by_hash(self, file_hash):
        """Get all files with a specific hash."""
        try:
            self.cursor.execute(
                "SELECT path, priority FROM file_hashes WHERE hash = ?",
                (file_hash,)
            )
            return self.cursor.fetchall()
        except Exception as e:
            self.logger.error(f"Error querying hash database: {e}")
            return []
    
    def file_exists(self, path):
        """Check if a file exists in the database."""
        try:
            self.cursor.execute(
                "SELECT 1 FROM file_hashes WHERE path = ?",
                (path,)
            )
            return bool(self.cursor.fetchone())
        except Exception as e:
            self.logger.error(f"Error checking file existence in database: {e}")
            return False
    
    def remove_file(self, path):
        """Remove a file from the database."""
        try:
            self.cursor.execute(
                "DELETE FROM file_hashes WHERE path = ?",
                (path,)
            )
            self.conn.commit()
        except Exception as e:
            self.logger.error(f"Error removing file from hash database: {e}")
            self.conn.rollback()
    
    def build_for_directory(self, directory, stop_flag_ref=None):
        """Build the hash database for all files in a directory."""
        def get_file_priority(filename):
            import re
            # Same priority logic as in the Merger class
            if re.match(r'\d{8}_\d{6}_\w+\.\w+$', filename):
                return 1
            if re.match(r'\d{8}_\d{6}_\w+', filename):
                return 2
            if re.match(r'.*\d{8}_\d{6}_\w+_\d+\.\w+$', filename):
                return 3
            if not re.search(r'\(\d+\)\.\w+$', filename):
                return 4
            return 5
        
        count = 0
        start_time = time.time()
        self.logger.info(f"Building hash database for directory: {directory}")
        
        # Get image and video extensions from the Merger class
        image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.heic', '.heif'}
        video_extensions = {'.mp4', '.mov', '.avi', '.wmv', '.mkv', '.flv', '.webm', '.m4v', '.3gp', '.mpg', '.mpeg'}
        
        for root, _, files in os.walk(directory):
            if stop_flag_ref and stop_flag_ref():
                self.logger.info("Stopped hash database building due to stop flag.")
                break
                
            for filename in files:
                if stop_flag_ref and stop_flag_ref():
                    break
                    
                file_path = os.path.join(root, filename)
                file_ext = os.path.splitext(filename)[1].lower()
                
                # Only process relevant file types
                if file_ext not in image_extensions and file_ext not in video_extensions:
                    continue
                
                try:
                    # Get file info
                    stat = os.stat(file_path)
                    size = stat.st_size
                    mtime = stat.st_mtime
                    
                    # Calculate hash
                    hasher = hashlib.blake2b()
                    with open(file_path, "rb") as f:
                        while True:
                            chunk = f.read(65536)
                            if not chunk:
                                break
                            hasher.update(chunk)
                    file_hash = hasher.hexdigest()
                    
                    # Get priority
                    priority = get_file_priority(filename)
                    
                    # Add to database
                    self.add_file(file_path, file_hash, size, mtime, priority)
                    
                    count += 1
                    if count % 1000 == 0:
                        elapsed = time.time() - start_time
                        self.logger.info(f"Processed {count} files in {elapsed:.2f} seconds. Current file: {file_path}")
                        
                except Exception as e:
                    self.logger.error(f"Error processing file {file_path}: {e}")
        
        elapsed = time.time() - start_time
        self.logger.info(f"Completed hash database build for {directory}. Processed {count} files in {elapsed:.2f} seconds.")
        return count

class EfficientMerger(Merger):
    """
    Extended Merger class with hash database for efficient duplicate detection.
    """
    def __init__(self, *args, **kwargs):
        # Extract additional parameters
        self.processed_files = kwargs.pop('processed_files', set())
        self.save_interval = kwargs.pop('save_interval', 50)
        self.hash_db = kwargs.pop('hash_db')
        self.file_counter = 0
        
        super().__init__(*args, **kwargs)
    
    def run(self, stop_flag_ref):
        """
        Override run method to use hash database for duplicate detection.
        """
        self.logger.info(f"[Merger] Starting merge run with {len(self.processed_files)} already processed files.")
        
        # Call the parent class run method
        super().run(stop_flag_ref)
        
        # Save final progress
        save_progress(self.processed_files)
        self.logger.info(f"[Merger] Final progress saved. Processed {len(self.processed_files)} files in total.")
    
    def _handle_file(self, source_path, dest_path, file_type, was_human, stop_flag_ref):
        """
        Override _handle_file to use hash database for duplicate detection.
        """
        # Check stop flag first
        if stop_flag_ref():
            return
        
        # Skip if already processed
        if source_path in self.processed_files:
            self.logger.debug(f"[Merger] Skipping already processed file: {source_path}")
            return
        
        # If destination file exists with the same name, check if it's a duplicate
        if os.path.exists(dest_path):
            # Direct path match, don't need to compute hash yet
            self.logger.debug(f"[Merger] File with same name exists: {dest_path}")
            self.processed_files.add(source_path)
            return
            
        self.logger.debug(f"[Merger] Processing file: {source_path}")
        
        try:
            # Calculate source file hash
            src_size = os.path.getsize(source_path)
            src_mtime = os.path.getmtime(source_path)
            
            # Compute source file hash
            hasher = hashlib.blake2b()
            with open(source_path, "rb") as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    hasher.update(chunk)
            src_hash = hasher.hexdigest()
            
            # Get source file priority
            src_filename = os.path.basename(source_path)
            src_priority = self._get_file_priority(src_filename)
            
            # Check if a file with the same hash exists in the database
            matching_files = self.hash_db.get_files_by_hash(src_hash)
            
            if matching_files:
                # Found duplicates in the hash database
                self.logger.debug(f"[Merger] Found {len(matching_files)} duplicate(s) for {source_path}")
                
                # Check if any duplicate has higher priority
                highest_priority = src_priority
                highest_priority_path = None
                
                for path, priority in matching_files:
                    if priority < highest_priority:  # Lower number = higher priority
                        highest_priority = priority
                        highest_priority_path = path
                
                if highest_priority < src_priority:
                    # A duplicate with higher priority exists
                    self.logger.info(
                        f"[Merger] Skipping file with lower priority: {source_path} (existing: {highest_priority_path})"
                    )
                    self.processed_files.add(source_path)
                    return
                    
                # If we get here, the source file has equal or higher priority
                for path, priority in matching_files:
                    if priority > src_priority:  # Source has higher priority
                        try:
                            self.logger.info(
                                f"[Merger] Replacing existing file with higher priority source: "
                                f"{path} <- {source_path}"
                            )
                            os.remove(path)
                            self.hash_db.remove_file(path)
                        except Exception as e:
                            self.logger.error(f"[Merger] Error replacing file: {e}")
            
            # If we reach here, we should copy the file
            try:
                # Ensure destination directory exists
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                
                # Copy the file
                import shutil
                shutil.copy2(source_path, dest_path)
                
                # Add to hash database
                self.hash_db.add_file(dest_path, src_hash, src_size, src_mtime, src_priority)
                
                self.logger.info(f"[Merger] Copied new ({file_type}, human: {was_human}) file: {source_path} -> {dest_path}")
            except Exception as e:
                self.logger.error(f"[Merger] Error copying {file_type} file {source_path} -> {dest_path}: {e}")
        
        except Exception as e:
            self.logger.error(f"[Merger] Error processing file {source_path}: {e}")
        
        # Mark as processed and periodically save progress
        self.processed_files.add(source_path)
        self.file_counter += 1
        
        if self.file_counter % self.save_interval == 0:
            save_progress(self.processed_files)
            self.logger.debug(f"[Merger] Progress saved. Processed {len(self.processed_files)} files so far.")

def main():
    logger = CustomLogger(__name__).get_logger()
    
    # Catch Ctrl+C
    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    args = parse_arguments()
    logger.info("[Merger] Starting merge operation with arguments:")
    logger.info(f"  sources={args.sources}")
    logger.info(f"  image_dest={args.image_dest}")
    logger.info(f"  video_dest={args.video_dest}")
    logger.info(f"  threads={args.threads}")
    logger.info(f"  human_only={args.human_only}")
    logger.info(f"  resume={args.resume}")
    logger.info(f"  rebuild_hash_db={args.rebuild_hash_db}")
    
    # Initialize YOLO if human_only is enabled
    yolo_provider = None
    if args.human_only:
        logger.info(f"[Merger] Initializing YOLO model from {args.yolo_model}")
        try:
            yolo_provider = YoloProvider(
                model_path=args.yolo_model,
                iou=0.5,
                conf=0.5,
                device="auto"
            )
        except Exception as e:
            logger.error(f"[Merger] Failed to initialize YOLO model: {e}")
            logger.error("[Merger] Continuing without human detection")
            args.human_only = False  # Disable human-only if YOLO failed

    # Ensure destinations exist
    os.makedirs(args.image_dest, exist_ok=True)
    os.makedirs(args.video_dest, exist_ok=True)
    logger.info(f"[Merger] Image destination ensured: {args.image_dest}")
    logger.info(f"[Merger] Video destination ensured: {args.video_dest}")

    # Initialize hash database
    hash_db = HashDatabase()
    
    # Rebuild hash database if requested
    if args.rebuild_hash_db or not os.path.exists(hash_db_file):
        logger.info("[Merger] Building/rebuilding hash database for destination directories...")
        
        # Build hash database for destination directories
        image_count = hash_db.build_for_directory(args.image_dest, lambda: stop_flag)
        if stop_flag:
            logger.warning("[Merger] Stop flag set during image hash database build. Exiting.")
            hash_db.close()
            return
            
        video_count = hash_db.build_for_directory(args.video_dest, lambda: stop_flag)
        if stop_flag:
            logger.warning("[Merger] Stop flag set during video hash database build. Exiting.")
            hash_db.close()
            return
            
        logger.info(f"[Merger] Hash database built with {image_count + video_count} entries.")
    else:
        logger.info("[Merger] Using existing hash database.")
    
    # Load progress if resuming
    processed_files = load_progress() if args.resume else set()
    if args.resume:
        logger.info(f"[Merger] Resuming from previous run. {len(processed_files)} files already processed.")
    
    # Create and run the enhanced Merger
    merger = EfficientMerger(
        logger=logger,
        sources=args.sources,
        image_destination=args.image_dest,
        video_destination=args.video_dest,
        thread_count=args.threads,
        yolo_provider=yolo_provider,
        human_only=args.human_only,
        processed_files=processed_files,
        save_interval=50,
        hash_db=hash_db
    )

    try:
        start_time = time.time()
        merger.run(lambda: stop_flag)
        elapsed_time = time.time() - start_time
        logger.info(f"[Merger] Completed merge operation in {elapsed_time:.2f} seconds.")
    except Exception as e:
        logger.error(f"[Merger] Unexpected error in merging: {e}", exc_info=True)
    finally:
        # Ensure hash database is closed properly
        hash_db.close()

if __name__ == "__main__":
    main()