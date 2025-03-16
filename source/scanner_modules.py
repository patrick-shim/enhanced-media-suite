import re
from typing import List, Callable
import os
import traceback
import time
from datetime import datetime
from dataclasses import dataclass
import fnmatch  # for matching file patterns

from source.logging_modules import CustomLogger
from source.database_modules import DatabaseConnection, DatabaseManager
from source.hash_modules import HashCalculator
from source.fingerprint_modules import VideoFingerprinter
from source.yolo_modules import YoloProvider

# ------------------------------
# Data Classes
# ------------------------------
@dataclass
class BasicFileInfo:
    path: str
    filename: str
    directory: str
    extension: str
    file_type: str
    file_size: int

# ------------------------------
# External Variables
# ------------------------------
image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.heic', '.heif']
video_extensions = ['.mp4', '.mov', '.avi', '.wmv', '.mkv', '.flv', '.webm', '.m4v', '.3gp', '.mpg', '.mpeg']
others_extensions = ['.json', '.xz', '.json.xz', '.txt', '.csv', '.zip', '.rar', '.7z', '.iso', '.dmg']

# You mentioned these patterns in scanner.py, but we can store them here so that
# they are automatically used for the entire scanning logic in one place.
EXCLUDE_FILE_PATTERNS = [
    "Thumbs.db", 
    "*.tmp", 
    ".*", 
    "~*.*", 
    "._*"
]
EXCLUDE_DIR_PATTERNS = [
    '$Recycle.Bin',
    '.recycle',
    '.*'
]

class Scanner:
    """
    Scans directories for image/video files, computes hashes, runs YOLO, and inserts
    metadata into a SQL database. Also provides a pre-scan summary of media counts.
    """
    def __init__(
        self, 
        logger, 
        hash_calculator: HashCalculator, 
        yolo_provider: YoloProvider, 
        video_fingerprinter: VideoFingerprinter,
        stop_flag_ref: Callable[[], bool] = None
    ):
        self.logger = logger
        self.hash_calculator = hash_calculator
        self.yolo_provider = yolo_provider
        self.video_fingerprinter = video_fingerprinter
        self.stop_flag_ref = stop_flag_ref

    ##############################################################################################################################
    # Public Methods
    ##############################################################################################################################

    def reset_table(self, table_name: str, db_connection: DatabaseConnection, db_manager: DatabaseManager) -> None:
        """
        Drop and recreate the table indicated by `table_name`.
        """
        if db_connection and db_manager:
            try:
                db_manager.reset_table(db_connection.connection, table_name)
                self.logger.info(f"[bright_black][scanner]📸[/bright_black][bold green] Table '{table_name}' reset successfully.[/bold green]")
            except Exception as e:
                self.logger.error(f"[bright_black][scanner]📸[/bright_black] Failed to reset table {table_name}: {e}")

    def process_directories(
        self, 
        table_name: str, 
        db_connection: DatabaseConnection, 
        db_manager: DatabaseManager, 
        directories: List[str]
    ) -> int:
        """
        1) Logs a pre-scan statistic of images/videos in each directory/subdirectory.
        2) Iterates the directories, calling `scan_and_load` on each.
        """
        # 1. If no directories given, bail out
        if not directories:
            self.logger.error("[bright_black][scanner]📸[/bright_black] No directories provided for processing.")
            return -1
        
        total_dirs = len(directories)
        processed_dirs = 0
        self.logger.info(f"[bright_black][scanner]📸[/bright_black] Starting to process {total_dirs} directories")

        # 2. Scan each directory
        for directory in directories:
            # Check stop flag before starting each directory
            if self.stop_flag_ref and self.stop_flag_ref():
                self.logger.warning("[bright_black][scanner]📸[/bright_black] Stop flag detected. Stopping directory processing.")
                break
                
            try:
                result = self.scan_and_load(table_name, db_connection, db_manager, directory)
                processed_dirs += 1
                self.logger.info(f"[bright_black][scanner]📸[/bright_black] Processed directory {processed_dirs}/{total_dirs}: {directory}")
            except Exception as e:
                self.logger.error(f"[bright_black][scanner]📸[/bright_black] Error processing directory {directory}: {e}")
                continue
        
        self.logger.info(f"[bright_black][scanner]📸[/bright_black] Completed processing {processed_dirs}/{total_dirs} directories")
        return processed_dirs

    def log_pre_scan_stats(self, directories: List[str]) -> None:
        """
        Gathers and logs the number of videos/images in each directory 
        and subdirectory before the actual scanning.
        Excludes files and dirs matching the patterns in EXCLUDE_FILE_PATTERNS / EXCLUDE_DIR_PATTERNS.
        """
        total_video_count = 0
        total_image_count = 0
        total_directories = 0
        
        self.logger.info("=" * 100)
        self.logger.info(f"[bold cyan]PRE-SCAN STATISTICS[/bold cyan]")
        self.logger.info("=" * 100)
        
        for i, directory in enumerate(directories, start=1):
            dir_video_count = 0
            dir_image_count = 0
            subdir_stats = {}
            non_empty_subdirs = 0

            for root, dirs, files in os.walk(directory):
                # Exclude certain directories
                dirs[:] = [d for d in dirs if not self._should_exclude_dir(d)]
                
                subdir_name = os.path.relpath(root, start=directory)
                if subdir_name == ".":
                    subdir_name = "(top-level)"

                # Exclude certain files
                files = [f for f in files if not self._should_exclude_file(f)]

                video_count = 0
                image_count = 0

                for filename in files:
                    ext = os.path.splitext(filename)[1].lower()
                    if ext in video_extensions:
                        video_count += 1
                    elif ext in image_extensions:
                        image_count += 1

                subdir_stats[subdir_name] = {
                    "videos": video_count,
                    "images": image_count,
                    "total": video_count + image_count
                }

                dir_video_count += video_count
                dir_image_count += image_count
                if video_count > 0 or image_count > 0:
                    non_empty_subdirs += 1

            total_video_count += dir_video_count
            total_image_count += dir_image_count
            total_directories += non_empty_subdirs
            
            # Format with thousands separators
            formatted_video = f"{dir_video_count:,}"
            formatted_image = f"{dir_image_count:,}"
            formatted_total = f"{dir_video_count + dir_image_count:,}"
            
            self.logger.info(f"[bold green]{i}. Source directory[/bold green] [bold yellow]'{directory}'[/bold yellow]")
            self.logger.info(f"   [bold magenta]Videos:[/bold magenta] {formatted_video} | [bold blue]Images:[/bold blue] {formatted_image} | [bold white]Total:[/bold white] {formatted_total}")
            
            # Sort subdirectories by total file count (descending)
            sorted_subdirs = sorted(
                [(name, stats) for name, stats in subdir_stats.items() if stats['total'] > 0],
                key=lambda x: x[1]['total'],
                reverse=True
            )
            
            if sorted_subdirs:
                self.logger.info("   [bold cyan]Subdirectories:[/bold cyan]")
                
                # Use better indentation and numbering
                for idx, (subdir_name, statdict) in enumerate(sorted_subdirs, 1):
                    prefix = f"      {idx:02d}."
                    self.logger.info(
                        f"{prefix} [bold white]{subdir_name}:[/bold white] "
                        f"[magenta]Videos: {statdict['videos']:,}[/magenta] | "
                        f"[blue]Images: {statdict['images']:,}[/blue] | "
                        f"Total: {statdict['total']:,}"
                    )
                
                self.logger.info(f"   [bold cyan]Total subdirectories:[/bold cyan] {non_empty_subdirs}")
            
            self.logger.info("-" * 100)

        # Overall summary
        self.logger.info(f"[bold green]SUMMARY:[/bold green]")
        self.logger.info(f"[bold yellow]Total directories:[/bold yellow] {len(directories)}")
        self.logger.info(f"[bold yellow]Total subdirectories:[/bold yellow] {total_directories}")
        self.logger.info(f"[bold magenta]Total videos:[/bold magenta] {total_video_count:,}")
        self.logger.info(f"[bold blue]Total images:[/bold blue] {total_image_count:,}")
        self.logger.info(f"[bold white]Total files:[/bold white] {total_video_count + total_image_count:,}")
        self.logger.info("=" * 100)
        self.logger.info("📸 [bold green]Pre-scan completed. Starting file processing...[/bold green]")
        self.logger.info("=" * 100)

    def scan_and_load(self, table_name: str, db_connection: DatabaseConnection, db_manager: DatabaseManager, base_directory: str) -> int:
        """
        Walk the given directory, process each media file (image or video),
        compute relevant hashes, run YOLO, and insert records into the database.
        Also excludes directories/files matching EXCLUDE_DIR_PATTERNS / EXCLUDE_FILE_PATTERNS.
        """
        file_count = 0
        processed_count = 0
        skipped_count = 0
        error_count = 0
        
        self.logger.info(f"[bright_black][scanner]📸[/bright_black] Scanning directory for files: {base_directory}")
        
        # First collect all eligible files
        all_files = []
        try:
            for root, dirs, files in os.walk(base_directory):
                # Check stop flag during directory traversal
                if self.stop_flag_ref and self.stop_flag_ref():
                    self.logger.info("[bright_black][scanner]📸[/bright_black] Stop flag is set. Stopping file collection.")
                    break
                    
                # Exclude directories
                dirs[:] = [d for d in dirs if not self._should_exclude_dir(d)]
                # Exclude files
                files = [f for f in files if not self._should_exclude_file(f)]
                
                for filename in files:
                    file_path = os.path.join(root, filename)
                    file_info = self._extract_file_components(file_path)
                    
                    # Skip non-media files
                    if file_info.file_type not in ['image', 'video']:
                        skipped_count += 1
                        self.logger.debug(
                            f"[bright_black][scanner]📸[/bright_black] "
                            f"Skipping non-media file: {file_path} (type={file_info.file_type})"
                        )
                        continue
                    
                    all_files.append((file_path, filename))
        except Exception as e:
            self.logger.critical(
                f"[bright_black][scanner]📸[/bright_black] "
                f"Critical error during directory scan: {e}\n{traceback.format_exc()}"
            )
            return 0
        
        # Sort files by priority
        self.logger.info(f"[bright_black][scanner]📸[/bright_black] Sorting {len(all_files)} files by priority pattern...")
        sorted_files = sorted(all_files, key=lambda x: self._get_file_priority(x[1]))
        
        # Process files in priority order
        for idx, (file_path, _) in enumerate(sorted_files, 1):
            # Check global stop_flag each iteration
            if self.stop_flag_ref and self.stop_flag_ref():
                self.logger.info("[bright_black][scanner]📸[/bright_black] Stop flag is set. Stopping file processing.")
                break

            file_count += 1
            priority = self._get_file_priority(os.path.basename(file_path))
            self.logger.info(f"[bright_black][scanner]📸[/bright_black][#FFA500]🔄 Processing file {file_count}/{len(sorted_files)} (Priority {priority}): {file_path}[/#FFA500]")
            
            try:
                # Insert the media file
                self._insert_media(table_name, db_connection, db_manager, file_path)
                processed_count += 1
                # A small delay to prevent hammering the DB too hard
                time.sleep(0.1)

            except Exception as e:
                error_count += 1
                self.logger.error(
                    f"[bright_black][scanner]📸[/bright_black] "
                    f"Database insert error for {file_path}: {e}\n{traceback.format_exc()}"
                )
                continue
            
            # Progress log every 100 files
            if file_count % 100 == 0:
                self.logger.info(
                    f"[bright_black][scanner]📸[/bright_black] "
                    f"Progress: {file_count}/{len(sorted_files)} files processed, {processed_count} successful, {error_count} errors"
                )

        self.logger.info(
            f"[bright_black][scanner]📸[/bright_black] Processing completed. "
            f"Total files found: {file_count}, Media files processed: {processed_count}, "
            f"Errors: {error_count}, Skipped: {skipped_count}"
        )
        return processed_count

    ##############################################################################################################################
    # Private Methods
    ##############################################################################################################################

    def _insert_media(
        self,
        table_name: str,
        db_connection: DatabaseConnection,
        db_manager: DatabaseManager,
        downloaded_file_path: str
    ) -> None:
        """
        Insert metadata for a single file into the DB, computing all hashes/YOLO/fingerprints as needed.
        """
        file_info = self._extract_file_components(downloaded_file_path)
        file_path = file_info.path
        file_type = file_info.file_type
        file_name = file_info.filename
        file_size = file_info.file_size
        file_directory = file_info.directory
        file_extension = file_info.extension

        # 1) Skip early if it's not image or video
        if file_type not in ['image', 'video']:
            self.logger.debug(f"[bright_black][scanner]📸[/bright_black]⏭️ Skipping non-media file: {file_path} (type={file_type})")
            return

        try:
            # 2) Compute only file-level BLAKE3 first
            file_hashes = self.hash_calculator.calculate_file_hash(file_path)
            blake3 = file_hashes.blake3  # We'll use this to check DB
            if not blake3:
                self.logger.warning(f"[bright_black][scanner]📸[/bright_black][yellow]⏭️ Skipping (missing blake3)[/yellow]: {file_path}.")
                return

            # 3) Check if this BLAKE3 already exists in DB
            if db_manager.exists_by_blake3(db_connection.connection, table_name, blake3):
                self.logger.info(f"[bright_black][scanner]📸[/bright_black][yellow]⏭️ Skipping (exists)[/yellow]: {file_path}")
                return

            # 4) Not in DB → compute other hashes or YOLO if needed
            md5 = file_hashes.md5
            sha256 = file_hashes.sha256
            sha512 = file_hashes.sha512

            dhash = phash = whash = chash = ahash = None
            has_human = False
            human_score = 0.0
            human_count = 0
            video_fingerprint = None
            video_width = video_height = 0
            video_length = 0.0
            video_fps = 0.0
            video_resolution = None

            # 5) If it’s an image → do image hashing & YOLO
            if file_type == 'image':
                image_hashes = self.hash_calculator.calculate_image_hash(file_path)
                dhash = image_hashes.dhash
                phash = image_hashes.phash
                whash = image_hashes.whash
                chash = image_hashes.chash
                ahash = image_hashes.ahash

                yolo_result = self.yolo_provider.has_human(file_path)
                has_human = yolo_result.has_human
                human_score = float(yolo_result.confidence)
                human_count = yolo_result.human_count

            # 6) If it’s a video → do fingerprinting
            elif file_type == 'video':
                vid_fp = self.video_fingerprinter.extract_fingerprint(file_path)
                video_fingerprint = vid_fp.hex
                video_width = vid_fp.width
                video_height = vid_fp.height
                video_length = vid_fp.length
                video_fps = vid_fp.fps
                video_resolution = vid_fp.resolution

            # 7) Insert into DB
            db_manager.insert(
                connection=db_connection.connection,
                table_name=table_name,
                file_path=file_path,
                file_size=file_size,
                file_directory=file_directory,
                file_name=file_name,
                file_type=file_type,
                file_extension=file_extension,
                md5=md5,
                sha256=sha256,
                sha512=sha512,
                blake3=blake3,
                dhash=dhash,
                phash=phash,
                whash=whash,
                chash=chash,
                ahash=ahash,
                has_human=has_human,
                has_human_score=human_score,
                has_human_count=human_count,
                video_fingerprint=video_fingerprint,
                video_width=video_width,
                video_height=video_height,
                video_length=video_length,
                video_fps=video_fps,
                video_resolution=video_resolution
            )

            self.logger.info(
                f"[bright_black][scanner]📸[/bright_black][bold #FFA500]"
                f"🎯 Record inserted successfully: {file_path} into {table_name}.[/bold #FFA500]"
            )

        except Exception as e:
            self.logger.error(
                f"[bright_black][scanner]📸[/bright_black][bold red]"
                f"🔄 Failed to process file {file_path}[/bold red]: {e}\n\t{traceback.format_exc()}"
            )

    def _extract_file_components(self, file_path: str) -> BasicFileInfo:
        """
        Extract file path components into BasicFileInfo.
        Determines whether it's an image, video, other, or unknown.
        """
        abs_path = os.path.abspath(file_path)
        directory_path = os.path.dirname(abs_path)
        filename = os.path.basename(abs_path)
        basename, extension = os.path.splitext(filename)
        extension = extension[1:] if extension.startswith('.') else extension
        file_size = os.path.getsize(abs_path) if os.path.exists(abs_path) else 0

        if abs_path.lower().endswith(tuple(image_extensions)):
            file_type = 'image'
        elif abs_path.lower().endswith(tuple(video_extensions)):
            file_type = 'video'
        elif abs_path.lower().endswith(tuple(others_extensions)):
            file_type = 'other'
        else:
            file_type = 'unknown'
        
        return BasicFileInfo(
            path=abs_path,
            filename=filename,
            directory=os.path.basename(directory_path),
            extension=extension,
            file_type=file_type,
            file_size=file_size
        )

    def _should_exclude_file(self, filename: str) -> bool:
        """
        Returns True if the given filename matches any pattern in EXCLUDE_FILE_PATTERNS.
        """
        for pat in EXCLUDE_FILE_PATTERNS:
            if fnmatch.fnmatch(filename, pat):
                return True
        return False

    def _should_exclude_dir(self, dirname: str) -> bool:
        """
        Returns True if the given dirname matches any pattern in EXCLUDE_DIR_PATTERNS.
        """
        for pat in EXCLUDE_DIR_PATTERNS:
            if fnmatch.fnmatch(dirname, pat):
                return True
        return False

    # Add this function to the Scanner class to determine file priority
    def _get_file_priority(self, filename: str) -> int:
        """
        Determine the priority of a file based on filename pattern.
        Lower number => higher priority.
        """
        # Highest priority: Instaloader exact pattern with timestamp and shortcode
        # Exact format: YYYYMMDD_HHMMSS_shortcode.ext
        # Example: 20231009_154612_C1x2Yz3AbC.jpg
        if re.match(r'\d{8}_\d{6}_\w+\.\w+$', filename):
            return 1

        # Pattern 2: Instagram-like format "{date:%Y%m%d_%H%M%S}_{shortcode}"
        # Example: "20231009_154612_ABCDEF"
        if re.match(r'\d{8}_\d{6}_\w+', filename):
            return 2

        # Pattern 3: format like "..._20231009_154612_something_2.jpg"
        if re.match(r'.*\d{8}_\d{6}_\w+_\d+\.\w+$', filename):
            return 3

        # Pattern 4: files that don't have (1).jpg at the end
        if not re.search(r'\(\d+\)\.\w+$', filename):
            return 4
        
        # Pattern 5: everything else
        return 5