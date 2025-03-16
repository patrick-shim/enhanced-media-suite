import re
import os
import shutil
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Callable, Optional, List, Dict

from source.logging_modules import CustomLogger
from source.database_modules import DatabaseManager

# Optional: Add recognized image/video extensions if you prefer strict extension checks
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.heic', '.heif'}
VIDEO_EXTENSIONS = {'.mp4', '.mov', '.avi', '.wmv', '.mkv', '.flv', '.webm', '.m4v', '.3gp', '.mpg', '.mpeg'}

class Copier:
    """
    Handles copying files from the DB-specified source paths to
    their respective destinations (image vs video).
    
    Supports:
      - Dedupe-based filtering (copy only representative if dedupe used)
      - 'Human only' filtering for images (via YOLO detection)
      - Threaded copying with a stop flag for graceful interruption
    """
    def __init__(
        self,
        logger,
        db_manager: DatabaseManager,
        db_connection,
        video_destination: str,
        image_destination: str,
        directory_depth: int = 0,
        human_only: bool = False,
        dedupe_option: str = "none"
    ):
        """
        :param logger:           The shared logger instance (CustomLogger)
        :param db_manager:       Instance of DatabaseManager
        :param db_connection:    An active pyodbc connection
        :param video_destination:Base directory for video files
        :param image_destination:Base directory for image files
        :param directory_depth:  How many nested folders to preserve from source
        :param human_only:       If True, only copy images that contain humans
        :param dedupe_option:    If "none", uses the base table. If "dhash" or "phash", uses dedupe tables
        """
        self.logger = logger
        self.db_manager = db_manager
        self.connection = db_connection
        self.video_destination = video_destination
        self.image_destination = image_destination
        self.directory_depth = directory_depth
        self.human_only = human_only
        self.dedupe_option = dedupe_option

        # Confirm destinations exist
        os.makedirs(self.video_destination, exist_ok=True)
        os.makedirs(self.image_destination, exist_ok=True)

    def run(self, thread_count: int = 4, stop_flag_ref: Callable[[], bool] = lambda: False):
        """
        Main method to fetch the file list from DB and perform copying in a thread pool.

        :param thread_count:  Number of worker threads for parallel copying
        :param stop_flag_ref: A function returning True if copying should stop immediately
        """
        # 1) Fetch list of media from DB
        media_list = self.fetch_media_records()
        total_files = len(media_list)
        self.logger.info(f"[Copier] Starting copy of {total_files} items with {thread_count} threads.")

        # 2) Threaded copying
        copied_count = 0
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            futures = []
            for index, record in enumerate(media_list, start=1):
                if stop_flag_ref():
                    self.logger.warning("[Copier] Stop flag detected. Aborting remaining tasks.")
                    break
                future = executor.submit(self._worker_copy_file, record, stop_flag_ref)
                futures.append(future)

                if index % 100 == 0:
                    self.logger.info(f"[Copier] Progress: {index}/{total_files} copied...")

            # Optional: Wait for all tasks to complete
            for fut in futures:
                if stop_flag_ref():
                    break
                success = fut.result()  # Wait for this task
                if success:
                    copied_count += 1
        
        self.logger.info(f"[Copier] Copy process completed. {copied_count}/{total_files} successfully copied.")

    def fetch_media_records(self) -> List[Dict]:
        """
        Fetch media records from the database. If dedupe is not "none",
        we require 'is_representative'. If dedupe=none, assume all are representative.

        Return: List of dicts with keys:
            - file_path
            - file_type (image|video|unknown)
            - is_representative (bool)
        """
        table_name = self._deduce_table_name()
        query = ""
        if self.dedupe_option == "none":
            query = f"""
                SELECT file_path, file_type, has_human
                FROM [dbo].[{table_name}]
            """
        else:
            # For deduped tables, we expect is_representative col
            # plus the base columns
            query = f"""
                SELECT file_path, file_type, is_representative, has_human
                FROM [dbo].[{table_name}]
            """

        rows = self.db_manager.fetch(self.connection, query)

        records = []
        for row in rows:
            file_path = row.file_path
            ftype = row.file_type
            has_human = row.has_human

            if self.dedupe_option == "none":
                rec = {
                    "file_path": file_path,
                    "file_type": ftype,
                    "is_representative": True, # by default
                    "has_human": has_human
                }
            else:
                rep_flag = bool(row.is_representative)
                rec = {
                    "file_path": file_path,
                    "file_type": ftype,
                    "is_representative": rep_flag,
                    "has_human": has_human
                }
            records.append(rec)
        return records

    def _deduce_table_name(self) -> str:
        """
        Determine which table to use based on dedupe_option.
        Adjust as needed for your real table names in the DB.
        """
        # Example mapping
        if self.dedupe_option == "none":
            # Original table (like "tbl_scanner")
            return "tbl_scanner"
        elif self.dedupe_option == "dhash":
            return "tbl_scanner_deduped_dhash"
        elif self.dedupe_option == "phash":
            return "tbl_scanner_deduped_phash"
        elif self.dedupe_option == "twophase":
            return "tbl_scanner_deduped_dhash_phash"
        else:
            # fallback if unknown
            return "tbl_scanner"

    def _worker_copy_file(self, record: Dict, stop_flag_ref: Callable[[], bool]) -> bool:
        """
        Worker function to copy a single file, respecting 'human_only' if it's an image
        and 'is_representative' if dedupe was used.
        """
        # If a global stop is signaled, skip
        if stop_flag_ref():
            return False

        file_path = record["file_path"]
        file_type = record["file_type"]
        is_representative = record["is_representative"]

        # Check existence
        if not os.path.isfile(file_path):
            self.logger.warning(f"[Copier] File not found: {file_path}")
            return False

        # Check if we skip non-representative
        if not is_representative:
            # For deduped tables, skip if not representative
            self.logger.debug(f"[Copier] Skipping non-representative file: {file_path}")
            return False

        # Determine if image vs video
        #  - Some older DB entries might have 'unknown' or partial data
        #  - Double-check extension to be safe
        actual_type = self._determine_file_type(file_path, file_type)
        if not actual_type:
            self.logger.debug(f"[Copier] Skipping file with unsupported extension: {file_path}")
            return False

        try:
            if actual_type == "image":
                # If "human_only" is set, verify YOLO detection
                if self.human_only:
                    has_human = record.get("has_human", False)
                    if not has_human:
                        self.logger.info(f"[Copier] Skipping image with no human: {file_path}")
                        return False
                # Copy to images destination
                dest_base = self.image_destination

            elif actual_type == "video":
                # Copy to video destination
                dest_base = self.video_destination
            else:
                # Should never happen if all checks pass
                return False

            dest_path = self._preserve_directory_structure(file_path, dest_base, self.directory_depth)
            success = self._copy_file(file_path, dest_path)
            return success
        except Exception as e:
            self.logger.error(f"[Copier] Unexpected error copying {file_path}: {e}")
            self.logger.debug(traceback.format_exc())
            return False

    def _determine_file_type(self, file_path: str, db_file_type: str) -> Optional[str]:
        """
        Check the file extension to see if it's an image or video.
        If extension check conflicts with DB info, we log a warning but
        still proceed with extension-based classification.
        """
        ext = os.path.splitext(file_path)[1].lower()
        # Extension-based
        if ext in IMAGE_EXTENSIONS:
            return "image"
        elif ext in VIDEO_EXTENSIONS:
            return "video"
        else:
            # If the DB says it's image/video but extension is not recognized, trust DB
            if db_file_type.lower() in ["image", "video"]:
                self.logger.warning(
                    f"[Copier] Extension {ext} not recognized. Falling back to DB file_type={db_file_type} for {file_path}"
                )
                return db_file_type.lower()
            # Otherwise, we skip
            return None

    def _preserve_directory_structure(self, source_path: str, dest_base: str, depth: int) -> str:
        """
        Create a destination path that preserves up to `depth` levels from the source path.
        If depth=0, place everything directly in dest_base with no subfolders.
        """
        if depth <= 0:
            # Flat structure
            return os.path.join(dest_base, os.path.basename(source_path))

        # Normalize path
        norm_path = os.path.normpath(source_path)
        path_parts = norm_path.split(os.sep)

        # last item is the filename
        filename = path_parts[-1]

        # Keep the last `depth` folders (not counting filename)
        if len(path_parts) <= (depth + 1):
            preserved_dirs = path_parts[:-1]
        else:
            preserved_dirs = path_parts[-(depth+1):-1]

        result_path = dest_base
        for d in preserved_dirs:
            # skip empty
            if d.strip():
                result_path = os.path.join(result_path, d)

        return os.path.join(result_path, filename)



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

        # Pattern 1: Instagram-like format "{date:%Y%m%d_%H%M%S}_{shortcode}"
        # Example: "20231009_154612_ABCDEF"
        if re.match(r'\d{8}_\d{6}_\w+', filename):
            return 2

        # Pattern 2: format like "..._20231009_154612_something_2.jpg"
        if re.match(r'.*\d{8}_\d{6}_\w+_\d+\.\w+$', filename):
            return 3

        # Pattern 3: files that don't have (1).jpg at the end
        if not re.search(r'\(\d+\)\.\w+$', filename):
            return 4

        # Pattern 4: everything else
        return 5

    def _copy_file(self, source_path: str, dest_path: str) -> bool:
        """
        Perform the actual file copy, creating directories as needed.
        Prioritize files based on filename patterns if a file with the same name exists.
        """
        try:
            # Check if destination file already exists
            if os.path.exists(dest_path):
                # Compare priorities of existing and new file
                existing_priority = self._get_file_priority(os.path.basename(dest_path))
                source_priority = self._get_file_priority(os.path.basename(source_path))
                
                # If source file has higher priority, replace the existing file
                if source_priority < existing_priority:
                    self.logger.info(f"[Copier] Replacing file with higher priority: {dest_path}")
                    os.remove(dest_path)
                else:
                    # Skip copying if existing file has equal or higher priority
                    self.logger.debug(f"[Copier] Skipping file with lower priority: {source_path}")
                    return False

            # Create destination directory if it doesn't exist
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            
            # Copy the file
            shutil.copy2(source_path, dest_path)
            self.logger.info(f"[Copier] Copied file: {source_path} -> {dest_path}")
            return True
        except Exception as e:
            self.logger.error(f"[Copier] Copy error {source_path} -> {dest_path}: {e}")
            return False