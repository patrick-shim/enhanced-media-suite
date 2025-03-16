import os
import re
import shutil
import hashlib
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List

from source.yolo_modules import YoloProvider
from source.logging_modules import CustomLogger

class Merger:
    """
    Merges multiple source directories into a single destination, 
    applying file-by-file rules to avoid overwriting entire folders.
    """ 
    def __init__(
        self,
        logger,
        sources: List[str],
        image_destination: str,
        video_destination: str,
        thread_count: int = 4,
        yolo_provider: YoloProvider = None,
        human_only: bool = False
    ):
        """
        :param logger:           A logger instance
        :param sources:          List of paths to source directories
        :param image_destination: Destination directory for image files
        :param video_destination: Destination directory for video files
        :param thread_count:     Number of threads for parallel merging
        """
        self.logger = logger
        self.sources = sources
        self.image_destination = image_destination
        self.video_destination = video_destination
        self.thread_count = thread_count
        self.yolo_provider = yolo_provider
        self.human_only = human_only
        
        # Define file extensions for classification
        self.image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.heic', '.heif'}
        self.video_extensions = {'.mp4', '.mov', '.avi', '.wmv', '.mkv', '.flv', '.webm', '.m4v', '.3gp', '.mpg', '.mpeg'}

    def run(self, stop_flag_ref: Callable[[], bool]):
        """
        Main entry point to perform the merge operation.
        Walk each source directory and merge files into appropriate destinations.
        """
        self.logger.info("[Merger] Starting merge run.")
        
        # Ensure destination directories exist
        os.makedirs(self.image_destination, exist_ok=True)
        os.makedirs(self.video_destination, exist_ok=True)
        
        with ThreadPoolExecutor(max_workers=self.thread_count) as executor:
            # For each source directory
            for src_dir in self.sources:
                if stop_flag_ref():
                    self.logger.warning("[Merger] Stop flag set. Aborting further merging.")
                    return

                if not os.path.isdir(src_dir):
                    self.logger.warning(f"[Merger] Source directory not found: {src_dir}")
                    continue

                self.logger.info(f"[Merger] Merging from source: {src_dir}")
                # Walk the source directory
                for root, dirs, files in os.walk(src_dir):
                    # Figure out the relative path from src_dir
                    rel_path = os.path.relpath(root, start=src_dir)
                    
                    # Create destination subdirectories in both image and video destinations
                    image_dest_subdir = os.path.join(self.image_destination, rel_path)
                    video_dest_subdir = os.path.join(self.video_destination, rel_path)
                    
                    os.makedirs(image_dest_subdir, exist_ok=True)
                    os.makedirs(video_dest_subdir, exist_ok=True)

                    for filename in files:
                        if stop_flag_ref():
                            break

                        source_file_path = os.path.join(root, filename)
                        
                        # Determine if it's an image or video by extension
                        file_ext = os.path.splitext(filename)[1].lower()
                        
                        if file_ext in self.image_extensions:
                            destination_file_path = os.path.join(image_dest_subdir, filename)
                            destination_type = "image"
                        elif file_ext in self.video_extensions:
                            destination_file_path = os.path.join(video_dest_subdir, filename)
                            destination_type = "video"
                        else:
                            # Skip unknown file types
                            self.logger.debug(f"[Merger] Skipping unknown file type: {source_file_path}")
                            continue
                        
                        # Submit a job for each file
                        executor.submit(self._handle_file, source_file_path, destination_file_path, destination_type, stop_flag_ref)

    def _handle_file(self, source_path: str, dest_path: str, file_type: str, stop_flag_ref: Callable[[], bool]):
        """
        Handle a single file with advanced deduplication and prioritization logic.
        
        :param source_path: Full path to the source file
        :param dest_path: Proposed destination path for the file
        :param file_type: Type of file (image/video)
        :param stop_flag_ref: Function to check if operation should stop
        """
        # Immediately check stop flag
        if stop_flag_ref():
            return

        was_human = False
        self.logger.debug(f"[Merger] currently processing {source_path}")
        # Rule 0: If human_only is enabled and no human detected => skip
        if self.human_only and file_type == "image" and self.yolo_provider:
            try:
                yolo_result = self.yolo_provider.has_human(source_path)
                if not yolo_result.has_human:
                    return
                # Log human detection details
                was_human = True

            except Exception as e:
                self.logger.error(f"[Merger] Error checking human detection in {source_path}: {e}")
                return

        # Compute source file's blake3 hash
        try:
            src_blake3 = self._calculate_blake3(source_path)
            if not src_blake3:
                self.logger.warning(f"[Merger] Could not compute hash for {source_path}. Skipping.")
                return
        except Exception as e:
            self.logger.error(f"[Merger] Error computing hash for {source_path}: {e}")
            return

        # Destination directory
        dest_dir = os.path.dirname(dest_path)

        # Comprehensive content check across destination directory
        for existing_file in os.listdir(dest_dir):
            full_existing_path = os.path.join(dest_dir, existing_file)
            
            # Skip if it's a directory or not a file
            if not os.path.isfile(full_existing_path):
                continue
            
            try:
                # Compute hash of existing file
                existing_blake3 = self._calculate_blake3(full_existing_path)
                
                # If content matches
                if existing_blake3 == src_blake3:
                    # Determine priority between source and existing file
                    existing_priority = self._get_file_priority(existing_file)
                    source_priority = self._get_file_priority(os.path.basename(source_path))
                    
                    if source_priority < existing_priority:
                        # Source file has higher priority, replace existing file
                        try:
                            self.logger.info(
                                f"[Merger] Replacing existing file with higher priority source: "
                                f"{full_existing_path} <- {source_path}"
                            )
                            os.remove(full_existing_path)
                        except Exception as e:
                            self.logger.error(f"[Merger] Error replacing file: {e}")
                            return
                    else:
                        # Existing file has equal or higher priority
                        self.logger.debug(
                            f"[Merger] Skipping file with lower priority: {source_path}"
                        )
                        return

            except Exception as e:
                self.logger.error(f"[Merger] Error processing existing file {full_existing_path}: {e}")
                continue

        # If we've passed all checks, copy the file
        try:
            # Ensure destination directory exists
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            
            # Copy the file
            shutil.copy2(source_path, dest_path)
            
            self.logger.info(f"[Merger] Copied new ({file_type}, human: {was_human}) file: {source_path} -> {dest_path}")
        except Exception as e:
            self.logger.error(f"[Merger] Error copying {file_type} file {source_path} -> {dest_path}: {e}")

    def _find_same_content_file(self, directory: str, file_blake3: str) -> str:
        """
        Look in `directory` for a file with matching blake3. 
        Return that file's path if found, else empty string.
        """
        try:
            for f in os.listdir(directory):
                candidate = os.path.join(directory, f)
                if os.path.isfile(candidate):
                    c_blake3 = self._calculate_blake3(candidate)
                    if c_blake3 == file_blake3:
                        return candidate
        except Exception as e:
            self.logger.error(f"[Merger] Error scanning directory {directory}: {e}")
        return ""

    def _calculate_blake3(self, file_path: str) -> str:
        """
        Calculate the BLAKE3-like digest for the file. 
        (We can emulate with blake2b from hashlib if standard BLAKE3 not installed.)
        """
        hasher = hashlib.blake2b()
        try:
            with open(file_path, "rb") as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    hasher.update(chunk)
        except Exception as e:
            self.logger.error(f"[Merger] Error reading file for BLAKE3: {file_path} => {e}")
            return ""
        return hasher.hexdigest()

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