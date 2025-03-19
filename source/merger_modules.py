import os
import re
import shutil
import hashlib
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List, Set, Any

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
        yolo_provider: Any = None,
        human_only: bool = False
    ):
        """
        :param logger:           A logger instance
        :param sources:          List of paths to source directories
        :param image_destination: Destination directory for image files
        :param video_destination: Destination directory for video files
        :param thread_count:     Number of threads for parallel merging
        :param yolo_provider:    Provider for human detection in images
        :param human_only:       Only copy images containing humans
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
        
        :param stop_flag_ref: Function that returns True if operation should stop
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
                    if stop_flag_ref():
                        self.logger.warning("[Merger] Stop flag set. Aborting further merging.")
                        break

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
                        
                        # Human detection for images if enabled
                        if self.human_only and file_ext in self.image_extensions and self.yolo_provider:
                            try:
                                yolo_result = self.yolo_provider.has_human(source_file_path)
                                if not yolo_result.has_human:
                                    self.logger.debug(f"[Merger] Skipping image with no humans: {source_file_path}")
                                    continue
                                was_human = True
                            except Exception as e:
                                self.logger.error(f"[Merger] Error checking human detection in {source_file_path}: {e}")
                                continue
                        else:
                            was_human = False

                        # Submit the file handling to the thread pool
                        executor.submit(
                            self._handle_file, 
                            source_file_path, 
                            destination_file_path, 
                            destination_type, 
                            was_human, 
                            stop_flag_ref
                        )

    def _handle_file(self, source_path: str, dest_path: str, file_type: str, was_human: bool, stop_flag_ref: Callable[[], bool]):
        """
        Handle a single file with advanced deduplication and prioritization logic.
        
        :param source_path: Full path to the source file
        :param dest_path: Proposed destination path for the file
        :param file_type: Type of file (image/video)
        :param was_human: Flag indicating if a human was detected in the image
        :param stop_flag_ref: Function to check if operation should stop
        """
        # Immediately check stop flag
        if stop_flag_ref():
            return

        self.logger.debug(f"[Merger] Currently processing {source_path}")

        # Compute source file's hash
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

        # First check if the destination file already exists
        if os.path.exists(dest_path):
            try:
                # Calculate hash of existing file
                dest_blake3 = self._calculate_blake3(dest_path)
                
                # If it's the same file (same hash), no need to copy
                if dest_blake3 == src_blake3:
                    self.logger.debug(f"[Merger] File already exists with same content: {dest_path}")
                    return
                
                # If different content but same name, determine priority
                source_priority = self._get_file_priority(os.path.basename(source_path))
                dest_priority = self._get_file_priority(os.path.basename(dest_path))
                
                if source_priority < dest_priority:
                    # Source has higher priority, replace the destination file
                    self.logger.info(
                        f"[Merger] Replacing existing file with higher priority source: "
                        f"{dest_path} <- {source_path}"
                    )
                    os.remove(dest_path)
                    # Continue to copy the file below
                else:
                    # Destination has equal or higher priority, skip
                    self.logger.debug(
                        f"[Merger] Skipping file with lower or equal priority: {source_path}"
                    )
                    return
            except Exception as e:
                self.logger.error(f"[Merger] Error checking existing file {dest_path}: {e}")
                return
                
        # Check for files with same content but different names in the destination directory
        for existing_file in os.listdir(dest_dir):
            if stop_flag_ref():
                return
                
            full_existing_path = os.path.join(dest_dir, existing_file)
            
            # Skip if it's a directory or not a file
            if not os.path.isfile(full_existing_path) or full_existing_path == dest_path:
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
                            # We'll copy to the original destination path
                            # This allows for a rename if filenames are different
                        except Exception as e:
                            self.logger.error(f"[Merger] Error replacing file: {e}")
                            return
                    else:
                        # Existing file has equal or higher priority
                        self.logger.debug(
                            f"[Merger] Skipping file with lower or equal priority: {source_path}"
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

    def _calculate_blake3(self, file_path: str) -> str:
        """
        Calculate the BLAKE3-like digest for the file using blake2b from hashlib.
        
        :param file_path: Path to the file
        :return: Hex digest string or empty string on error
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
            self.logger.error(f"[Merger] Error reading file for hash calculation: {file_path} => {e}")
            return ""
        return hasher.hexdigest()

    def _get_file_priority(self, filename: str) -> int:
        """
        Determine the priority of a file based on filename pattern.
        Lower number => higher priority.
        
        :param filename: The filename to check
        :return: Priority value (1-5, where 1 is highest)
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