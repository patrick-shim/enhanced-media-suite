from ast import Tuple
import os
import traceback
import re
import time
import collections
import random
import datetime
from dataclasses import dataclass
from source.logging_modules import CustomLogger
from source.database_modules import DatabaseConnection, DatabaseManager
from source.hash_modules import HashCalculator
from source.fingerprint_modules import VideoFingerprinter
from source.yolo_modules import YoloProvider
import instaloader
from instaloader import (
        Instaloader,
        TwoFactorAuthRequiredException, 
        BadCredentialsException, 
        ConnectionException, 
        Profile, 
        RateController, 
        QueryReturnedBadRequestException)

# Base RateController config
MAX_REQUESTS_PER_HOUR = 100
MIN_REQUEST_INTERVAL = 2.0
MIN_REQUEST_JITTER = 1.0
INITIAL_BACKOFF = 20
BACKOFF_FACTOR = 1.5
DAILY_LIMIT = 0

POSTS_BEFORE_WAIT_MIN = 5
POSTS_BEFORE_WAIT_MAX = 30

LONG_PAUSE_WAIT_MIN = 30
LONG_PAUSE_WAIT_MAX = 3600

class RateController(instaloader.RateController):
    def __init__(self, context):
        super().__init__(context)
        self.logger = CustomLogger(__name__).get_logger()

        # For normal request limiting
        self.hourly_request_times = collections.deque(maxlen=MAX_REQUESTS_PER_HOUR)
        self.daily_request_times = collections.deque(maxlen=DAILY_LIMIT if DAILY_LIMIT > 0 else None)
        self.last_request_time = 0.0

        # Error counters
        self.consecutive_429_errors = 0
        self.consecutive_softblock_errors = 0

        # Human-like pause logic
        # Start with a random target (e.g., after 3-7 posts, take a longer break)
        self.posts_since_pause = 0
        self.posts_until_next_pause = random.randint(POSTS_BEFORE_WAIT_MIN, POSTS_BEFORE_WAIT_MAX)  

    def wait_before_query(self, query_type: str):
        """
        Called by Instaloader before each request.  
        We do:
          1) Hourly and daily limit checks
          2) Minimum interval with jitter
          3) Possibly a "human-like" longer pause after some posts
        """
        now = time.time()

        # Reset error counters on a successful request
        self.consecutive_429_errors = 0
        self.consecutive_softblock_errors = 0

        # (A) Hourly limit
        if len(self.hourly_request_times) == self.hourly_request_times.maxlen:
            oldest_hr = self.hourly_request_times[0]
            elapsed_hr = now - oldest_hr
            if elapsed_hr < 3600:
                wait_time = 3600 - elapsed_hr
                self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black] Hourly limit reached ({MAX_REQUESTS_PER_HOUR}/hr). Sleeping {wait_time:.0f}s.")
                self._sleep(wait_time)
                now = time.time()

        # (B) Daily limit logic (only if DAILY_LIMIT > 0)
        if DAILY_LIMIT > 0:
            while self.daily_request_times and (now - self.daily_request_times[0] > 86400):
                self.daily_request_times.popleft()

            if len(self.daily_request_times) == self.daily_request_times.maxlen:
                oldest_daily = self.daily_request_times[0]
                elapsed_daily = now - oldest_daily
                wait_time = 86400 - elapsed_daily
                self.logger.warning(f"[bright_black][RateLimiter]🚦[/bright_black] Daily limit of {DAILY_LIMIT} in 24hr window reached. Sleeping {wait_time:.0f}s.")
                self._sleep(wait_time)
                now = time.time()

        # (C) Minimum interval + random jitter
        elapsed = now - self.last_request_time
        min_interval_with_jitter = MIN_REQUEST_INTERVAL + random.uniform(0, MIN_REQUEST_JITTER)
        if elapsed < min_interval_with_jitter:
            wait_time = min_interval_with_jitter - elapsed
            self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black][bold bright_blue] Sleeping {wait_time:.2f}s (min interval).[/bold bright_blue]")
            self._sleep(wait_time)
            now = time.time()

        # (D) Human-like random "long break"
        # Increment the post count, and if we've hit the threshold, do a bigger wait
        self.posts_since_pause += 1
        if self.posts_since_pause >= self.posts_until_next_pause:
            long_pause = random.uniform(LONG_PAUSE_WAIT_MIN, LONG_PAUSE_WAIT_MAX)  # e.g. 1-5 minutes
            self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black] Human-like long pause: sleeping {long_pause:.2f}s after {self.posts_since_pause} posts.")
            self._sleep(long_pause)
            self.posts_since_pause = 0
            self.posts_until_next_pause = random.randint(POSTS_BEFORE_WAIT_MIN, POSTS_BEFORE_WAIT_MAX)  # re-randomize

        # (E) Record timestamp
        self.hourly_request_times.append(time.time())
        if DAILY_LIMIT > 0:
            self.daily_request_times.append(time.time())
        self.last_request_time = time.time()

        self.logger.debug(f"[bright_black][RateLimiter]🚦[/bright_black] Query '{query_type}' at {time.strftime('%X')}.")

    def handle_soft_block(self, query_type: str, message: str):
        """
        Example approach: exponential backoff for repeated 401 "please wait" blocks
        """
        self.consecutive_softblock_errors += 1
        base_backoff = 1800  # e.g. 30 minutes
        factor = 1.5
        backoff_secs = base_backoff * (factor ** (self.consecutive_softblock_errors - 1))
        self.logger.warning(f"[bright_black][RateLimiter]🚦[/bright_black][bold bright_red] Soft-block. Sleeping for {backoff_secs:.0f}s.[/bold bright_red]")
        self._sleep(backoff_secs)

    def handle_429(self, query_type: str):
        """
        Exponential backoff for repeated 429 errors
        """
        self.consecutive_429_errors += 1
        backoff_secs = INITIAL_BACKOFF * (BACKOFF_FACTOR ** (self.consecutive_429_errors - 1))
        self.logger.error(f"[bright_black][RateLimiter]🚦[/bright_black][bold bright_blue] 429. Backing off {backoff_secs:.1f}s.[/bold bright_blue]")
        self._sleep(backoff_secs)

    def get_config():
        return {
            MAX_REQUESTS_PER_HOUR,
            DAILY_LIMIT
        }

    def sleep(self, secs: float):
        """Called by Instaloader in some situations."""
        self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black][bold bright_red] Sleeping {secs:.2f}s by Instaloader's request.[/bold bright_red]")
        self._sleep(secs)

    def _sleep(self, secs: float):
        """Unified place for all sleeps (with possible KeyboardInterrupt catching)."""
        try:
            time.sleep(secs)
        except KeyboardInterrupt:
            self.logger.warning("[bright_black][RateLimiter]🚦[/bright_black] Sleep interrupted by user.")
            raise

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

class InstagramFetcher:
    def __init__(self, logger, hash_calculator: HashCalculator, yolo_provider: YoloProvider, video_fingerprinter: VideoFingerprinter, skip_database: bool = False):
        self.logger = logger
        self.hash_calculator = hash_calculator
        self.yolo_provider = yolo_provider
        self.video_fingerprinter = video_fingerprinter
        self.skip_database = skip_database
        
    def reset_table(self, table_name: str, db_connection: DatabaseConnection, db_manager: DatabaseManager) -> None:
        if db_connection and db_manager:
            try:
                db_manager.reset_table(db_connection.connection, table_name)
                self.logger.info(f"[bright_black][Fetcher]📸[/bright_black][bold green] Table '{table_name}' reset successfully.[/bold green]")
            except Exception as e:
                self.logger.error(f"[bright_black][Fetcher]📸[/bright_black] Failed to reset table {table_name}: {e}")

    def instagram_login(self, username: str, loader: Instaloader, is_relogin: bool) -> bool:
        from getpass import getpass
        session_file_path = self._get_default_session_filename(username)
        try:
            if is_relogin:
                self._delete_session_for_relogin(username)

            if os.path.isfile(session_file_path):
                loader.load_session_from_file(username, session_file_path)
                self.logger.info(f"[bright_black][Fetcher]📸[/bright_black][bold magenta] Reusing session for[/bold magenta] [bold green]{username}[/bold green].")
                return True
            
            else:
                self.logger.info(f"[bright_black][Fetcher]📸[/bright_black][bold yellow]No saved session, logging in as {username}...[/bold yellow]")
                password = getpass(prompt=f"[bright_black][Fetcher]📸[/bright_black]Instagram password for {username}: ")
                loader.login(username, password)
                loader.save_session_to_file(session_file_path)
                self.logger.info(f"[bright_black][Fetcher]📸[/bright_black][bold green]Saved session to {session_file_path}.[/bold green]")
                return True
        
        except BadCredentialsException:
            self.logger.error("[bright_black][Fetcher]📸[/bright_black][bold red]Login failed: Bad username or password.[/bold red]")
            return False
        
        except TwoFactorAuthRequiredException:
            code = input("[bright_black][Fetcher]📸[/bright_black]Enter 2FA code: ")
            try:
                loader.two_factor_login(code)
                loader.save_session_to_file(session_file_path)
                self.logger.info("[bright_black][Fetcher]📸[/bright_black][bold green]Two-factor auth success, session saved.[/bold green]")
                return True
            except Exception as e:
                self.logger.error(f"[bright_black][Fetcher]📸[/bright_black][bold red]2FA login failed:[/bold red] {e}")
                return False
        except Exception as e:
            self.logger.error(f"[bright_black][Fetcher]📸[/bright_black][bold red]Unexpected login error:[/bold red] {e}")
            return False

    def reverse_scan(
        self, 
        table_name: str, 
        db_connection: DatabaseConnection, 
        db_manager: DatabaseManager, 
        base_directory: str, 
        reset_table: bool = True, 
        display_stats: bool = True) -> int:

        file_count = 0
        processed_count = 0
        skipped_count = 0
        error_count = 0
        
        if display_stats:
            self._display_pre_scan_stats(base_directory)

        if reset_table:
            self.logger.info(f"[bright_black][Fetcher]📸[/bright_black] Resetting table {table_name} before scanning...")
            
            try:
                db_manager.reset_table(db_connection.connection, table_name)
                self.logger.info(f"[bright_black][Fetcher]📸[/bright_black] Table {table_name} reset successfully.")
            
            except Exception as e:
                self.logger.error(f"[bright_black][Fetcher]📸[/bright_black] Failed to reset table {table_name}: {e}")
                return 0

        self.logger.info(f"[bright_black][Fetcher]📸[/bright_black] Scanning directory for reverse load: {base_directory}")
        
        # First, collect all media files
        all_files = []

        try:
            for root, _, files in os.walk(base_directory):
                for filename in files:
                    file_path = os.path.join(root, filename)
                    file_info = self._extract_file_components(file_path)
                    
                    # Skip non-media files
                    if file_info.file_type not in ['image', 'video']:
                        skipped_count += 1
                        self.logger.debug(f"[bright_black][Fetcher]📸[/bright_black] Skipping non-media file: {file_path} (type={file_info.file_type})")
                        continue
                    
                    all_files.append((file_path, filename))
        except Exception as e:
            self.logger.critical(f"[bright_black][Fetcher]📸[/bright_black] Critical error during directory scan: {e}\n{traceback.format_exc()}")
            return 0
        
        # Define a function to determine priority (lower number = higher priority)
        def _get_priority(filename):
            # Pattern 1: Instagram format "{date:%Y%m%d_%H%M%S}_{shortcode}"
            if re.match(r'\d{8}_\d{6}_\w+', filename):
                return 1
            
            # Pattern 2: Format like ".../20181105_113442_gojoonhee_2.jpg"
            if re.match(r'.*\d{8}_\d{6}_\w+_\d+\.\w+$', filename):
                return 2
            
            # Pattern 3: Files that DON'T have (1).jpg at the end
            if not re.search(r'\(\d+\)\.\w+$', filename):
                return 3
            
            # Pattern 4: Everything else
            return 4
        
        # Sort files by priority
        self.logger.info(f"[bright_black][Fetcher]📸[/bright_black] Sorting {len(all_files)} files by priority pattern...")
        sorted_files = sorted(all_files, key=lambda x: _get_priority(x[1]))
        
        # Now process files in order of priority
        for idx, (file_path, _) in enumerate(sorted_files, 1):
            file_count += 1
            
            # Log with priority information
            priority = _get_priority(os.path.basename(file_path))
            self.logger.info(f"[bright_black][Fetcher]📸[/bright_black] Processing file {idx}/{len(sorted_files)} (Priority {priority}): {file_path}")
            
            try:
                # Process and insert the media file
                self._insert_media(table_name, db_connection, db_manager, file_path)
                processed_count += 1
                
                # Add a small delay to prevent database overload
                time.sleep(0.1)
            except Exception as e:
                error_count += 1
                self.logger.error(f"[bright_black][Fetcher]📸[/bright_black] Database insert error for {file_path}: {e}\n{traceback.format_exc()}")
                continue
            
            # Progress log for every 100 files
            if idx % 100 == 0:
                self.logger.info(f"[bright_black][Fetcher]📸[/bright_black] Progress: {idx}/{len(sorted_files)} files processed, {processed_count} successful, {error_count} errors")
        
        self.logger.info(f"[bright_black][Fetcher]📸[/bright_black] Reverse load completed. Total files found: {file_count}, Media files processed: {processed_count}, Errors: {error_count}, Skipped: {skipped_count}")
        return processed_count

    def process_posts(
        self,
        db_connection: DatabaseConnection,
        db_manager: DatabaseManager,
        table_name: str,
        L: Instaloader,
        profile_name: str,
        download_directory: str,
        save_to: str,
        limit: int = None
    ) -> None:
        """
        Main public method that just calls a single consolidated helper, plus top-level error handling.
        """
        try:
            self._download_and_process_posts(
                L=L,
                profile_name=profile_name,
                save_to=save_to,
                download_directory=download_directory,
                table_name=table_name,
                db_connection=db_connection,
                db_manager=db_manager,
                limit=limit
            )

        except ConnectionException as e:
            # If we want to handle "Please wait a few minutes" or other 
            # ConnectionException triggers at a high level
            if "please wait a few minutes" in str(e).lower():
                if isinstance(L.context._rate_controller, RateController):
                    L.context._rate_controller.handle_soft_block("download_post", str(e))
            self.logger.error(f"[bright_black][Fetcher]📸[/bright_black] Connection error: {e}")
        
        except QueryReturnedBadRequestException as e:
            if "checkpoint_required" in str(e).lower() or "challenge_required" in str(e).lower():
                self.logger.error("[bright_black][Fetcher]📸[/bright_black] Instagram requires verification. Please log in through a browser.")
                input("[bright_black][Fetcher]📸[/bright_black] Press Enter after completing verification in the browser...")
                self.logger.info("[bright_black][Fetcher]📸[/bright_black] Retrying after checkpoint verification...")
                # Optional: You could add recursive retry logic here
            else:
                self.logger.error(f"[bright_black][Fetcher]📸[/bright_black] Bad request error: {e}")
        
        except Exception as e:
            self.logger.error(
                f"[bright_black][Fetcher]📸[/bright_black] Unexpected error processing posts for '{profile_name}': {e}\n"
                f"{traceback.format_exc()}"
            )

    ##############################################################################################################################
    # Private Methods
    ##############################################################################################################################
    def _display_pre_scan_stats(self, base_directory: str) -> None:
        """
        Gathers and logs statistics about media files before processing.
        Shows counts of images and videos in each subdirectory.
        """
        total_dirs = 0
        total_image_count = 0
        total_video_count = 0
        dir_stats = {}
        
        self.logger.info("=" * 50)
        self.logger.info(f"[bold cyan]PRE-SCAN STATISTICS[/bold cyan]")
        self.logger.info("=" * 50)
        
        try:
            # First, collect stats by directory
            for root, dirs, files in os.walk(base_directory):
                # Skip hidden directories or system directories
                dirs[:] = [d for d in dirs if not d.startswith('.') and not d.startswith('$')]
                
                rel_path = os.path.relpath(root, start=base_directory)
                if rel_path == '.':
                    rel_path = "(root)"
                    
                image_count = 0
                video_count = 0
                
                for filename in files:
                    # Skip hidden files
                    if filename.startswith('.') or filename.startswith('~'):
                        continue
                        
                    ext = os.path.splitext(filename)[1].lower()
                    if ext in image_extensions:
                        image_count += 1
                    elif ext in video_extensions:
                        video_count += 1
                
                if image_count > 0 or video_count > 0:
                    dir_stats[rel_path] = {
                        'images': image_count,
                        'videos': video_count,
                        'total': image_count + video_count
                    }
                    total_image_count += image_count
                    total_video_count += video_count
                    total_dirs += 1
            
            # Print the stats
            self.logger.info(f"[bold green]BASE DIRECTORY:[/bold green] [bold yellow]'{base_directory}'[/bold yellow]")
            self.logger.info(f"[bold magenta]Total subdirectories with media:[/bold magenta] {total_dirs}")
            self.logger.info(f"[bold blue]Total images:[/bold blue] {total_image_count:,}")
            self.logger.info(f"[bold cyan]Total videos:[/bold cyan] {total_video_count:,}")
            self.logger.info(f"[bold white]Total media files:[/bold white] {total_image_count + total_video_count:,}")
            
            # Sort directories by total file count and print details
            if dir_stats:
                self.logger.info("\n[bold green]DIRECTORY BREAKDOWN:[/bold green]")
                sorted_dirs = sorted(dir_stats.items(), key=lambda x: x[1]['total'], reverse=True)
                
                for i, (dir_path, stats) in enumerate(sorted_dirs, 1):
                    self.logger.info(
                        f"{i:2d}. [bold white]{dir_path}:[/bold white] "
                        f"[blue]Images: {stats['images']:,}[/blue] | "
                        f"[cyan]Videos: {stats['videos']:,}[/cyan] | "
                        f"Total: {stats['total']:,}"
                    )
                    
                    # Limit to top 20 directories if there are many
                    if i >= 20 and len(sorted_dirs) > 25:
                        remaining = len(sorted_dirs) - i
                        self.logger.info(f"   ... and {remaining} more directories")
                        break
                        
            self.logger.info("=" * 100)
            self.logger.info("📸 [bold green]Pre-scan completed. Starting file processing...[/bold green]")
            self.logger.info("=" * 100)
            
        except Exception as e:
            self.logger.error(f"[bright_black][Fetcher]📸[/bright_black] Error collecting pre-scan statistics: {e}")
            self.logger.debug(traceback.format_exc())
            
    def _download_and_process_posts(
        self,
        L: Instaloader,
        profile_name: str,
        save_to: str,
        download_directory: str,
        table_name: str,
        db_connection: DatabaseConnection,
        db_manager: DatabaseManager,
        limit: int = None
    ) -> None:
        """
        Retrieves the profile's posts, handles 'challenge_required', and downloads each post
        with up to 3 retries on ConnectionException. Then processes inserted media in the DB.
        Consolidates what used to be scattered among multiple try/except blocks.
        """
        try:
            # 1) Attempt to get the posts with challenge handling
            media_count, posts = self._retrieve_posts(L, profile_name)
            
            file_count = 0
            max_retries = 3

            self.logger.info(f"[bright_black][Fetcher]📸[/bright_black] Fetching posts from: [bold green] {profile_name}[/bold green]")
            self.logger.info(f"[bright_black][Fetcher]📸[/bright_black] Saved Directory: [bold green]{save_to}[/bold green]")
            self.logger.info(f"[bright_black][Fetcher]📸[/bright_black] Download Path: {download_directory}")
            self.logger.info(f"[bright_black][Fetcher]📸[/bright_black] Estimated Posts: {media_count}")

            current_media_count = 0

            # Convert the generator to a list to handle exceptions during iteration
            post_list = []
            try:
                # Limit how many posts we attempt to collect to avoid long-running operations
                post_limit = limit if limit is not None else float('inf')  # Default to 50 if no limit provided
                for i, post in enumerate(posts):
                    post_list.append(post)
                    if i + 1 >= post_limit:
                        break

            except QueryReturnedBadRequestException as e:
                if "checkpoint_required" in str(e).lower():
                    self.logger.error("[bright_black][Fetcher]📸[/bright_black] Instagram checkpoint required while fetching posts.")
                    input("[bright_black][Fetcher]📸[/bright_black] Please log in via browser/app, resolve the checkpoint, then press Enter to continue...")
                    # After manual intervention, try to retrieve posts again
                    _, posts = self._retrieve_posts(L, profile_name)
                    # Try again to collect posts
                    post_limit = limit if limit is not None else float('inf')
                    for i, post in enumerate(posts):
                        post_list.append(post)
                        if i + 1 >= post_limit:
                            break
                else:
                    raise    

            for post in post_list:
                # 2) Retry logic for each post
                for attempt in range(max_retries):
                    try:
                        L.download_post(post, target=save_to)
                        current_media_count += 1
                        break  # If successful, break out of retry loop
                    except ConnectionException as e:
                        self.logger.warning(
                            f"[bright_black][Fetcher]📸[/bright_black] ConnectionException: {e} - Attempt {attempt+1}/{max_retries} failed."
                        )
                        # If it's the last attempt, re-raise or log
                        if attempt == max_retries - 1:
                            self.logger.error(
                                f"[bright_black][Fetcher]📸[/bright_black] Failed to download post {post.shortcode} after {max_retries} attempts."
                            )
                            raise

                # 3) Process newly downloaded files
                final_download_directory = os.path.join(download_directory)
                if os.path.isdir(final_download_directory):
                    for file in os.listdir(final_download_directory):
                        if post.shortcode in file:
                            file_path = os.path.join(final_download_directory, file)
                            self._insert_media(table_name, db_connection, db_manager, file_path)
                            file_count += 1
                            self.logger.info(f"[bright_black][Fetcher]📸[/bright_black][green] Total {file_count} files completed ({current_media_count}/{media_count})[/green]")
                # 4) Honor optional limit
                if limit is not None and file_count >= limit:
                    break
        except Exception as e:
            self.logger.error(f"[bright_black][Fetcher]📸[/bright_black] Error in download_and_process_posts: {e}")
            raise

    def _retrieve_posts(self, L: Instaloader, profile_name: str) -> Tuple:
        """
        Retrieve the post generator for the given profile_name,
        gracefully handling 'challenge_required' if needed.
        """
        media_count = 0
        retries = 3

        while retries > 0:
            retries -= 1
            try:
                profile = Profile.from_username(L.context, profile_name)

                if profile:
                    media_count = profile.mediacount
                    # Return the generator directly
                    return media_count, profile.get_posts()

            except QueryReturnedBadRequestException as e:
                error_message = str(e).lower()
                # Check for both "challenge_required" or "checkpoint_required"
                if "challenge_required" in error_message or "checkpoint_required" in error_message:
                    self.logger.error("[bright_black][Fetcher]📸[/bright_black] Instagram challenge/checkpoint required. Log in via browser/app.")
                    input("[bright_black][Fetcher]📸[/bright_black] Press Enter once challenge is resolved, or Ctrl+C to abort.")
                    if retries == 0:
                        self.logger.error("[bright_black][Fetcher]📸[/bright_black] Challenge still not resolved after 3 attempts.")
                        raise
                    # else: continue looping
                else:
                    # Some other 400 error
                    raise
            except Exception as e:
                self.logger.error(f"[bright_black][Fetcher]📸[/bright_black] Error retrieving posts: {e}")
                if retries == 0:
                    raise
                time.sleep(5)  # Wait a bit before retrying
                
        # If we get here, we gave up
        raise RuntimeError("Could not retrieve posts after repeated attempts.")
        
    # this is a critical method that gets / checks for the session file. 
    def _get_default_session_filename(self, username: str) -> str:
        home = os.path.expanduser("~")
        session_dir = os.path.join(home, ".config", "instaloader")
        if not os.path.isdir(session_dir):
            os.makedirs(session_dir, exist_ok=True)
        return os.path.join(session_dir, f"session-{username}")

    def _insert_media(
        self,
        table_name: str,
        db_connection: DatabaseConnection,
        db_manager: DatabaseManager,
        downloaded_file_path: str
    ) -> None:

        file_info = self._extract_file_components(downloaded_file_path)
        file_path = file_info.path
        file_type = file_info.file_type
        file_name = file_info.filename
        file_size = file_info.file_size
        file_directory = file_info.directory
        file_extension = file_info.extension

        if self.skip_database:
            self.logger.debug(f"[bright_black][Fetcher]📸[/bright_black] --skip-database: not inserting {downloaded_file_path} into DB")
            return

        # 1) Skip early if it's not image or video
        if file_type not in ['image', 'video']:
            self.logger.debug(f"[bright_black][Fetcher]📸[/bright_black] Skipping non-media file: {file_path} (type={file_type})")
            return

        #self.logger.info(f"[bright_black][Fetcher]📸[/bright_black][bright_blue] Processing file:[/bright_blue] {file_path}")

        try:
            # 2) Compute only file-level BLAKE3 first
            file_hashes = self.hash_calculator.calculate_file_hash(file_path)
            blake3 = file_hashes.blake3  # We'll use this to check DB
            if not blake3:
                self.logger.warning(f"[bright_black][Fetcher]📸[/bright_black] [yellow]Skipping (missing blake3)[/yellow]: {file_path}.")
                return

            # 3) Check if this BLAKE3 already exists in DB
            if db_manager.exists_by_blake3(db_connection.connection, table_name, blake3):
                # 'exists_by_blake3' is a custom method you'd implement in your db_manager
                self.logger.info(f"[bright_black][Fetcher]📸[/bright_black] [yellow]Skipping (exists)[/yellow]: {file_path}")
                return

            # 4) Not in DB → compute other hashes or YOLO if needed
            md5 = file_hashes.md5
            sha256 = file_hashes.sha256
            sha512 = file_hashes.sha512

            # Initialize fields in case we skip or raise
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
                # you have two different ways to check for human in image (one in local yolo model, and the other is yolo api)
                # if --use-yolo-api is set, use yolo api
                # yolo_result = self.yolo_provider.has_human(file_path, use_api=True)
                # human_score = float(yolo_result.confidence)
                # human_count = yolo_result.human_count
                # if --use-yolo-api is not set, then yuse local yolo model
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

            self.logger.info(f"[bright_black][Fetcher]📸[/bright_black][bold #FFA500]🎯 Record inserted successfully: {file_path} into {table_name}.[/bold #FFA500]")

        except Exception as e:
            self.logger.error(f"[bright_black][Fetcher]📸[/bright_black][bold red]🔄 Failed to process file {file_path}[/bold red]: {e}\n\t{traceback.format_exc()}")

    def _delete_session_for_relogin(self, username: str):
        session_file_path = self._get_default_session_filename(username)
        if os.path.isfile(session_file_path):
            os.remove(session_file_path)
            self.logger.info(f"[bright_black][Fetcher]📸[/bright_black][bold yellow] Deleted session file for[/bold yellow] [bold green]{username}[/bold green].")
            return True
        return False
    
    def _extract_file_components(self, file_path: str) -> BasicFileInfo:
        """Extract file path components into BasicFileInfo."""
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