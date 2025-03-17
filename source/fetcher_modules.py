from ast import Tuple
import os
import traceback
import re
import time
import collections
import random
from typing import Optional, Tuple
from zoneinfo import ZoneInfo
from enum import Enum
from datetime import datetime
from dataclasses import dataclass
import instaloader
from source.logging_modules import CustomLogger
from source.database_modules import DatabaseConnection, DatabaseManager
from source.hash_modules import HashCalculator
from source.fingerprint_modules import VideoFingerprinter
from source.yolo_modules import YoloProvider
from instaloader import (
        Instaloader,
        TwoFactorAuthRequiredException, 
        BadCredentialsException, 
        ConnectionException, 
        Profile, 
        RateController, 
        QueryReturnedBadRequestException)

# Constants
SECONDS_IN_MINUTE = 60
SECONDS_IN_HOUR = 60 * SECONDS_IN_MINUTE
SECONDS_IN_DAY = 24 * SECONDS_IN_HOUR
SECONDS_IN_WEEK = 7 * SECONDS_IN_DAY
SECONDS_IN_MONTH = 30 * SECONDS_IN_DAY

# Configurable variables
MIN_REQUESTS_PER_MINUTE = 1
MAX_REQUESTS_PER_MINUTE = 30

MIN_REQUESTS_PER_HOUR = 10
MAX_REQUESTS_PER_HOUR = 100

MIN_REQUESTS_PER_DAY = 1
MAX_REQUESTS_PER_DAY = 100

MIN_REQUESTS_PER_WEEK = 1
MAX_REQUESTS_PER_WEEK = 50

MIN_REQUESTS_PER_MONTH = 1
MAX_REQUESTS_PER_MONTH = 100

POSTS_BEFORE_WAIT_MIN = 10
POSTS_BEFORE_WAIT_MAX = 50

PAUSE_BEFORE_BETWEEN_POSTS_MIN = 1
PAUSE_BEFORE_BETWEEN_POSTS_MAX = 5

DAILY_LIMIT_REQUEST_MIN = 100
DAILY_LIMIT_REQUEST_MAX = 200

INITIAL_BACKOFF_FACTOR = 20
BACKOFF_FACTOR = 1.5

# Time-based rate control constants
MORNING_START = 5  # 5 AM
MORNING_END = 8  # 8 AM
DAY_START = 9  # 9 AM
DAY_END = 12  # 12 PM
MIDDAY_START = 13  # 1 PM
MIDDAY_END = 18  # 6 PM
EVENING_START = 18  # 6 PM
EVENING_END = 22  # 10 PM
NIGHT_START = 22  # 10 PM
NIGHT_END = 24  # 12 AM
SLEEP_START = 0  # 12 AM
SLEEP_END = 5  # 5 AM

# Request limits by time of day
EARLY_MORNING_MIN_REQUESTS = 5
EARLY_MORNING_MAX_REQUESTS = 10
MORNING_MIN_REQUESTS = 10
MORNING_MAX_REQUESTS = 50
MIDDAY_MIN_REQUESTS = 10
MIDDAY_MAX_REQUESTS = 50
EVENING_MIN_REQUESTS = 30
EVENING_MAX_REQUESTS = 100
LATE_NIGHT_MIN_REQUESTS = 20
LATE_NIGHT_MAX_REQUESTS = 80
SLEEP_REQUESTS = 0

LONG_PAUSE_WAIT_MIN = 30  # 30 seconds minimum for long pauses
LONG_PAUSE_WAIT_MAX = 300  # 5 minutes maximum for long pauses

class TimeBasedRateLimit(Enum):
    """ Rate Limits for different time periods """
    SLEEP = 0
    EARLY_MORNING = 1
    MORNING = 2
    MID_DAY = 3
    EVENING = 4
    LATE_NIGHT = 5
    DAILY = 6
    WEEKLY = 7
    MONTHLY = 8


class RateController(instaloader.RateController):
    """Custom rate controller for Instaloader to manage request rates"""

    def __init__(self, context, timezone="Asia/Seoul", logger=None):
        super().__init__(context)

        # Get logger if provided or use print statements
        if logger:
            self.logger = logger
        else:
            from source.logging_modules import CustomLogger
            self.logger = CustomLogger(__name__).get_logger()

        # Configuration parameters
        self.min_requests_per_hour = MIN_REQUESTS_PER_HOUR
        self.max_requests_per_hour = MAX_REQUESTS_PER_HOUR
        self.min_requests_per_min = MIN_REQUESTS_PER_MINUTE
        self.max_requests_per_min = MAX_REQUESTS_PER_MINUTE
        self.posts_before_wait_min = POSTS_BEFORE_WAIT_MIN
        self.posts_before_wait_max = POSTS_BEFORE_WAIT_MAX
        self.pause_before_between_posts_min = PAUSE_BEFORE_BETWEEN_POSTS_MIN
        self.pause_before_between_posts_max = PAUSE_BEFORE_BETWEEN_POSTS_MAX
        self.daily_limit_request_min = DAILY_LIMIT_REQUEST_MIN
        self.daily_limit_request_max = DAILY_LIMIT_REQUEST_MAX
        self.initial_backoff_factor = INITIAL_BACKOFF_FACTOR

        # Set the timezone for time-based rate limiting
        self.timezone = ZoneInfo(timezone)

        # For rate tracking
        self.minute_request_times = collections.deque(maxlen=MAX_REQUESTS_PER_MINUTE)
        self.hourly_request_times = collections.deque(maxlen=MAX_REQUESTS_PER_HOUR)
        self.daily_request_times = collections.deque(maxlen=MAX_REQUESTS_PER_DAY)
        self.weekly_request_times = collections.deque(maxlen=MAX_REQUESTS_PER_WEEK)
        self.monthly_request_times = collections.deque(maxlen=MAX_REQUESTS_PER_MONTH)
        self.last_request_time = 0.0

        # Error handling counters
        self.consecutive_429_errors = 0
        self.consecutive_403_errors = 0
        self.consecutive_500_errors = 0

        # Human-like pause logic
        self.posts_since_pause = 0
        self.posts_until_next_pause = random.randint(self.posts_before_wait_min, self.posts_before_wait_max)

        # Set the daily request limit based on the specified range
        self.daily_request_limit = random.randint(DAILY_LIMIT_REQUEST_MIN, DAILY_LIMIT_REQUEST_MAX)
        self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black] RateController initialized with daily limit: {self.daily_request_limit} requests")

    def wait_before_query(self, query_type: str):
        """
        Called by Instaloader before each request.
        Implements:
        1. Time-based rate control
        2. Mandatory minimum interval with jitter
        3. Per-minute, per-hour, per-day limits
        4. Human-like pauses between posts
        """
        now = time.time()
        current_datetime = datetime.now(self.timezone)

        # Reset error counters on successful requests
        self.consecutive_429_errors = 0
        self.consecutive_403_errors = 0
        self.consecutive_500_errors = 0

        # 1. Time-based rate control
        time_period, min_req, max_req = self._get_time_based_rate_control(current_datetime)

        # If we're in sleep time, wait until the end of sleep period
        if time_period == TimeBasedRateLimit.SLEEP:
            sleep_hours_end = SLEEP_END
            current_hour = current_datetime.hour

            if current_hour < sleep_hours_end:
                # Calculate seconds until SLEEP_END hour
                wait_seconds = (sleep_hours_end - current_hour) * SECONDS_IN_HOUR
                # Subtract already elapsed minutes and seconds
                wait_seconds -= (current_datetime.minute * SECONDS_IN_MINUTE + current_datetime.second)

                self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black] Sleep time period. Waiting until {sleep_hours_end}:00 AM - {wait_seconds // 60} minutes, {wait_seconds % 60} seconds")
                self._sleep(wait_seconds)
                return self.wait_before_query(query_type)  # Retry after sleep

        # 2. Apply per-minute limit
        if len(self.minute_request_times) >= max_req:
            oldest_req = self.minute_request_times[0]
            elapsed = now - oldest_req

            if elapsed < SECONDS_IN_MINUTE:
                wait_time = SECONDS_IN_MINUTE - elapsed
                self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black] Per-minute limit reached. Waiting {wait_time:.2f}s")
                self._sleep(wait_time)
                now = time.time()

        # 3. Apply per-hour limit
        if len(self.hourly_request_times) >= self.max_requests_per_hour:
            oldest_hr_req = self.hourly_request_times[0]
            elapsed_hr = now - oldest_hr_req

            if elapsed_hr < SECONDS_IN_HOUR:
                wait_time = SECONDS_IN_HOUR - elapsed_hr
                self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black] Hourly limit reached ({self.max_requests_per_hour}/hr). Waiting {wait_time:.0f}s")
                self._sleep(wait_time)
                now = time.time()

        # 4. Apply daily limit
        if len(self.daily_request_times) >= self.daily_request_limit:
            # Clean up old entries from more than 24 hours ago
            while self.daily_request_times and (now - self.daily_request_times[0] > SECONDS_IN_DAY):
                self.daily_request_times.popleft()

            if len(self.daily_request_times) >= self.daily_request_limit:
                oldest_daily = self.daily_request_times[0]
                elapsed_daily = now - oldest_daily

                if elapsed_daily < SECONDS_IN_DAY:
                    wait_time = SECONDS_IN_DAY - elapsed_daily
                    self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black] Daily limit of {self.daily_request_limit} reached. Waiting {wait_time / SECONDS_IN_HOUR:.1f} hours")
                    self._sleep(wait_time)
                    now = time.time()

        # 5. Minimum interval + random jitter
        elapsed = now - self.last_request_time
        min_interval = 60.0 / max_req  # Minimum seconds between requests
        jitter = random.uniform(0, min_interval * 0.5)  # Up to 50% random jitter

        min_interval_with_jitter = min_interval + jitter

        if elapsed < min_interval_with_jitter:
            wait_time = min_interval_with_jitter - elapsed
            self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black] Applying minimum interval. Waiting {wait_time:.2f}s")
            self._sleep(wait_time)
            now = time.time()

        # 6. Human-like random "long break" between posts
        if query_type in ["get_feed_posts", "get_profile", "get_post_page", "get_igtv_page"]:
            self.posts_since_pause += 1

            if self.posts_since_pause >= self.posts_until_next_pause:
                # Take a longer pause after a certain number of posts
                long_pause = random.uniform(LONG_PAUSE_WAIT_MIN, LONG_PAUSE_WAIT_MAX)
                self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black] Taking a human-like break after {self.posts_since_pause} posts. Pausing for {long_pause:.1f}s")
                self._sleep(long_pause)

                # Reset the post counter and randomize the next pause
                self.posts_since_pause = 0
                self.posts_until_next_pause = random.randint(self.posts_before_wait_min, self.posts_before_wait_max)

                now = time.time()

        # 7. Record the request time in all tracking deques
        current_time = time.time()
        self.minute_request_times.append(current_time)
        self.hourly_request_times.append(current_time)
        self.daily_request_times.append(current_time)
        self.weekly_request_times.append(current_time)
        self.monthly_request_times.append(current_time)
        self.last_request_time = current_time

        # Log the query being made
        self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black] Executing query '{query_type}' at {datetime.now(self.timezone).strftime('%H:%M:%S')}")

    def _get_time_based_rate_control(self, current_time: datetime) -> Tuple[TimeBasedRateLimit, int, int]:
        """
        Determine rate limits based on time of day.
        Returns a tuple of (time_period, min_requests, max_requests)

        Time periods:
        - Early Morning (5am-8am): 5-10 requests per minute
        - Morning (9am-12pm): 10-50 requests per minute
        - Mid-day (1pm-6pm): 10-50 requests per minute
        - Evening (6pm-10pm): 30-100 requests per minute
        - Late Night (10pm-12am): 20-80 requests per minute
        - Sleep (12am-5am): 0 requests per minute (sleep mode)
        """
        hour = current_time.hour

        if SLEEP_START <= hour < SLEEP_END:
            # Sleep time (12am-5am): No requests
            return TimeBasedRateLimit.SLEEP, 0, 0

        elif MORNING_START <= hour < MORNING_END:
            # Early morning (5am-8am): Very low rate
            return TimeBasedRateLimit.EARLY_MORNING, EARLY_MORNING_MIN_REQUESTS, EARLY_MORNING_MAX_REQUESTS

        elif DAY_START <= hour < DAY_END:
            # Morning (9am-12pm): Moderate rate
            return TimeBasedRateLimit.MORNING, MORNING_MIN_REQUESTS, MORNING_MAX_REQUESTS

        elif MIDDAY_START <= hour < MIDDAY_END:
            # Mid-day (1pm-6pm): Moderate rate
            return TimeBasedRateLimit.MID_DAY, MIDDAY_MIN_REQUESTS, MIDDAY_MAX_REQUESTS

        elif EVENING_START <= hour < EVENING_END:
            # Evening (6pm-10pm): High rate
            return TimeBasedRateLimit.EVENING, EVENING_MIN_REQUESTS, EVENING_MAX_REQUESTS

        else:  # NIGHT_START <= hour < NIGHT_END
            # Late night (10pm-12am): Moderate-high rate
            return TimeBasedRateLimit.LATE_NIGHT, LATE_NIGHT_MIN_REQUESTS, LATE_NIGHT_MAX_REQUESTS

    def handle_200(self, query_type: str):
        """Handle successful request (HTTP 200) - reset error counters"""
        self.consecutive_429_errors = 0
        self.consecutive_403_errors = 0
        self.consecutive_500_errors = 0
        self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black] Request '{query_type}' succeeded with 200 OK")

    def handle_400(self, query_type: str):
        """Handle Bad Request (HTTP 400) - client-side error"""
        self.logger.warning(f"Bad request (400) for '{query_type}'. This is usually a client-side error.")
        # Implement a small delay
        wait_time = random.uniform(1.0, 3.0)
        self.logger.warning(f"[bright_black][RateLimiter]🚦[/bright_black] Waiting {wait_time:.2f}s after 400 error")
        self._sleep(wait_time)

    def handle_401(self, query_type: str):
        """Handle Unauthorized (HTTP 401) - auth issues"""
        self.logger.warning(f"[bright_black][RateLimiter]🚦[/bright_black] Unauthorized (401) for '{query_type}'. Authentication issue detected.")
        # Suggest login or re-authentication
        wait_time = random.uniform(5.0, 10.0)
        self.logger.warning(f"[bright_black][RateLimiter]🚦[/bright_black] Waiting {wait_time:.2f}s after 401 error")
        self._sleep(wait_time)

    def handle_403(self, query_type: str):
        """Handle Forbidden (HTTP 403) - may indicate account actions needed"""
        self.consecutive_403_errors += 1
        backoff_secs = self.initial_backoff_factor * (BACKOFF_FACTOR ** (self.consecutive_403_errors - 1))

        # Cap the maximum backoff time to 2 hours
        backoff_secs = min(backoff_secs, 7200)

        self.logger.error(f"[bright_black][RateLimiter]🚦[/bright_black] Forbidden (403) for '{query_type}'. This may indicate account actions required.")
        self.logger.warning(f"[bright_black][RateLimiter]🚦[/bright_black] Consecutive 403 errors: {self.consecutive_403_errors}. Backing off for {backoff_secs:.1f}s")
        self._sleep(backoff_secs)

    def handle_404(self, query_type: str):
        """Handle Not Found (HTTP 404) - resource doesn't exist"""
        self.logger.error(f"[bright_black][RateLimiter]🚦[/bright_black] Not found (404) for '{query_type}'. The requested resource doesn't exist.")
        # Minimal waiting for 404s as they're expected sometimes
        wait_time = random.uniform(1.0, 2.0)
        self._sleep(wait_time)

    def handle_429(self, query_type: str):
        """Handle Too Many Requests (HTTP 429) - rate limiting"""
        self.consecutive_429_errors += 1
        backoff_secs = self.initial_backoff_factor * (BACKOFF_FACTOR ** (self.consecutive_429_errors - 1))

        # Cap the maximum backoff time to 4 hours
        backoff_secs = min(backoff_secs, 14400)

        self.logger.warning(f"[bright_black][RateLimiter]🚦[/bright_black] Rate limit (429) for '{query_type}'. This indicates we're sending too many requests.")
        self.logger.warning(f"[bright_black][RateLimiter]🚦[/bright_black] Consecutive 429 errors: {self.consecutive_429_errors}. Backing off for {backoff_secs:.1f}s")
        self._sleep(backoff_secs)

    def handle_500(self, query_type: str):
        """Handle Server Error (HTTP 500) - Instagram server issue"""
        self.consecutive_500_errors += 1

        # For server errors, use a more gradual backoff
        backoff_secs = 5.0 * (BACKOFF_FACTOR ** (self.consecutive_500_errors - 1))
        backoff_secs = min(backoff_secs, 3600)  # Cap at 1 hour

        self.logger.warning(f"[bright_black][RateLimiter]🚦[/bright_black] Server error (500) for '{query_type}'. This is an Instagram server issue.")
        self.logger.warning(f"[bright_black][RateLimiter]🚦[/bright_black] Consecutive 500 errors: {self.consecutive_500_errors}. Backing off for {backoff_secs:.1f}s")
        self._sleep(backoff_secs)

    def handle_soft_block(self, query_type: str, message: str):
        """
        Handle soft blocks from Instagram (usually in 400 responses with specific messages).
        Implements exponential backoff for repeated blocks.
        """
        self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black] Soft block detected for '{query_type}': {message}")

        # For soft blocks, implement a very conservative backoff
        base_backoff = 1800  # 30 minutes
        factor = 1.5
        self.consecutive_403_errors += 1  # Use the 403 counter for soft blocks too

        backoff_secs = base_backoff * (factor ** (self.consecutive_403_errors - 1))
        # Cap at 8 hours max
        backoff_secs = min(backoff_secs, 28800)

        self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black] Soft block backoff: sleeping for {backoff_secs / 60:.1f} minutes")
        self._sleep(backoff_secs)

    def sleep(self, secs: float):
        """Called by Instaloader in some situations."""
        self.logger.warning(f"[bright_black][RateLimiter]🚦[/bright_black] Sleeping {secs:.2f}s by Instaloader's request")
        self._sleep(secs)

    def _sleep(self, secs: float):
        """Unified place for all sleeps with KeyboardInterrupt handling."""
        try:
            # Ensure minimum sleep time and round to 2 decimals for logging clarity
            secs = max(secs, 0.1)
            secs = round(secs, 2)

            if secs > 60:
                mins = secs / 60
                self.logger.warning(f"[bright_black][RateLimiter]🚦[/bright_black] Sleeping for {mins:.1f} minutes")

            time.sleep(secs)
        except KeyboardInterrupt:
            self.logger.info(f"[bright_black][RateLimiter]🚦[/bright_black] Sleep interrupted by user. Exiting...")
            raise

    def get_config(self):
        """Return current configuration settings"""
        return {
            "min_requests_per_hour": self.min_requests_per_hour,
            "max_requests_per_hour": self.max_requests_per_hour,
            "min_requests_per_min": self.min_requests_per_min,
            "max_requests_per_min": self.max_requests_per_min,
            "posts_before_wait": (self.posts_before_wait_min, self.posts_before_wait_max),
            "pause_between_posts": (self.pause_before_between_posts_min, self.pause_before_between_posts_max),
            "daily_limit": self.daily_request_limit,
            "timezone": str(self.timezone),
            "backoff_factor": BACKOFF_FACTOR
        }

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