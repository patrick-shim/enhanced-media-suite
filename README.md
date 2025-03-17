# Enhanced Media Suite

A comprehensive media management toolkit for scanning, organizing, deduplicating, and merging large collections of images and videos.

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [System Architecture](#system-architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Scanner](#scanner)
  - [Instagram Fetcher](#instagram-fetcher)
  - [Deduplicator](#deduplicator)
  - [Media Copier](#media-copier)
  - [Media Merger](#media-merger)
- [Modules in Detail](#modules-in-detail)
  - [Core Modules](#core-modules)
  - [Utility Modules](#utility-modules)
- [Database Schema](#database-schema)
- [Advanced Usage](#advanced-usage)
  - [Multi-Process Scanning](#multi-process-scanning)
  - [Two-Phase Deduplication](#two-phase-deduplication)
  - [Custom File Prioritization](#custom-file-prioritization)
- [Performance Considerations](#performance-considerations)
- [Requirements](#requirements)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Overview

Enhanced Media Suite is a powerful set of Python tools designed to solve the complex challenges of managing large media collections. It provides sophisticated tools for media scanning, content fetching, deduplication, and organization, making it ideal for photographers, content creators, archivists, and anyone dealing with large image and video libraries.

This suite addresses several key pain points in media management:
- **Redundancy**: Identifies and eliminates duplicate content while preserving the best quality versions
- **Organization**: Intelligently structures media collections into logical hierarchies
- **Analysis**: Provides detailed metadata and content analysis (including human detection)
- **Automation**: Simplifies repetitive media management tasks through batch processing

## Features

### Media Processing
- **Multi-format Support**: Processes common image formats (JPEG, PNG, GIF, WEBP, HEIC, etc.) and video formats (MP4, MOV, AVI, MKV, etc.)
- **Metadata Extraction**: Extracts and stores comprehensive file metadata
- **Human Detection**: Uses YOLO machine learning to identify images containing people
- **Video Analysis**: Extracts video resolution, length, fps, and creates fingerprints for similarity detection

### Hash Calculation
- **Cryptographic Hashing**: Calculates MD5, SHA256, SHA512, and BLAKE3 hashes for exact file matching
- **Perceptual Hashing**: Implements dHash, pHash, wHash, cHash, and aHash for similarity detection
- **Video Fingerprinting**: Creates compact representations of video content for similarity detection

### Content Management
- **Deduplication**: Multiple strategies for detecting and handling duplicate media
- **Intelligent Merging**: Combines media collections while preserving high-quality content
- **Instagram Fetching**: Downloads media from Instagram profiles with rate-limiting and session management
- **Directory Organization**: Maintains or creates logical directory structures

### Performance
- **Multi-process Scanning**: Utilizes parallel processing for faster scanning of large collections
- **Incremental Processing**: Skips previously processed files for efficiency
- **Threaded Operations**: Implements multi-threading for improved performance
- **Graceful Interruption**: Handles keyboard interrupts to safely stop long-running operations

## System Architecture

Enhanced Media Suite follows a modular architecture with clear separation of concerns:

```
enhanced-media-suite/
├── copier.py             # File copying utility
├── deduper.py            # Media deduplication tool
├── fetcher.py            # Instagram content fetcher
├── merger.py             # Media collection merger
├── scanner.py            # Media scanning and database population
├── requirements.txt      # Python dependencies
├── source/
│   ├── copier_modules.py     # Core copying functionality
│   ├── database_modules.py   # Database operations and schema
│   ├── deduper_modules_db.py # Database-based deduplication
│   ├── deduper_modules_files.py # File-based deduplication
│   ├── fetcher_modules.py    # Instagram fetching logic
│   ├── fingerprint_modules.py # Video fingerprinting
│   ├── hash_modules.py       # Cryptographic and perceptual hashing
│   ├── logging_modules.py    # Logging utilities
│   ├── merger_modules.py     # Media merging functionality
│   ├── scanner_modules.py    # Media scanning logic
│   └── yolo_modules.py       # Human detection with YOLO
└── model/
    └── yolov8x.pt            # YOLO model for human detection
```

### Component Interactions

1. **Scanner** → Walks directories → Computes hashes → Detects humans → Stores in database
2. **Fetcher** → Downloads Instagram content → Processes files → Stores in database
3. **Deduper** → Reads database → Groups similar images → Marks representatives
4. **Copier/Merger** → Reads database → Copies/merges files → Organizes destinations

## Installation

### Prerequisites
- Python 3.8 or higher
- SQL Server database
- GPU with CUDA support (recommended for faster YOLO processing)
- 8GB+ RAM (16GB+ recommended for large collections)

### Steps

1. Clone the repository:
```bash
git clone https://github.com/yourusername/enhanced-media-suite.git
cd enhanced-media-suite
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Download the YOLO model:
```bash
mkdir -p model
# Download YOLOv8x.pt from Ultralytics and place in the model folder
# Or download automatically with:
python -c "from ultralytics import YOLO; YOLO('yolov8x.pt')"
mv yolov8x.pt model/
```

5. Verify installation:
```bash
python scanner.py --help
```

### Docker Installation (Alternative)

A Dockerfile is available for containerized deployment:

```bash
docker build -t enhanced-media-suite .
docker run -v /path/to/media:/media -v /path/to/config:/config enhanced-media-suite
```

## Configuration

### Database Connection

The suite requires a SQL Server database. Configure the connection in each script or use environment variables:

```python
db_credentials = {
    "db_server_ip": "172.16.8.31",   # Your database server IP
    "db_server_port": "1433",        # SQL Server port
    "db_name": "media_db",           # Database name 
    "db_user": "sa",                 # Database username
    "db_password": "your_password"   # Database password
}
```

### Default Paths

Default paths can be configured in each script:

```python
VIDEO_DESTINATION = "/mnt/nas3/TEST/Videos/"  # Default destination for videos
IMAGE_DESTINATION = "/mnt/nas3/TEST/Images/"  # Default destination for images
```

### YOLO Configuration

YOLO model settings can be adjusted:

```python
yolo_provider = YoloProvider(
    model_path="model/yolov8x.pt",  # Path to YOLO model
    iou=0.5,                        # Intersection over Union threshold
    conf=0.5,                       # Confidence threshold
    device="auto"                   # Device selection (auto, cpu, cuda)
)
```

## Usage

### Scanner

The scanner recursively walks directories, processes media files, and populates the database with metadata.

```bash
python scanner.py --workers 4 --display-stats --reset-table
```

#### Options
- `--workers N`: Number of worker processes for parallel scanning (0 for single process)
- `--display-stats`: Show pre-scan statistics only, without scanning
- `--reset-table`: Reset (drop and recreate) the database table before scanning
- `--no-stats`: Skip displaying pre-scan statistics

#### Example
```bash
# Scan with 8 worker processes and reset the database table
python scanner.py --workers 8 --reset-table

# Display statistics about media files in the source directories
python scanner.py --display-stats 
```

### Instagram Fetcher

The fetcher downloads media from Instagram profiles and processes them for the database.

```bash
python fetcher.py download --login YOUR_USERNAME --profile TARGET_PROFILE --limit 100
```

```bash
python fetcher.py scan --path /path/to/instagram/folder
```

#### Download Mode Options
- `--login`: Your Instagram login username (required)
- `--profile`: Target Instagram profile to download from (required)
- `--relogin`: Re-login to Instagram by deleting the session file
- `--save-to`: Directory to save downloaded media (default: profile name)
- `--resume`: Resume from last post (default: True)
- `--limit`: Limit the number of posts to download
- `--table-name`: Database table name
- `--skip-database`: Skip record insertion to database
- `--reset-table`: Reset the media table in database

#### Scan Mode Options
- `path`: Path to the directory to scan (required)
- `--no-reset-table`: Do not reset the table before scanning
- `--no-stats`: Do not display statistics before scanning
- `--table-name`: Database table name
- `--skip-database`: Skip record insertion to database

#### Example
```bash
# Download media from a profile with a limit of 50 posts
python fetcher.py download --login my_instagram_username --profile target_profile --limit 50

# Scan previously downloaded Instagram content
python fetcher.py scan --path /mnt/data/instagram_downloads/profile_name
```

### Deduplicator

The deduplicator finds and marks duplicate images based on perceptual hashing.

```bash
python deduper.py --method twophase --phash-threshold 3 --dhash-threshold 5
```

#### Options
- `--source-table`: Source table name (default: tbl_fetcher)
- `--target-table`: Optional target table name (default: auto-generated based on method)
- `--method`: Deduplication method (single, twophase, both)
- `--phash-threshold`: Hamming distance threshold for pHash (default: 3)
- `--dhash-threshold`: Hamming distance threshold for dHash (default: 5)

#### Examples
```bash
# Run single-phase deduplication using pHash
python deduper.py --method single --phash-threshold 3

# Run two-phase deduplication (dHash then pHash)
python deduper.py --method twophase --dhash-threshold 5 --phash-threshold 3

# Run both methods and create two result tables
python deduper.py --method both
```

### Media Copier

The copier transfers files from the database to destination directories based on various criteria.

```bash
python copier.py --directory-depth 2 --threads 8 --human-only --dedupe phash
```

#### Options
- `--directory-depth`: Directory depth to preserve when copying (0 = flat structure)
- `--threads`: Number of threads for parallel copying
- `--human-only`: Only copy images where YOLO detects a person
- `--dedupe`: Which deduplication source table to use (none, dhash, phash, twophase)

#### Examples
```bash
# Copy all files to destination, keeping the original directory structure
python copier.py --directory-depth 2

# Copy only images with people, using 8 threads
python copier.py --human-only --threads 8

# Copy representative images from deduplication results
python copier.py --dedupe phash --directory-depth 0
```

### Media Merger

The merger combines media from multiple sources into organized destination folders.

```bash
python merger.py --sources /path/source1 /path/source2 --threads 8 --human-only
```

#### Options
- `--sources`: One or more source directories to merge from (required)
- `--image-dest`: Destination directory for image files
- `--video-dest`: Destination directory for video files
- `--threads`: Number of threads for parallel merging
- `--human-only`: Only copy images that contain humans
- `--yolo-model`: Path to YOLO model file

#### Examples
```bash
# Merge media from multiple sources with 8 threads
python merger.py --sources /path/source1 /path/source2 /path/source3 --threads 8

# Merge only media containing people into custom destinations
python merger.py --sources /path/source1 --human-only --image-dest /custom/images --video-dest /custom/videos
```

## Modules in Detail

### Core Modules

#### Database Modules
- `DatabaseConnection`: Manages database connections with SQL Server
- `DatabaseManager`: Provides database operations (table creation, queries, inserts)

```python
# Example of database interaction
db_connection = DatabaseConnection(
    db_server_ip="172.16.8.31",
    db_server_port="1433",
    db_name="media_db", 
    db_user="sa",
    db_password="password"
)
db_manager = DatabaseManager()

# Create a table
db_manager.create_table(db_connection.connection, "my_table")

# Insert a record
db_manager.insert(
    connection=db_connection.connection,
    table_name="my_table",
    file_path="/path/to/file.jpg",
    file_type="image",
    # ... other fields
)
```

#### Hash Modules
- `HashCalculator`: Calculates various cryptographic and perceptual hashes
- `FileHashes`: Dataclass for storing file-level hashes
- `ImageHashes`: Dataclass for storing image-level perceptual hashes

```python
# Example of hash calculation
hash_calculator = HashCalculator()

# Calculate file hashes
file_hashes = hash_calculator.calculate_file_hash("/path/to/file.jpg")
print(f"MD5: {file_hashes.md5}")
print(f"SHA256: {file_hashes.sha256}")
print(f"BLAKE3: {file_hashes.blake3}")

# Calculate image hashes
image_hashes = hash_calculator.calculate_image_hash("/path/to/image.jpg")
print(f"dHash: {image_hashes.dhash}")
print(f"pHash: {image_hashes.phash}")
```

#### YOLO Modules
- `YoloProvider`: Handles human detection using the YOLOv8 model
- `YoloResult`: Dataclass for storing detection results

```python
# Example of human detection
yolo_provider = YoloProvider("model/yolov8x.pt", conf=0.5, iou=0.5)
result = yolo_provider.has_human("/path/to/image.jpg")

if result.has_human:
    print(f"Image contains {result.human_count} humans with confidence {result.confidence}")
```

#### Video Fingerprinting
- `VideoFingerprinter`: Creates compact fingerprints for video comparison
- `VideoFileInfo`: Dataclass for storing video metadata and fingerprint

```python
# Example of video fingerprinting
fingerprinter = VideoFingerprinter()
video_info = fingerprinter.extract_fingerprint("/path/to/video.mp4")

print(f"Resolution: {video_info.resolution}")
print(f"FPS: {video_info.fps}")
print(f"Length: {video_info.length} seconds")
print(f"Fingerprint: {video_info.hex[:32]}...")  # Truncated fingerprint
```

### Utility Modules

#### Logging Module
- `CustomLogger`: Provides consistent logging across all modules with rich console output

```python
# Example of logging
from source.logging_modules import CustomLogger

logger = CustomLogger(__name__).get_logger()
logger.info("This is an information message")
logger.warning("This is a warning message")
logger.error("This is an error message")
```

#### Rate Controller for Instagram
- `RateController`: Manages request rates to avoid Instagram rate limiting

```python
# The rate controller is used internally by the Instagram fetcher
# It implements sophisticated delay strategies:
# - Enforces hourly and daily limits
# - Adds random jitter to seem more human-like
# - Takes longer pauses after several requests
# - Implements exponential backoff for errors
```

## Database Schema

The database schema is centered around media metadata storage:

### Main Media Table

```sql
CREATE TABLE [dbo].[tbl_fetcher] (
    [file_path]         NVARCHAR(1024) NOT NULL,
    [file_name]         NVARCHAR(256)  NOT NULL,
    [file_directory]    NVARCHAR(128)  NOT NULL,
    [file_type]         NVARCHAR(64)   NOT NULL,
    [file_extension]    NVARCHAR(8)    NOT NULL,
    [file_size]         BIGINT         NOT NULL,
    [md5]               CHAR(64)       NULL,
    [sha256]            CHAR(256)      NULL,
    [sha512]            CHAR(512)      NULL,
    [blake3]            CHAR(512)      NOT NULL,
    [dhash]             CHAR(256)      NULL,
    [phash]             CHAR(256)      NULL,
    [whash]             CHAR(256)      NULL,
    [chash]             CHAR(256)      NULL,
    [ahash]             CHAR(256)      NULL,
    [video_fingerprint] CHAR(512)      NULL,
    [video_width]       INT            NULL,
    [video_height]      INT            NULL,
    [video_resolution]  VARCHAR(16)    NULL,
    [video_fps]         INT            NULL,
    [video_length]      FLOAT          NULL,
    [has_human]         BIT            NOT NULL DEFAULT 0,
    [has_human_score]   DECIMAL(5, 2)  NULL,
    [has_human_count]   INT            NULL,
    [date]              DATETIME       NOT NULL DEFAULT GETDATE()
);
```

### Deduplication Tables

Deduplication creates tables with additional fields:

```sql
-- Additional fields in deduplication tables
[is_representative] BIT            NOT NULL DEFAULT 0,
[dedupe_method]     NVARCHAR(64)   NULL,
[dedupe_phase1]     NVARCHAR(64)   NULL,
[dedupe_phase2]     NVARCHAR(64)   NULL
```

### Indexes

The following indexes are created for performance:

```sql
-- Unique constraints
ALTER TABLE [dbo].[table_name] 
ADD CONSTRAINT [UQ_table_name_blake3] UNIQUE NONCLUSTERED ([blake3] ASC);

ALTER TABLE [dbo].[table_name] 
ADD CONSTRAINT [UQ_table_name_file_path] UNIQUE NONCLUSTERED ([file_path] ASC);

-- Non-clustered indexes for performance
CREATE NONCLUSTERED INDEX [IX_table_name_dhash]
ON [dbo].[table_name] ([dhash] ASC);

CREATE NONCLUSTERED INDEX [IX_table_name_phash]
ON [dbo].[table_name] ([phash] ASC);

-- Additional indexes for deduplication tables
CREATE NONCLUSTERED INDEX [IX_table_name_is_representative]
ON [dbo].[table_name] ([is_representative] ASC);

CREATE NONCLUSTERED INDEX [IX_table_name_file_directory]
ON [dbo].[table_name] ([file_directory] ASC);
```

## Advanced Usage

### Multi-Process Scanning

The scanner supports multi-process operation for faster processing of large collections:

```bash
python scanner.py --workers 8
```

This divides the source directories into groups and processes each group in a separate process. For optimal performance:

- Set `--workers` to match your CPU core count
- Consider memory usage (each worker needs memory for YOLO)
- Monitor CPU, memory, and disk I/O during scanning

### Two-Phase Deduplication

Two-phase deduplication provides a balance between speed and accuracy:

1. **Phase 1 (dHash)**: Fast but less precise clustering
2. **Phase 2 (pHash)**: More accurate refinement of initial clusters

This approach is particularly effective for large collections:

```bash
python deduper.py --method twophase --dhash-threshold 10 --phash-threshold 3
```

Adjust thresholds based on your needs:
- Higher threshold = more aggressive deduplication (more false positives)
- Lower threshold = more conservative (might miss some duplicates)

### Custom File Prioritization

When merging or copying files, the system uses smart prioritization to determine which version to keep when duplicates are found:

```python
def _get_file_priority(filename: str) -> int:
    """
    Determine the priority of a file based on filename pattern.
    Lower number => higher priority.
    """
    # Highest priority: Instaloader exact pattern with timestamp and shortcode
    if re.match(r'\d{8}_\d{6}_\w+\.\w+$', filename):
        return 1

    # Instagram-like format
    if re.match(r'\d{8}_\d{6}_\w+', filename):
        return 2

    # Format like "..._20231009_154612_something_2.jpg"
    if re.match(r'.*\d{8}_\d{6}_\w+_\d+\.\w+$', filename):
        return 3

    # Files that don't have (1).jpg at the end
    if not re.search(r'\(\d+\)\.\w+$', filename):
        return 4

    # Everything else
    return 5
```

You can customize this function to match your specific naming conventions.

## Performance Considerations

### Memory Usage

- YOLO model requires significant RAM (4-8 GB depending on model size)
- Perceptual hash calculations can be memory-intensive for large images
- Database operations buffer results in memory

### Disk I/O

- Reading large media files can be I/O bound
- Consider using SSDs for database and intermediate storage
- Network attached storage may become a bottleneck

### CPU Usage

- Hash calculations are CPU-intensive
- Video fingerprinting requires significant CPU power
- Database operations can be CPU-bound for large transactions

### GPU Acceleration

- YOLO detection is significantly faster with GPU
- The suite auto-detects CUDA availability
- Feature extraction can use GPU when available

## Requirements

### Software Requirements
- Python 3.8 or higher
- SQL Server database
- Required Python packages (see requirements.txt)

### Hardware Recommendations
- **Minimum**: 4-core CPU, 8GB RAM, SSD storage
- **Recommended**: 8+ core CPU, 16GB+ RAM, NVMe SSD, NVIDIA GPU with 6GB+ VRAM
- **For Large Collections**: 16+ core CPU, 32GB+ RAM, NVMe RAID, NVIDIA RTX series GPU

### Network
- High-speed internet connection for Instagram fetching
- Fast local network for accessing network storage

## Troubleshooting

### Common Issues

#### Database Connection Errors
```
Error connecting to database: Login failed for user 'sa'
```
- Verify database credentials
- Check SQL Server is running and accessible
- Ensure firewall allows connections on the database port

#### YOLO Model Issues
```
Error loading YOLO model: No such file or directory
```
- Ensure the model file exists at the specified path
- Download the correct model version (yolov8x.pt)
- Check GPU drivers are properly installed for GPU acceleration

#### Permission Errors
```
Permission denied when accessing file: /path/to/file.jpg
```
- Check file and directory permissions
- Ensure the user running the script has appropriate access

#### Memory Issues
```
CUDA out of memory
```
- Reduce batch size for YOLO processing
- Use a smaller YOLO model (yolov8m.pt or yolov8s.pt)
- Increase available GPU memory or switch to CPU mode

### Logging

The suite uses a comprehensive logging system with both console and file output:

- Console logs use rich formatting for better readability
- File logs are stored in the `logs/` directory with timestamps
- Log levels can be adjusted in the `CustomLogger` class

To increase logging verbosity:
```python
logger = CustomLogger(__name__, level=logging.DEBUG).get_logger()
```

## Contributing

Contributions to Enhanced Media Suite are welcome! Here's how to contribute:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

### Coding Standards
- Follow PEP 8 style guidelines
- Document all functions and classes with docstrings
- Add type hints for better code readability
- Write unit tests for new functionality

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

© 2025 Enhanced Media Suite
