# Media Management System

A comprehensive Python-based system for scanning, downloading, deduplicating, and organizing media files from various sources.

## Features

- **Media Scanning**: Recursively scan directories to catalog image and video files
- **Instagram Fetching**: Download media from Instagram profiles
- **AI-Powered Analysis**: Detect humans in images using YOLO object detection
- **Deduplication**: Identify and mark duplicate images using perceptual hashing
- **Intelligent Copying**: Copy files based on customizable criteria (human detection, unique files only)
- **Multi-Processing/Threading**: High-performance operations with parallel processing

## Components

### Main Modules

| Module | Description |
|--------|-------------|
| `scanner.py` | Scans directories and catalogs media files in database |
| `fetcher.py` | Downloads Instagram profile content |
| `deduper.py` | Identifies duplicate images using perceptual hashing |
| `copier.py` | Copies files from sources to destinations with filtering |

### Core Libraries

- **Database**: `DatabaseConnection` and `DatabaseManager` for SQL Server operations
- **Analysis**: 
  - `HashCalculator` for file and image hash computations
  - `YoloProvider` for human detection in images
  - `VideoFingerprinter` for video metadata extraction
- **Processing**:
  - `Scanner` for directory traversal and media processing
  - `InstagramFetcher` for downloading and cataloging Instagram content
  - `Deduper` for perceptual hash-based deduplication
  - `Copier` for intelligent file copying
- **Utilities**:
  - `CustomLogger` for rich, formatted logging
  - `RateController` for managing Instagram API rate limits

## Installation

### Prerequisites

- Python 3.8+
- SQL Server 2019+
- CUDA-compatible GPU (optional, for faster YOLO processing)

### Required Python Packages

```bash
pip install pyodbc instaloader ultralytics pillow torch rich
```

### Setting Up YOLO

Download the YOLOv8 model:

```bash
mkdir -p model
# For a standard model
wget -O model/yolov8x.pt https://github.com/ultralytics/assets/releases/download/v0.0.0/yolov8x.pt
```

## Usage

### Scanning Media Files

```bash
# Single-process mode
python scanner.py

# Multi-process mode with 4 workers
python scanner.py --workers 4

# Display pre-scan statistics only
python scanner.py --display-stats

# Reset table before scanning
python scanner.py --reset-table
```

### Downloading from Instagram

```bash
# Basic usage
python fetcher.py --login YOUR_INSTAGRAM_USERNAME --profile TARGET_PROFILE 

# Specify save directory
python fetcher.py --login YOUR_USERNAME --profile TARGET_PROFILE --save-to /path/to/save

# Re-login (clear session)
python fetcher.py --login YOUR_USERNAME --profile TARGET_PROFILE --relogin

# Reset table before downloading
python fetcher.py --login YOUR_USERNAME --profile TARGET_PROFILE --reset-table

# Reverse scan (process existing downloaded files)
python fetcher.py --reverse-scan /path/to/media/files
```

### Deduplicating Media

```bash
# Single-phase deduplication with pHash
python deduper.py --method single --phash-threshold 3

# Two-phase deduplication (dHash then pHash)
python deduper.py --method twophase --dhash-threshold 5 --phash-threshold 3

# Run both methods
python deduper.py --method both

# Specify source and target tables
python deduper.py --source-table my_source --target-table my_target --method single
```

### Copying Files

```bash
# Basic usage with default settings
python copier.py

# Only copy images with humans detected
python copier.py --human-only

# Use deduplication table (only copy representative images)
python copier.py --dedupe phash

# Preserve directory structure
python copier.py --directory-depth 2

# Set custom destinations
python copier.py --video-dest /path/to/videos --image-dest /path/to/images

# Use multiple threads
python copier.py --threads 8
```

## Database Schema

The system uses SQL Server with tables that track:

- File paths, names, and directories
- File types and sizes
- Multiple hash types (blake3, md5, sha256, dhash, phash, etc.)
- Human detection results
- Video metadata (resolution, length, fps)

Deduplication tables additionally include:
- `is_representative` flag indicating unique files
- Deduplication method information

## Configuration

Database connection settings are currently hardcoded in each script:
- Server: 172.16.8.31
- Port: 1433
- Database: media_db
- User: sa
- Password: Abcd!5678

Source and destination directories are defined in the respective scripts:
- Scanning sources in `scanner.py`
- Instagram download path in `fetcher.py`
- Copy destinations in `copier.py`

## Workflow Example

1. **Scan existing media**:
   ```bash
   python scanner.py --workers 4
   ```

2. **Download Instagram content**:
   ```bash
   python fetcher.py --login YOUR_USERNAME --profile TARGET_PROFILE
   ```

3. **Deduplicate the media**:
   ```bash
   python deduper.py --method both
   ```

4. **Copy unique files with humans to destinations**:
   ```bash
   python copier.py --dedupe phash --human-only
   ```

## Notes

- The system is designed to handle large media collections efficiently
- YOLO detection requires significant system resources; a GPU is recommended
- Instagram rate limiting is implemented to prevent account blocks
- Error handling is built in for graceful recovery from network/file issues

## Customization

The main configuration points are:
- Source directories in scanner.py
- Download directories in fetcher.py
- Destination directories in copier.py
- Database connection parameters in all scripts
- YOLO model path and parameters

## License

[Your License Information]