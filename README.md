# Enhanced Media Suite

A comprehensive media management toolkit for scanning, organizing, deduplicating, and merging large collections of images and videos.

## Overview

Enhanced Media Suite is a set of Python tools designed to help manage large media collections. It provides several key functionalities:

- **Media Scanning**: Scans directories recursively for images and videos, extracts metadata, calculates multiple hash types, and stores information in a SQL database.
- **Instagram Content Fetching**: Downloads media from Instagram profiles while respecting rate limits.
- **Media Deduplication**: Identifies and marks duplicate media files using perceptual hashing algorithms.
- **Media Merging**: Combines media from multiple source directories into organized destination folders.

## Key Features

- Multi-hash calculation (MD5, SHA256, SHA512, BLAKE3)
- Perceptual image hashing (dHash, pHash, wHash, cHash, aHash) for similarity detection
- Human detection using YOLOv8
- Video fingerprinting and metadata extraction
- Multi-process scanning for performance
- Content-based deduplication
- Smart file prioritization for merging

## Requirements

- Python 3.8+
- SQL Server database
- Dependencies listed in `requirements.txt`

## Installation

1. Clone the repository:
```bash
git clone https://github.com/username/enhanced-media-suite.git
cd enhanced-media-suite
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Download the YOLOv8 model:
```bash
mkdir -p model
# Download YOLOv8x.pt from Ultralytics and place in the model folder
```

5. Configure database connection parameters in the scripts or using command-line arguments.

## Usage

### Media Scanner

```bash
python scanner.py [--workers N] [--reset-table] [--display-stats-only] [--no-stats]
```

- `--workers N`: Number of parallel processes (0 for single process)
- `--reset-table`: Clear existing database table before scanning
- `--display-stats-only`: Only show statistics without scanning
- `--no-stats`: Skip displaying statistics

### Instagram Fetcher

```bash
python fetcher.py download --profile USERNAME [--limit N] [--login LOGIN] [--password PASS]
```

```bash
python fetcher.py scan --path /path/to/directory [--table-name TABLE] [--no-reset-table] [--no-stats]
```

### Deduplicator

```bash
python deduper.py [--method {dhash,phash,twophase,both}] [--source-table TABLE] [--dhash-threshold N] [--phash-threshold N]
```

### Media Merger

```bash
python merger.py --sources /path/to/source1 /path/to/source2 [--image-dest /path/to/images] [--video-dest /path/to/videos] [--threads N] [--human-only] [--yolo-model /path/to/model]
```

## Architecture

The suite follows a modular architecture with these key components:

- Source modules in `source/` directory contain the core functionality
- Executable scripts in the root directory provide command-line interfaces
- Support utilities in the `utils/` directory

## Database Schema

The system stores media metadata in SQL tables with the following key fields:
- File information (path, name, size, type)
- Hash values (MD5, SHA256, SHA512, BLAKE3)
- Perceptual hashes (dHash, pHash, wHash, cHash, aHash)
- Video metadata (resolution, length, fps)
- Human detection results

## License

[Your license here]

## Contributing

Contributions are welcome. Please feel free to submit a Pull Request.
