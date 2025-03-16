# Enhanced Media Suite - Product Requirements Document

## 1. Executive Summary

Enhanced Media Suite is a comprehensive media management solution designed to solve the challenges of organizing, deduplicating, and managing large collections of images and videos. It provides tools for scanning media files, fetching content from social media, finding duplicates using advanced perceptual hashing techniques, and intelligently merging collections.

## 2. Product Vision

To provide media professionals and collectors with a powerful toolset for efficient organization and management of large media libraries, eliminating redundancy while preserving the highest quality versions of each asset.

## 3. Target Users

- Media archivists and collectors
- Content creators managing large media libraries
- Digital asset managers
- Social media content analysts

## 4. User Stories

### As a media archivist:
- I want to scan directories for all media files and extract metadata so I can catalog my collection
- I want to detect duplicate files across my collection so I can save storage space
- I want to merge multiple collections while keeping the highest quality versions of each file
- I want to quickly find images containing people for better organization

### As a social media analyst:
- I want to download media from Instagram profiles efficiently while respecting rate limits
- I want to scan downloaded content for processing and analysis
- I want to identify duplicate content across different sources

## 5. System Architecture

### 5.1 Core Components

#### Scanner Module
- Recursively scans directories for media files
- Calculates cryptographic hashes (MD5, SHA256, SHA512, BLAKE3)
- Calculates perceptual hashes (dHash, pHash, wHash, cHash, aHash)
- Performs human detection using YOLO
- Extracts video fingerprints and metadata
- Stores all data in a SQL database

#### Fetcher Module
- Downloads content from Instagram profiles
- Implements proper rate limiting to avoid blocking
- Processes downloaded files for database storage

#### Deduplicator Module
- Uses perceptual hashing to identify similar images
- Implements single-phase or two-phase deduplication strategies
- Creates representative selection from duplicate groups
- Preserves directory-based organization

#### Merger Module
- Merges media from multiple sources into organized destinations
- Separates images and videos into appropriate folders
- Uses smart file prioritization to keep best versions
- Preserves source directory structure as needed

### 5.2 Database Schema

The system uses a SQL database with the following key tables:

- Main media table (tbl_fetcher by default)
  - File information (path, name, directory, extension, type, size)
  - Hash values (MD5, SHA256, SHA512, BLAKE3)
  - Perceptual hashes (dHash, pHash, wHash, cHash, aHash)
  - Video metadata (width, height, fps, length, resolution, fingerprint)
  - Human detection (has_human flag, score, count)
  - Date added

- Deduplication tables (dynamically created)
  - Same schema as main table plus is_representative flag

## 6. Functional Requirements

### 6.1 Scanner

- **Multi-format Support**: Process common image formats (JPEG, PNG, GIF, WEBP, etc.) and video formats (MP4, MOV, AVI, etc.)
- **Multi-process Operation**: Utilize parallel processing for scanning large collections
- **File Prioritization**: Use smart prioritization based on filename patterns
- **Incremental Scanning**: Skip already processed files using BLAKE3 hash check
- **Statistics**: Generate comprehensive statistics about scanned collections

### 6.2 Instagram Fetcher

- **Profile Downloading**: Download media from Instagram profiles
- **Rate Limiting**: Implement sophisticated rate control to avoid blocking
- **Session Management**: Handle login sessions and authenticate as needed
- **Metadata Extraction**: Extract relevant metadata from downloaded content
- **Reverse Scanning**: Scan previously downloaded content for processing

### 6.3 Deduplicator

- **Multiple Hash Methods**: Support different perceptual hash algorithms for deduplication
- **Two-phase Deduplication**: Implement coarse-to-fine deduplication for better results
- **Directory-based Grouping**: Process duplicates within directory context
- **Representative Selection**: Mark representative images from duplicate groups
- **Result Analytics**: Generate statistics and reports on deduplication results

### 6.4 Merger

- **Smart Merging**: Merge files from multiple sources while handling conflicts
- **Content-based Verification**: Use hash verification to identify identical content
- **Human Detection Filtering**: Option to only copy images containing humans
- **Directory Structure Preservation**: Maintain source directory structure if desired
- **Type Separation**: Automatically separate images and videos into different destinations

## 7. Non-functional Requirements

### 7.1 Performance

- Support multi-process scanning for large collections
- Implement efficient database operations with proper indexing
- Optimize memory usage for large media collections
- Provide progress reporting for long-running operations

### 7.2 Reliability

- Implement robust error handling and logging
- Support graceful termination via keyboard interrupts
- Provide transaction safety for database operations

### 7.3 Security

- Secure storage of Instagram credentials
- Safe handling of database connection parameters

### 7.4 Scalability

- Design for handling millions of media files
- Support distributed operation across multiple machines

## 8. Technical Constraints

- Python 3.8+ runtime environment
- SQL Server database for metadata storage
- YOLO model for human detection (requires sufficient GPU or CPU resources)
- Sufficient disk space for media collections
- Network access for Instagram content fetching

## 9. Command-line Interfaces

### 9.1 Scanner CLI

