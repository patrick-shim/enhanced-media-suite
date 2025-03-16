#!/usr/bin/env python3

import os
import argparse
import math

def scan_directories(base_directory):
    """
    Scan the base directory and return a list of all subdirectories.
    
    Args:
        base_directory (str): The root directory to start scanning from
    
    Returns:
        list: A list of full paths to all subdirectories
    """
    # Ensure the base directory exists
    if not os.path.isdir(base_directory):
        print(f"Error: {base_directory} is not a valid directory.")
        return []

    # List to store all discovered directories
    directories = []

    # Walk through the directory tree
    for root, dirs, _ in os.walk(base_directory):
        # Add each directory's full path to the list
        for dir_name in dirs:
            full_path = os.path.join(root, dir_name)
            directories.append(full_path)

    return sorted(directories)

def group_directories(directories, workers):
    """
    Group directories into workers number of groups, 
    attempting to balance the number of directories in each group.
    
    Args:
        directories (list): List of directory paths
        workers (int): Number of worker processes
    
    Returns:
        list of lists: Grouped directories
    """
    # If no workers specified or 0, return all directories in one group
    if workers <= 0:
        return [directories]

    # Calculate approximate group size
    total_dirs = len(directories)
    group_size = math.ceil(total_dirs / workers)

    # Group directories
    grouped_directories = []
    for i in range(0, total_dirs, group_size):
        grouped_directories.append(directories[i:i+group_size])

    # Ensure we have exactly 'workers' number of groups
    # If we have fewer groups, pad with empty lists
    while len(grouped_directories) < workers:
        grouped_directories.append([])

    return grouped_directories

def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Scan and group directories within a base directory.")
    parser.add_argument("--base-directory", 
                        required=True, 
                        help="Base directory to start scanning from")
    parser.add_argument("--workers", 
                        type=int, 
                        default=4, 
                        help="Number of worker processes (default: 4)")
    parser.add_argument("--output", 
                        choices=['print', 'file'], 
                        default='print', 
                        help="Output method (print to console or write to file)")
    parser.add_argument("--output-file", 
                        default="directory_groups.py", 
                        help="File to write directory groups to (if --output=file)")

    # Parse arguments
    args = parser.parse_args()

    # Scan directories
    directories = scan_directories(args.base_directory)

    # Group directories
    grouped_directories = group_directories(directories, args.workers)

    # Output handling
    if args.output == 'print':
        # Print grouped directories to console
        print("Grouped Directories:")
        print("directories = [")
        for group in grouped_directories:
            print("    [")
            for directory in group:
                print(f"        \"{directory}\",")
            print("    ],")
        print("]")
    else:
        # Write grouped directories to file in a Python-compatible format
        with open(args.output_file, 'w') as f:
            f.write("# Automatically generated list of directory groups\n")
            f.write("directories = [\n")
            for group in grouped_directories:
                f.write("    [\n")
                for directory in group:
                    f.write(f"        \"{directory}\",\n")
                f.write("    ],\n")
            f.write("]\n")
        print(f"Directory groups written to {args.output_file}")

    # Print summary
    print(f"\nTotal directories found: {len(directories)}")
    print(f"Number of groups: {len(grouped_directories)}")
    for i, group in enumerate(grouped_directories, 1):
        print(f"Group {i} size: {len(group)} directories")

if __name__ == "__main__":
    main()