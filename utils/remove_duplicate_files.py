#!/usr/bin/env python3

import os
import sys
import re
import shutil
import blake3

def compute_blake3(file_path: str) -> str:
    """
    Compute and return the BLAKE3 hash hex digest for the given file.
    """
    hasher = blake3.blake3()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            hasher.update(chunk)
    return hasher.hexdigest()

def find_and_move_duplicates_single_folder(base_directory: str):
    """
    1) Recursively walk `base_directory` for files matching "<base> <n>.<ext>".
    2) Check if "<base>.<ext>" exists in the same folder.
    3) Compare BLAKE3 hashes. If identical, move the numbered file into a single
       'dupes' folder under `base_directory`.
    
    Returns:
       A list of tuples (duplicate_file_path, original_file_path, moved_destination).
    """
    # Regex: captures everything until the last space, then a number, then a dot+extension
    pattern = re.compile(r'^(.*)\s(\d+)\.(\w+)$')
    moved_duplicates = []

    # Create one common dupes folder in the base directory
    dupes_dir = os.path.join(base_directory, "dupes")
    os.makedirs(dupes_dir, exist_ok=True)

    for root, dirs, files in os.walk(base_directory):
        # Skip the dupes folder itself to avoid re-processing duplicates
        if os.path.abspath(root) == os.path.abspath(dupes_dir):
            continue

        for filename in files:
            # Skip if we are already inside the dupes folder
            if os.path.abspath(root) == os.path.abspath(dupes_dir):
                continue

            match = pattern.match(filename)
            if match:
                base_no_suffix = match.group(1)  # e.g. "holiday"
                extension = match.group(3)       # e.g. "jpg"

                original_name = f"{base_no_suffix}.{extension}"
                original_path = os.path.join(root, original_name)
                numbered_path = os.path.join(root, filename)

                # If an "original" file exists, compare BLAKE3
                if os.path.isfile(original_path):
                    hash_original = compute_blake3(original_path)
                    hash_numbered = compute_blake3(numbered_path)
                    if hash_original == hash_numbered:
                        # Build the path for the duplicate in the single dupes folder
                        destination_path = os.path.join(dupes_dir, filename)

                        # If there's already a file with the same name in dupes, handle collision
                        if os.path.exists(destination_path):
                            # For demonstration, append a numeric suffix to avoid overwrite
                            base_filename, ext = os.path.splitext(filename)
                            counter = 1
                            while True:
                                new_filename = f"{base_filename}_{counter}{ext}"
                                new_destination = os.path.join(dupes_dir, new_filename)
                                if not os.path.exists(new_destination):
                                    destination_path = new_destination
                                    break
                                counter += 1

                        shutil.move(numbered_path, destination_path)
                        moved_duplicates.append((numbered_path, original_path, destination_path))

    return moved_duplicates

def main():
    if len(sys.argv) < 2:
        print(f"Usage: python {os.path.basename(sys.argv[0])} <directory>")
        sys.exit(1)

    base_directory = sys.argv[1]
    if not os.path.isdir(base_directory):
        print(f"Error: '{base_directory}' is not a directory.")
        sys.exit(1)

    duplicates = find_and_move_duplicates_single_folder(base_directory)
    if duplicates:
        print("Moved duplicates to a single 'dupes' folder:")
        for dup_src, orig, dup_dst in duplicates:
            print(f" - Original:   {orig}")
            print(f"   Duplicate:  {dup_src}")
            print(f"   Moved to:   {dup_dst}\n")
    else:
        print("No duplicates found with OS-like numbering patterns.")

if __name__ == "__main__":
    main()