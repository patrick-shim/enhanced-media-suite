import os
import hashlib
from PIL import Image
import imagehash
from source.logging_modules import CustomLogger
from dataclasses import dataclass

logger = CustomLogger(__name__).get_logger()

@dataclass
class FileHashes:
    md5: str
    sha256: str
    sha512: str
    blake3: str

@dataclass
class ImageHashes:
    dhash: str
    phash: str
    whash: str
    chash: str
    ahash: str

class HashCalculator:
    """
    A class to calculate various hashes for files and images.
    """

    def __init__(self):
        pass

    def calculate_all_hashes(self, filepath: str) -> tuple[FileHashes, ImageHashes]:
        """
        Calculate both file and image hashes for the given file path.

        Args:
            filepath (str): Path to the file.

        Returns:
            tuple: A tuple containing FileHashes and ImageHashes.
        """
        logger.info(f"Calculating hashes for {filepath}")
        file_hash = self.calculate_file_hash(filepath)
        image_hash = self.calculate_image_hash(filepath)
        return file_hash, image_hash

    def calculate_file_hash(self, filepath: str) -> FileHashes:
        """
        Calculate various hashes for a given file.

        Args:
            filepath (str): Path to the file to be hashed.

        Returns:
            FileHashes: An instance containing the MD5, SHA256, SHA512, and BLAKE3 hashes of the file.
        """
        with open(filepath, "rb") as f:
            data = f.read()
        md5 = hashlib.md5(data).hexdigest()
        sha256 = hashlib.sha256(data).hexdigest()
        sha512 = hashlib.sha512(data).hexdigest()
        blake3 = hashlib.blake2b(data).hexdigest()
        return FileHashes(md5, sha256, sha512, blake3)

    def calculate_image_hash(self, filepath: str, hash_size: int = 32) -> ImageHashes:
        """
        Calculate various perceptual hashes for an image.

        Args:
            filepath (str): Path to the image file.

        Returns:
            ImageHashes: An instance containing different perceptual hashes of the image.

        Raises:
            ValueError: If the file is not a valid image.
        """
        valid_extensions = ['.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp', '.tiff', '.heic', '.heif', '.raw']
        
        if not os.path.exists(filepath):
            raise ValueError(f"File {filepath} does not exist")
        
        if not any(filepath.lower().endswith(ext) for ext in valid_extensions):
            raise ValueError(f"File {filepath} is not an image")

        try:
            image = Image.open(filepath)
            image.verify()
            # Now reopen or copy the image for actual processing
            image = Image.open(filepath)
            image.load()  # optional: ensures full decode
        except Exception as e:
            raise Exception(f"Error opening image from {filepath}: {e}")

        dhash = str(imagehash.dhash(image, hash_size))
        phash = str(imagehash.phash(image, hash_size))
        whash = str(imagehash.whash(image, hash_size))
        chash = str(imagehash.colorhash(image, hash_size))
        ahash = str(imagehash.average_hash(image, hash_size))
        return ImageHashes(dhash, phash, whash, chash, ahash)