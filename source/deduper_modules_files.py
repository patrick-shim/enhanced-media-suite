# deduplication_modules.py
import pywt
import numpy as np
from PIL import Image
from collections import deque

class Deduper:
    """
    Core class providing:
      - Various perceptual hashing (aHash, dHash, pHash, wHash)
      - BFS-based clustering to group near-duplicates
    """

    def __init__(self):
        pass

    @staticmethod
    def hamming_distance(hash1: int, hash2: int) -> int:
        return bin(hash1 ^ hash2).count('1')

    @staticmethod
    def average_hash(image_path: str, hash_size: int = 8) -> int:
        with Image.open(image_path) as img:
            img = img.convert('L').resize((hash_size, hash_size), Image.ANTIALIAS)
            pixels = list(img.getdata())
        avg = sum(pixels) / len(pixels)
        bits = [1 if px >= avg else 0 for px in pixels]
        bit_string = ''.join(str(b) for b in bits)
        return int(bit_string, 2)

    @staticmethod
    def dhash(image_path: str, hash_size: int = 8) -> int:
        with Image.open(image_path) as img:
            img = img.convert('L').resize((hash_size + 1, hash_size), Image.ANTIALIAS)
            pixels = np.array(img, dtype=np.uint8)
        diff = pixels[:, 1:] > pixels[:, :-1]
        bits = diff.flatten().astype(int)
        bit_string = ''.join(str(b) for b in bits)
        return int(bit_string, 2)

    @staticmethod
    def phash(image_path: str, hash_size: int = 8, highfreq_factor: int = 4) -> int:
        img_size = hash_size * highfreq_factor
        with Image.open(image_path) as img:
            img = img.convert('L').resize((img_size, img_size), Image.ANTIALIAS)
            pixels = np.array(img, dtype=np.float32)

        dct = dct_2d_numpy(pixels)
        dct_low_freq = dct[:hash_size, :hash_size]
        dct_mean = (np.sum(dct_low_freq) - dct_low_freq[0, 0]) / (hash_size*hash_size - 1)

        diff = dct_low_freq > dct_mean
        bits = diff.flatten().astype(int)
        bit_string = ''.join(str(b) for b in bits)
        return int(bit_string, 2)

    @staticmethod
    def wHash(image_path: str, hash_size: int = 8, mode: str = 'haar') -> int:
        if pywt is None:
            raise ImportError("PyWavelets is not installed; cannot compute wHash.")

        with Image.open(image_path) as img:
            img = img.convert('L').resize((hash_size, hash_size), Image.ANTIALIAS)
            pixels = np.array(img, dtype=np.float32)

        LL, (LH, HL, HH) = pywt.dwt2(pixels, mode)
        sub_band = LL.flatten()
        mean_val = np.mean(sub_band)
        bits = [1 if px > mean_val else 0 for px in sub_band]
        bit_string = ''.join(str(b) for b in bits)
        return int(bit_string, 2)

    def compute_hash(self, image_path: str, method: str) -> int:
        m = method.lower()
        if m == 'ahash':
            return self.average_hash(image_path)
        elif m == 'dhash':
            return self.dhash(image_path)
        elif m == 'phash':
            return self.phash(image_path)
        elif m == 'whash':
            return self.wHash(image_path)
        else:
            raise ValueError(f"Unknown hash method: {method}")

    def cluster_images(self, image_paths, method='phash', distance_threshold=10):
        # 1) Compute all hashes
        hash_map = {}
        for p in image_paths:
            hash_map[p] = self.compute_hash(p, method)

        # 2) Build adjacency
        adj = {p: [] for p in image_paths}
        n = len(image_paths)
        for i in range(n):
            for j in range(i+1, n):
                p1 = image_paths[i]
                p2 = image_paths[j]
                dist = self.hamming_distance(hash_map[p1], hash_map[p2])
                if dist <= distance_threshold:
                    adj[p1].append(p2)
                    adj[p2].append(p1)

        # 3) BFS
        visited = set()
        clusters = []
        for p in image_paths:
            if p not in visited:
                c = []
                queue = deque([p])
                visited.add(p)
                while queue:
                    curr = queue.popleft()
                    c.append(curr)
                    for nei in adj[curr]:
                        if nei not in visited:
                            visited.add(nei)
                            queue.append(nei)
                clusters.append(c)
        return clusters


def dct_2d_numpy(matrix: np.ndarray) -> np.ndarray:
    M = dct_1d_numpy(matrix, axis=0)
    M = dct_1d_numpy(M, axis=1)
    return M

def dct_1d_numpy(x: np.ndarray, axis: int = -1) -> np.ndarray:
    N = x.shape[axis]
    alpha0 = np.sqrt(1.0 / N)
    alphaK = np.sqrt(2.0 / N)

    x = np.swapaxes(x, axis, -1)
    out = np.zeros_like(x, dtype=np.float32)
    n = np.arange(N)

    for k in range(N):
        if k == 0:
            out[..., k] = alpha0 * np.sum(
                x * np.cos(np.pi * (2*n + 1) * k / (2.0*N)),
                axis=-1
            )
        else:
            out[..., k] = alphaK * np.sum(
                x * np.cos(np.pi * (2*n + 1) * k / (2.0*N)),
                axis=-1
            )

    out = np.swapaxes(out, axis, -1)
    return out