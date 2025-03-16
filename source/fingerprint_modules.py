import os
import torch
import numpy as np
import hashlib
import cv2
from dataclasses import dataclass
from source.logging_modules import CustomLogger

@dataclass
class VideoFileInfo:
    """Holds the computed fingerprint and key video metadata."""
    hex: str
    fps: float
    width: int
    height: int
    resolution: str
    frame_count: int
    length: float  # Store length in seconds as a float


class VideoFingerprinter:
    """
    Extracts a simplified 'fingerprint' from video frames and returns
    metadata such as fps, resolution, total frame count, and computed length (in seconds).
    """

    def __init__(
        self,
        fingerprint_length=1024,
        use_gpu=True,
        frame_grid_size=16,
        scale_factor=8,
        frames_to_sample=None
    ):
        """
        :param fingerprint_length: Total bits in the resulting fingerprint.
        :param use_gpu:           Whether to use GPU acceleration (if available).
        :param frame_grid_size:   Grid size for feature extraction (e.g. 16 => 16x16 cells).
        :param scale_factor:      Factor to downscale each frame before extraction.
        :param frames_to_sample:  Optional override for how many frames to sample (up to 16).
        """
        self.fingerprint_length = fingerprint_length

        # Decide on device: GPU if available and requested, otherwise CPU
        self.device = torch.device("cuda" if use_gpu and torch.cuda.is_available() else "cpu")

        # Grid-based feature extraction: each frame is split into frame_grid_size x frame_grid_size cells
        self.frame_grid_size = frame_grid_size
        self.bits_per_frame = self.frame_grid_size * self.frame_grid_size

        # Determine how many frames we need to capture to reach fingerprint_length bits
        if frames_to_sample is not None:
            self.frames_needed = min(16, frames_to_sample)
        else:
            self.frames_needed = min(16, int(np.ceil(fingerprint_length / self.bits_per_frame)))

        self.scale_factor = scale_factor
        self.sampling_strategy = "uniform"

        # Logging setup
        self.logger = CustomLogger(__name__).get_logger()
        self.logger.debug("VideoFingerprinter initialized with:")
        self.logger.debug(f"  - Device: {self.device}")
        self.logger.debug(f"  - Fingerprint length: {self.fingerprint_length} bits")
        self.logger.debug(f"  - Grid size: {self.frame_grid_size}x{self.frame_grid_size}")
        self.logger.debug(f"  - Scale factor: {self.scale_factor}")
        self.logger.debug(f"  - Frames to sample: {self.frames_needed}")

    def extract_fingerprint(self, video_path: str) -> VideoFileInfo:
        """
        Extract a video fingerprint and metadata from the given video file.
        Returns a VideoFileInfo object with a hex digest and core metadata.
        """
        try:
            # Check file existence
            if not os.path.exists(video_path):
                self.logger.warning(f"Video file not found: {video_path}")
                return VideoFileInfo(
                    hex=None, fps=0, width=0, height=0,
                    resolution='0x0', frame_count=0, length=0.0
                )

            # Attempt to open video
            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                self.logger.warning(f"Could not open video file: {video_path}")
                return VideoFileInfo(
                    hex=None, fps=0, width=0, height=0,
                    resolution='0x0', frame_count=0, length=0.0
                )

            # Gather metadata
            fps = cap.get(cv2.CAP_PROP_FPS)
            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            resolution = f"{width}x{height}"

            # Fallback if fps or frame_count are invalid
            if frame_count <= 0 or fps <= 0:
                if frame_count <= 0:
                    frame_count = 300  # fallback
                if fps <= 0:
                    fps = 30  # fallback

            # Sample frames for feature extraction
            frame_indices = self._uniform_sampling(min(frame_count, 1000))

            # Read up to 5 frames for faster processing
            frames = []
            for idx in frame_indices[:min(5, len(frame_indices))]:
                cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
                ret, frame = cap.read()
                if ret:
                    # Resize frame to smaller size
                    small_frame = cv2.resize(
                        frame,
                        (self.frame_grid_size * self.scale_factor, self.frame_grid_size * self.scale_factor)
                    )
                    # Convert to grayscale
                    gray = cv2.cvtColor(small_frame, cv2.COLOR_BGR2GRAY)
                    frames.append(gray)
                if len(frames) >= 5:
                    break

            cap.release()

            # If no frames could be read, fallback to file-metadata-based hash
            if not frames:
                file_hash = hashlib.md5(f"{video_path}-{width}-{height}-{fps}".encode()).hexdigest()
                return VideoFileInfo(
                    hex=file_hash,
                    fps=fps,
                    width=width,
                    height=height,
                    frame_count=frame_count,
                    resolution=resolution,
                    length=frame_count / fps
                )

            # Extract features: GPU if available, else CPU
            if str(self.device) == 'cuda':
                features = self._extract_features_gpu(frames)
            else:
                features = self._extract_features_simple(frames)

            # Convert features to binary
            binary_fingerprint = self._features_to_binary_simple(features)
            # Convert binary to hex
            hex_fingerprint = self._binary_to_hex_simple(binary_fingerprint)

            return VideoFileInfo(
                hex=hex_fingerprint,
                fps=fps,
                width=width,
                height=height,
                frame_count=frame_count,
                resolution=resolution,
                length=frame_count / fps
            )

        except Exception as e:
            # If something unexpected happens, generate a fallback
            self.logger.warning(f"Error extracting video fingerprint for {video_path}: {str(e)}")
            fallback_hash = hashlib.md5(video_path.encode()).hexdigest()
            return VideoFileInfo(
                hex=fallback_hash,
                fps=0,
                width=0,
                height=0,
                resolution='0x0',
                frame_count=0,
                length=0.0
            )

    def _uniform_sampling(self, frame_count: int):
        """Uniformly sample frames from the video, up to self.frames_needed total."""
        if frame_count <= self.frames_needed:
            return list(range(frame_count))
        step = frame_count / self.frames_needed
        return [int(i * step) for i in range(self.frames_needed)]

    def _extract_features_simple(self, frames: list[np.ndarray]) -> list[list[float]]:
        """
        CPU-based feature extraction. For each frame, compute the average
        brightness in each cell of a grid, returning a list of features per frame.
        """
        features = []
        for frame in frames:
            h, w = frame.shape
            cell_h = h // self.frame_grid_size
            cell_w = w // self.frame_grid_size

            grid_features = []
            for i in range(self.frame_grid_size):
                for j in range(self.frame_grid_size):
                    cell = frame[i*cell_h:(i+1)*cell_h, j*cell_w:(j+1)*cell_w]
                    avg = np.mean(cell)
                    grid_features.append(avg)
            features.append(grid_features)
        return features

    def _extract_features_gpu(self, frames: list[np.ndarray]) -> list[list[float]]:
        """
        GPU-accelerated feature extraction using PyTorch.
        Adaptively average-pools each frame to a grid, then extracts the cell means.
        """
        try:
            batch_size = len(frames)
            if batch_size == 0:
                self.logger.debug("No frames to process with GPU.")
                return []

            # Dimensions from the first frame
            h, w = frames[0].shape
            cell_h = h // self.frame_grid_size
            cell_w = w // self.frame_grid_size
            if h <= 0 or w <= 0 or cell_h <= 0 or cell_w <= 0:
                self.logger.warning(f"Invalid frame dimensions: {h}x{w}, falling back to CPU.")
                return self._extract_features_simple(frames)

            if torch.cuda.is_available():
                torch.cuda.empty_cache()

            # Create a batch tensor on the GPU
            try:
                batch_tensor = torch.zeros((batch_size, 1, h, w), dtype=torch.float32, device=self.device)
            except Exception as e:
                self.logger.warning(f"Failed to allocate GPU memory for batch tensor: {e}, falling back to CPU.")
                return self._extract_features_simple(frames)

            # Fill the batch tensor with frame data
            for i, frame in enumerate(frames):
                try:
                    frame_tensor = torch.from_numpy(frame).float().unsqueeze(0)
                    batch_tensor[i] = frame_tensor
                except Exception as e:
                    self.logger.warning(f"Failed to convert frame {i} to tensor: {e}, falling back to CPU.")
                    return self._extract_features_simple(frames)

            # Use adaptive pooling to get average cell values
            adaptive_pool = torch.nn.AdaptiveAvgPool2d((self.frame_grid_size, self.frame_grid_size)).to(self.device)
            pooled = adaptive_pool(batch_tensor)
            # Flatten each frame's pooled grid
            grid_features = pooled.view(batch_size, -1)

            # Move results back to CPU
            features = []
            for i in range(batch_size):
                frame_features = grid_features[i].cpu().numpy().tolist()
                features.append(frame_features)

            # Debug GPU usage
            if torch.cuda.is_available():
                self.logger.debug(f"GPU memory allocated: {torch.cuda.memory_allocated() / 1024**2:.2f} MB")
                self.logger.debug(f"GPU memory cached: {torch.cuda.memory_reserved() / 1024**2:.2f} MB")

            return features
        except Exception as e:
            import traceback
            self.logger.warning(f"GPU feature extraction failed: {str(e)}. Falling back to CPU.")
            self.logger.debug(f"GPU extraction traceback: {traceback.format_exc()}")
            if torch.cuda.is_available():
                try:
                    torch.cuda.empty_cache()
                except:
                    pass
            return self._extract_features_simple(frames)

    def _features_to_binary_simple(self, features: list[list[float]]) -> list[int]:
        """
        Convert feature arrays to a binary fingerprint. By default,
        if GPU is available, attempt a GPU approach; otherwise, do CPU.
        """
        if str(self.device) == 'cuda' and features and len(features) > 0:
            try:
                features_tensor = torch.tensor(features, dtype=torch.float32, device=self.device)
                medians = torch.median(features_tensor, dim=1).values.unsqueeze(1)
                binary_tensor = (features_tensor > medians).int()
                binary = binary_tensor.view(-1).cpu().numpy().tolist()

                # Ensure correct length
                if len(binary) < self.fingerprint_length:
                    binary.extend([0] * (self.fingerprint_length - len(binary)))
                else:
                    binary = binary[:self.fingerprint_length]
                return binary
            except Exception as e:
                self.logger.warning(f"GPU binary conversion failed: {str(e)}. Falling back to CPU.")

        # CPU fallback
        binary = []
        for frame_features in features:
            median = np.median(frame_features)
            frame_binary = [1 if f > median else 0 for f in frame_features]
            binary.extend(frame_binary)

        if len(binary) < self.fingerprint_length:
            binary.extend([0] * (self.fingerprint_length - len(binary)))
        else:
            binary = binary[:self.fingerprint_length]

        return binary

    def _binary_to_hex_simple(self, binary: list[int]) -> str:
        """
        Convert a list of bits (0/1) into a hex string by grouping each 8 bits into a byte.
        """
        byte_count = (len(binary) + 7) // 8
        byte_array = bytearray(byte_count)

        for i, bit in enumerate(binary):
            if bit:
                byte_array[i // 8] |= 1 << (i % 8)

        return byte_array.hex()