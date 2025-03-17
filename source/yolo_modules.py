import sys
import os
import contextlib
import time
import warnings
import requests
import numpy as np
import torch
from decimal import Decimal
from typing import List, Optional, Tuple
from PIL import Image
from dataclasses import dataclass
from io import StringIO
from ultralytics import YOLO
from decimal import Decimal
from source.logging_modules import CustomLogger

# Disable all warnings from ultralytics and torch
os.environ['ULTRALYTICS_DISABLE_PRINT'] = 'True'
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=RuntimeWarning)

@dataclass
class YoloResult:
    has_human: bool
    confidence: Decimal
    human_count: int

def get_yolo_provider(use_remote=False, remote_url=None, max_retries=3, retry_delay=2, enable_fallback=True, **local_params):
    logger = CustomLogger(__name__).get_logger()
    
    # Default parameters for local provider
    default_params = {
        "model_path": "model/yolov8x.pt",
        "iou": 0.5,
        "conf": 0.5,
        "device": "auto"
    }
    
    # Override with any provided parameters
    default_params.update(local_params)
    
    if use_remote:
        if not remote_url:
            raise ValueError("Remote URL is required when use_remote=True")
            
        # If fallback enabled, pass YoloProvider class and params
        fallback_provider = YoloProvider if enable_fallback else None
        fallback_params = default_params if enable_fallback else None
        
        logger.info(f"[bright_black][Yolo]📸[/bright_black][bold green] Initializing REMOTE YOLO provider with URL: {remote_url}[/bold green]")
        return RemoteYoloProvider(
            server_url=remote_url, 
            max_retries=max_retries,
            retry_delay=retry_delay,
            fallback_provider=fallback_provider,
            fallback_params=fallback_params
        )
    else:
        # Just use local provider directly
        logger.info(f"[bright_black][Yolo]📸[/bright_black][bold yellow] Initializing LOCAL YOLO provider with model: {default_params['model_path']}[/bold yellow]")
        return YoloProvider(**default_params)

class YoloProvider:
    def __init__(self, 
                 model_path: str, 
                 iou: float = 0.5, 
                 conf: float = 0.5, 
                 device: str = "auto"):
        """
        Initialize the YOLO model with minimal output.
        """
        # Redirect stdout and stderr to devnull to suppress all prints
        self.devnull = open(os.devnull, 'w')
        sys.stdout = self.devnull
        sys.stderr = self.devnull

        self.logger = CustomLogger(__name__).get_logger()
        
        try:
            # Suppress prints during model loading
            with contextlib.redirect_stdout(self.devnull), contextlib.redirect_stderr(self.devnull):
                self.model = YOLO(model_path, verbose=False)
                self.model.task = "detect"
                self.model.iou = iou
                self.model.conf = conf

            # Restore stdout and stderr
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__
            self.devnull.close()

            self.device = self._select_device(device)
        except Exception as e:
            # Restore stdout and stderr in case of error
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__
            self.devnull.close()

            self.logger.error(
                f"[bright_black][Yolo]📸[/bright_black][bold red] "
                f"Failed to load YOLO model[/bold red] from [bold yellow]{model_path}[/bold yellow]: {e}"
            )
            raise

    def _select_device(self, device: str) -> str:
        """
        Select the most appropriate device for YOLO inference.
        """
        cuda_available = torch.cuda.is_available()
        
        if device.lower() == "auto":
            selected_device = "cuda:0" if cuda_available else "cpu"
        elif device.lower() == "gpu":
            selected_device = "cuda:0" if cuda_available else "cpu"
        else:
            selected_device = "cpu"

        return selected_device

    def _predict(self, image: np.ndarray) -> List[dict]:
        """
        Perform object detection on a given image with suppressed output.
        """
        try:
            # Additional image preprocessing and validation
            if image is None:
                self.logger.error("[bright_black][Yolo]📸[/bright_black] Received None image")
                return []

            # Ensure the image has the correct number of channels
            if len(image.shape) < 3:
                # Convert grayscale to RGB
                image = np.stack((image,)*3, axis=-1)
            elif image.shape[2] > 3:
                # Truncate to first 3 channels if more exist
                image = image[:,:,:3]

            # Ensure the image is in uint8 format
            if image.dtype != np.uint8:
                image = (image * 255).astype(np.uint8)

            # Validate image dimensions
            if image.shape[0] == 0 or image.shape[1] == 0:
                self.logger.error("[bright_black][Yolo]📸[/bright_black] Image has zero dimensions")
                return []

            # Redirect stdout and stderr to devnull
            devnull = open(os.devnull, 'w')
            sys.stdout = devnull
            sys.stderr = devnull

            try:
                start_time = time.time()
                results = self.model.predict(
                    image, 
                    device=self.device, 
                    verbose=False,
                    stream=False
                )
                
                prediction_time = time.time() - start_time
                self.logger.debug(
                    f"[bright_black][Yolo]📸[/bright_black] "
                    f"Prediction completed in {prediction_time:.4f} seconds"
                )
                return results
            finally:
                # Restore stdout and stderr
                sys.stdout = sys.__stdout__
                sys.stderr = sys.__stderr__
                devnull.close()

        except Exception as e:
            self.logger.error(
                f"[bright_black][Yolo]📸[/bright_black] Prediction failed: {e}\n"
                f"Image shape: {image.shape if image is not None else 'N/A'}\n"
                f"Image dtype: {image.dtype if image is not None else 'N/A'}"
            )
            return []

    def has_human(self, filepath: str) -> YoloResult:
        """
        Detect if an image contains humans.
        """
        try:
            # Load and validate image
            image = Image.open(filepath)
            image.verify()
            image = Image.open(filepath)
            image_data = np.array(image)

            # Log image details        
            self.logger.debug(
                f"[bright_black][Yolo]📸[/bright_black] "
                f"Image loaded: path={filepath}, shape={image_data.shape}, dtype={image_data.dtype}"
            )
          
            # Predict
            results = self._predict(image_data)
            
            # Process results
            max_conf = Decimal('0.0')
            has_human = False
            human_count = 0
            
            for result in results:
                boxes = result.boxes
                for box in boxes:
                    cls_id = int(box.cls[0]) if hasattr(box.cls, '__getitem__') else int(box.cls)
                    cls_name = result.names[cls_id]
                    conf_val = float(box.conf[0]) if hasattr(box.conf, '__getitem__') else float(box.conf)
                    conf = Decimal(str(round(conf_val, 4)))
                    
                    if cls_name == "person":
                        has_human = True
                        if conf > max_conf:
                            max_conf = conf
                        human_count += 1
            
            return YoloResult(has_human, max_conf, human_count)
        
        except Exception as e:
            self.logger.error(
                f"[bright_black][Yolo]📸[/bright_black][bold red] "
                f"Error processing image from {filepath}:[/bold red] {e}"
            )
            return YoloResult(False, Decimal('0.0'), 0)

class RemoteYoloProvider:
    """
    YoloProvider implementation that uses a remote API server.
    Implements the same interface as the local YoloProvider with fallback capability.
    """
    def __init__(self, 
                 server_url: str,
                 timeout: int = 10,
                 max_retries: int = 3,
                 retry_delay: int = 2,
                 fallback_provider=None,
                 fallback_params=None):
        """
        Initialize with the remote server URL.
        
        Args:
            server_url: URL of the YOLO API server
            timeout: HTTP request timeout in seconds
            max_retries: Maximum number of retries before falling back
            retry_delay: Seconds to wait between retries
            fallback_provider: Local YoloProvider class for fallback
            fallback_params: Parameters to initialize local provider if fallback needed
        """
        self.server_url = server_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.fallback_provider = fallback_provider
        self.fallback_params = fallback_params or {}
        self.logger = CustomLogger(__name__).get_logger()
        self._local_provider = None
        
        # Test connection to server
        self.server_available = self._check_server_health()
        
        if not self.server_available and self.fallback_provider:
            self.logger.warning("Remote YOLO server unavailable. Will use local fallback.")
            self._initialize_fallback()

    def _check_server_health(self) -> bool:
        """Check if the remote server is available and healthy."""
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Checking remote YOLO server health (attempt {attempt+1}/{self.max_retries})...")
                response = requests.get(
                    f"{self.server_url}/health", 
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    self.logger.info(f"Remote YOLO server available at {self.server_url}")
                    return True
                else:
                    self.logger.warning(
                        f"Remote YOLO server health check failed with status code: {response.status_code}"
                    )
            except requests.RequestException as e:
                self.logger.warning(f"Failed to connect to remote YOLO server: {e}")
            
            # Wait before retrying
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay)
        
        return False

    def _initialize_fallback(self):
        """Initialize the local fallback provider if needed."""
        if self._local_provider is None and self.fallback_provider:
            try:
                self.logger.info("Initializing local YOLO fallback provider...")
                self._local_provider = self.fallback_provider(**self.fallback_params)
                self.logger.info("Local YOLO fallback provider initialized successfully")
            except Exception as e:
                self.logger.error(f"Failed to initialize local fallback provider: {e}")

    def has_human(self, filepath: str) -> YoloResult:
        """
        Detect if an image contains humans using the remote API.
        Falls back to local provider if remote call fails.
        """
        # If we already know the server is not available, use fallback immediately
        if not self.server_available:
            self.logger.info(f"[bright_black][Yolo-REMOTE]📸[/bright_black] Server known to be unavailable, using fallback for: {filepath}")
            return self._fallback_has_human(filepath)
                
        self.logger.debug(f"[bright_black][Yolo-REMOTE]📸[/bright_black] Processing image with REMOTE YOLO: {filepath}")
        
        # Try remote API with retries
        for attempt in range(self.max_retries):
            try:
                # Send the request to the remote API
                with open(filepath, 'rb') as file_data:
                    files = {'file': file_data}
                    self.logger.debug(f"Sending image to remote YOLO API (attempt {attempt+1}/{self.max_retries})")
                    
                    response = requests.post(
                        f"{self.server_url}/has_human", 
                        files=files,
                        timeout=self.timeout
                    )
                # Process response...
                
                if response.status_code == 200:
                    # Parse the response
                    result_data = response.json()
                    
                    # Map API response to YoloResult
                    return YoloResult(
                        has_human=result_data.get("has_human", False),
                        confidence=Decimal(str(result_data.get("score", 0.0))),
                        human_count=len([r for r in result_data.get("results", []) 
                                        if r.get("class_name", "").lower() == "person"])
                    )
                else:
                    self.logger.warning(
                        f"Remote YOLO API returned error: {response.status_code}, {response.text}"
                    )
            except Exception as e:
                self.logger.warning(f"Error calling remote YOLO API (attempt {attempt+1}): {e}")
            
            # Wait before retrying
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay)
        
        # If we get here, all remote attempts failed
        self.logger.error(f"All {self.max_retries} attempts to use remote YOLO API failed. Falling back to local.")
        self.server_available = False  # Mark server as unavailable to skip retries on future calls
        return self._fallback_has_human(filepath)
    
    def _fallback_has_human(self, filepath: str) -> YoloResult:
        """Use local provider as fallback when remote fails."""
        # Initialize local provider if needed
        if self._local_provider is None:
            self._initialize_fallback()
            
        # If we still don't have a local provider, return a default result
        if self._local_provider is None:
            self.logger.error("No fallback provider available. Returning default result.")
            return YoloResult(False, Decimal('0.0'), 0)
            
        # Use local provider
        self.logger.info(f"Using local YOLO fallback for: {filepath}")
        return self._local_provider.has_human(filepath)

if __name__ == "__main__":
    pass