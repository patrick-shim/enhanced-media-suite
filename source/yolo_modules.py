import sys
import os
import contextlib
import time
import warnings
import numpy as np
import torch
from decimal import Decimal
from typing import List
from PIL import Image
from dataclasses import dataclass
from io import StringIO
from ultralytics import YOLO

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

if __name__ == "__main__":
    pass