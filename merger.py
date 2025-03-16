#!/usr/bin/env python3

import os
import sys
import signal
import argparse

from source.logging_modules import CustomLogger
from source.merger_modules import Merger
from source.yolo_modules import YoloProvider

stop_flag = False

def handle_sigint(sig, frame):
    """
    Set a global stop flag on Ctrl+C for graceful shutdown.
    """
    global stop_flag
    logger = CustomLogger(__name__).get_logger()
    logger.warning("Keyboard interrupt detected. Stopping the merger...")
    stop_flag = True

def parse_arguments():
    """
    Parse command line arguments for the merger script.
    """
    parser = argparse.ArgumentParser(
        description="Merge media files from source directories into separate image and video destinations."
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        required=True,
        help="One or more source directories to merge from"
    )
    parser.add_argument(
        "--image-dest",
        default="/mnt/nas1/Photos/뷰티사진",
        help="Destination directory for image files (default: /mnt/nas1/Photos/뷰티사진)"
    )
    parser.add_argument(
        "--video-dest",
        default="/mnt/nas1/소장영상/뷰티영상",
        help="Destination directory for video files (default: /mnt/nas1/소장영상/뷰티영상)"
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Number of threads for parallel merging (default: 4)"
    )
    parser.add_argument(
        "--human-only",
        action="store_true",
        help="Only copy images that contain humans (requires YOLO model)"
    )
    parser.add_argument(
        "--yolo-model",
        default="model/yolov8x.pt",
        help="Path to YOLO model file (default: model/yolov8x.pt)"
    )
    return parser.parse_args()

def main():
    logger = CustomLogger(__name__).get_logger()
    
    # Catch Ctrl+C
    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    args = parse_arguments()
    logger.info("[Merger] Starting merge operation with arguments:")
    logger.info(f"  sources={args.sources}")
    logger.info(f"  image_dest={args.image_dest}")
    logger.info(f"  video_dest={args.video_dest}")
    logger.info(f"  threads={args.threads}")
    logger.info(f"  human_only={args.human_only}")
    
    # Initialize YOLO if human_only is enabled
    yolo_provider = None
    if args.human_only:
        logger.info(f"[Merger] Initializing YOLO model from {args.yolo_model}")
        try:
            yolo_provider = YoloProvider(
                model_path=args.yolo_model,
                iou=0.5,
                conf=0.5,
                device="auto"
            )
        except Exception as e:
            logger.error(f"[Merger] Failed to initialize YOLO model: {e}")
            logger.error("[Merger] Continuing without human detection")

    # Ensure destinations exist
    os.makedirs(args.image_dest, exist_ok=True)
    os.makedirs(args.video_dest, exist_ok=True)
    logger.info(f"[Merger] Image destination ensured: {args.image_dest}")
    logger.info(f"[Merger] Video destination ensured: {args.video_dest}")

    # Create and run the Merger
    merger = Merger(
        logger=logger,
        sources=args.sources,
        image_destination=args.image_dest,
        video_destination=args.video_dest,
        thread_count=args.threads,
        yolo_provider=yolo_provider,
        human_only=args.human_only
    )

    try:
        merger.run(lambda: stop_flag)
        logger.info("[Merger] Completed merge operation.")
    except Exception as e:
        logger.error(f"[Merger] Unexpected error in merging: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()