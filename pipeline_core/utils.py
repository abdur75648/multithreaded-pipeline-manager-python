# pipeline_core/utils.py
"""
Utility functions and constants for the multithreaded pipeline.

Created on 2025-05-19 by [Abdur Rahman](https://github.com/abdur75648)
"""
import logging
import threading
from queue import Queue, Full
from threading import Event

# Sentinel object to signal the end of data in queues for graceful thread shutdown
SENTINEL = object()

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Sets up a simple logger.

    Args:
        name: The name of the logger.
        level: The logging level (e.g., logging.INFO, logging.DEBUG).

    Returns:
        A configured logging.Logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers: # Avoid adding multiple handlers if called multiple times
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

# Get a root logger for utils, can be used by other modules if they don't set up their own
logger = setup_logger(__name__)

def robust_put(q: Queue, item: any, stop_event: Event,
               worker_name: str = "UnknownWorker", timeout: float = 0.1) -> bool:
    """
    Puts an item onto a queue with a timeout, checking a stop_event.
    This prevents indefinite blocking if the consumer thread has died and the queue is full.

    Args:
        q: The queue to put the item onto.
        item: The item to put on the queue.
        stop_event: An Event object that signals whether the operation should be aborted.
        worker_name: Name of the worker attempting the put, for logging.
        timeout: The timeout in seconds for each queue.put attempt.

    Returns:
        True if the item was successfully put on the queue, False otherwise
        (e.g., if stop_event was set).
    """
    while not stop_event.is_set():
        try:
            q.put(item, block=True, timeout=timeout)
            return True  # Item successfully put
        except Full:
            # Queue is full, continue to loop and check stop_event
            continue
    # If loop exits, stop_event was set before item could be put
    if item is not SENTINEL: # Don't log excessively for sentinel during shutdown
        logger.debug(f"{worker_name}: Stop event set, not putting item: {type(item)}")
    return False