# pipeline_core/pipeline_manager.py
"""
Defines the PipelineManager and BaseWorker classes for creating
multithreaded data processing pipelines.

Created on 2025-05-19 by [Abdur Rahman](https://github.com/abdur75648)
"""
import time
import threading
import traceback
import os
from tqdm import tqdm
import concurrent.futures
from queue import Queue, Empty
from threading import Thread, Event
from typing import Callable, Any, Dict, List, Optional

from .utils import SENTINEL, robust_put, setup_logger

logger = setup_logger(__name__)

class BaseWorker:
    """
    Base class for a worker in the pipeline.
    Subclasses should implement the `process_item` method.
    """
    def __init__(self, name: str, input_queue: Queue, output_queue: Optional[Queue],
                 stop_event: Event, pbar: Optional[tqdm] = None):
        self.name = name
        self.input_queue = input_queue
        self.output_queue = output_queue # Can be None for the last worker
        self.stop_event = stop_event
        self.pbar = pbar # Optional tqdm progress bar instance
        self.thread: Optional[Thread] = None

    def _run(self):
        """Internal method executed by the worker's thread."""
        logger.info(f"Worker '{self.name}' started.")
        success_flag = True
        items_processed = 0
        try:
            while not self.stop_event.is_set():
                try:
                    # Get item from input queue with a timeout to allow checking stop_event
                    item = self.input_queue.get(block=True, timeout=0.1)
                except Empty:
                    if self.stop_event.is_set(): # If stop signaled during wait
                        break
                    continue # Queue empty, loop again

                if item is SENTINEL:
                    logger.info(f"Worker '{self.name}' received SENTINEL. Signaling downstream and exiting.")
                    if self.output_queue: # Pass sentinel to next stage
                        if not robust_put(self.output_queue, SENTINEL, self.stop_event, self.name):
                            logger.warning(f"Worker '{self.name}' could not put SENTINEL on output queue due to stop signal.")
                    self.input_queue.task_done() # Mark sentinel as processed
                    break # Exit worker loop

                try:
                    # --- Core processing logic by the specific worker ---
                    processed_item = self.process_item(item)
                    items_processed += 1

                    if processed_item is not None and self.output_queue:
                        if not robust_put(self.output_queue, processed_item, self.stop_event, self.name):
                            logger.warning(f"Worker '{self.name}' could not put processed item on output queue due to stop signal.")
                            success_flag = False; break # Stop if cannot output
                    elif processed_item is None and self.output_queue:
                        # If process_item returns None, it might mean filter out or error for this item.
                        # Depending on design, you might still need to update pbar or handle explicitly.
                        logger.debug(f"Worker '{self.name}' processed item but got None result. Not queueing.")
                    
                    # Update progress bar if it's the final processing stage before writing
                    # (or if pbar is passed to intermediate stages for different tracking)
                    if self.pbar and not self.output_queue: # Assume last worker updates main pbar
                        self.pbar.update(1)

                except Exception as e_item_proc:
                    logger.error(f"Worker '{self.name}' failed to process item: {item}. Error: {e_item_proc}")
                    traceback.print_exc()
                    # Decide if this is a critical error that should stop the pipeline
                    # For this generic template, we'll log and continue with next item,
                    # but for specific apps, you might want self.stop_event.set() here.
                    # self.stop_event.set(); success_flag = False; break # Example of critical failure
                finally:
                    self.input_queue.task_done() # Mark item from input queue as processed

            if not success_flag or self.stop_event.is_set():
                logger.warning(f"Worker '{self.name}' main loop exited due to failure or stop signal.")

        except Exception as e_outer: # Catch-all for unexpected errors in the worker's main loop
            logger.error(f"Worker '{self.name}' CRASHED: {e_outer}")
            traceback.print_exc()
            self.stop_event.set() # Signal global stop on worker crash
            success_flag = False
        finally:
            final_status = "normally" if success_flag and not self.stop_event.is_set() else "due to error/stop"
            logger.info(f"Worker '{self.name}' finished ({items_processed} items processed, status: {final_status}).")
            # Ensure SENTINEL is propagated if this worker exits prematurely and has an output queue
            if not success_flag or self.stop_event.is_set():
                if self.output_queue and item is not SENTINEL: # Check if last item processed was not sentinel
                    # Attempt to put sentinel if not already done, use a fresh event for this critical put
                    logger.info(f"Worker '{self.name}' attempting to put final SENTINEL on exit.")
                    robust_put(self.output_queue, SENTINEL, Event(), self.name)
            logger.info(f"Worker '{self.name}' thread exit.")

    def process_item(self, item: Any) -> Optional[Any]:
        """
        Processes a single item from the input queue.
        This method MUST be implemented by subclasses.

        Args:
            item: The data item to process.

        Returns:
            The processed item, or None if the item should be filtered out
            or an error occurred that should not halt the pipeline for this item.
        
        Raises:
            Exception: If a critical error occurs that should halt the pipeline.
                       The main _run loop will catch this and set the stop_event.
        """
        raise NotImplementedError("Subclasses must implement the process_item method.")

    def start(self):
        """Starts the worker thread."""
        if self.thread is None:
            self.thread = Thread(target=self._run, name=self.name, daemon=True)
            self.thread.start()
            logger.debug(f"Worker '{self.name}' thread object created and started.")
        else:
            logger.warning(f"Worker '{self.name}' attempt to start an already started thread.")

    def join(self, timeout: Optional[float] = None):
        """Waits for the worker thread to complete."""
        if self.thread and self.thread.is_alive():
            logger.debug(f"Joining worker '{self.name}' thread...")
            self.thread.join(timeout)
            if self.thread.is_alive():
                logger.warning(f"Worker '{self.name}' thread did not join within timeout.")
            else:
                logger.info(f"Worker '{self.name}' thread joined.")
        elif self.thread:
             logger.info(f"Worker '{self.name}' thread already finished.")
        else:
            logger.warning(f"Worker '{self.name}' no thread to join (was it started?).")


class PipelineManager:
    """
    Manages the overall multithreaded pipeline, including setup,
    starting workers, and handling graceful shutdown.
    """
    def __init__(self, num_total_items: Optional[int] = None):
        """
        Initializes the PipelineManager.

        Args:
            num_total_items: Optional total number of items to be processed, for progress bar.
        """
        self.workers: List[BaseWorker] = []
        self.queues: List[Queue] = []
        self.stop_event = Event() # Master stop event for this pipeline instance
        self.pbar: Optional[tqdm] = None
        self.num_total_items = num_total_items
        self.pipeline_started = False
        if self.num_total_items is not None and self.num_total_items > 0:
            self.pbar = tqdm(total=self.num_total_items, desc="Pipeline Progress", unit="item", dynamic_ncols=True)
        elif self.num_total_items == 0: # Explicitly zero items
             self.pbar = tqdm(total=0, desc="Pipeline Progress (0 items)", unit="item", dynamic_ncols=True)
        else: # Unknown number of items
            self.pbar = tqdm(desc="Pipeline Progress", unit="item", dynamic_ncols=True)


    def add_worker(self, worker_class: type[BaseWorker], name: str,
                   input_queue_idx: Optional[int] = None,
                   output_queue_needed: bool = True,
                   worker_args: Optional[Dict[str, Any]] = None) -> Queue:
        """
        Adds a worker stage to the pipeline.

        Args:
            worker_class: The class of the worker to add (must be a subclass of BaseWorker).
            name: A descriptive name for this worker stage.
            input_queue_idx: The index of the queue to be used as input for this worker.
                             If None, this is the first worker and will use a new input queue.
            output_queue_needed: If True, an output queue will be created for this worker.
                                 Set to False for the last worker in the pipeline.
            worker_args: Optional dictionary of additional arguments for the worker's constructor.

        Returns:
            The output queue of the added worker (or input queue if it's the first worker
            and no output queue is needed, though typically an output queue is returned).
            This returned queue can be used as the input for the next worker.
        """
        if not issubclass(worker_class, BaseWorker):
            raise TypeError("worker_class must be a subclass of BaseWorker.")
        if self.pipeline_started:
            raise RuntimeError("Cannot add workers after the pipeline has started.")

        current_input_queue: Queue
        if input_queue_idx is None: # First worker
            if self.workers: # Not the first worker if workers list is not empty
                raise ValueError("input_queue_idx cannot be None if it's not the first worker.")
            current_input_queue = Queue(maxsize=10) # Default maxsize, can be configurable
            self.queues.append(current_input_queue)
        elif 0 <= input_queue_idx < len(self.queues):
            current_input_queue = self.queues[input_queue_idx]
        else:
            raise ValueError(f"Invalid input_queue_idx: {input_queue_idx}")

        current_output_queue: Optional[Queue] = None
        if output_queue_needed:
            current_output_queue = Queue(maxsize=10) # Default maxsize
            self.queues.append(current_output_queue)
        
        # Pass pbar only to the last worker by default, or if explicitly configured
        pbar_for_worker = self.pbar if not output_queue_needed else None

        worker_instance = worker_class(name=name, input_queue=current_input_queue,
                                       output_queue=current_output_queue,
                                       stop_event=self.stop_event,
                                       pbar=pbar_for_worker,
                                       **(worker_args or {}))
        self.workers.append(worker_instance)
        logger.info(f"Added worker '{name}' to the pipeline.")
        # Return the output queue for chaining, or input if it's the very first and has no output
        return current_output_queue if current_output_queue else current_input_queue

    def start(self, initial_data_producer: Optional[Callable[[Queue, Event], None]] = None):
        """
        Starts all worker threads in the pipeline.

        Args:
            initial_data_producer: An optional callable that will be run in a separate
                                   thread to feed initial data into the pipeline's first queue.
                                   It should take the input queue and a stop event as arguments.
                                   If None, data must be put into the first queue externally.
        """
        if not self.workers:
            logger.error("Pipeline has no workers. Cannot start.")
            return
        if self.pipeline_started:
            logger.warning("Pipeline already started.")
            return

        logger.info("Starting pipeline workers...")
        self.pipeline_started = True
        # Reset stop event in case manager is reused (though typically one manager per run)
        self.stop_event.clear() 

        for worker in self.workers:
            worker.start()

        if initial_data_producer:
            if not self.queues:
                logger.error("No queues available for initial_data_producer. Add a worker first.")
                return
            
            producer_thread = Thread(target=initial_data_producer,
                                     args=(self.queues[0], self.stop_event),
                                     name="InitialDataProducer", daemon=True)
            producer_thread.start()
            # Add to a list if we need to join it specifically, for now it's daemon.
            logger.info("Initial data producer thread started.")


    def wait_for_completion(self, timeout: Optional[float] = None):
        """
        Waits for all worker threads to complete their processing.

        Args:
            timeout: Optional timeout in seconds for waiting on each worker.
        """
        if not self.pipeline_started:
            logger.warning("Pipeline not started, nothing to wait for.")
            return

        logger.info("Waiting for pipeline to complete...")
        try:
            # Main loop to keep the current thread alive and responsive to KeyboardInterrupt
            # while workers are running.
            while any(w.thread and w.thread.is_alive() for w in self.workers):
                if self.stop_event.is_set():
                    logger.info("Stop event detected by manager. Waiting for workers to terminate.")
                    break
                time.sleep(0.5) # Check periodically

        except KeyboardInterrupt:
            logger.warning("\nKeyboardInterrupt received by manager. Signaling pipeline to stop...")
            self.stop_event.set()
        
        # Ensure stop_event is set before joining, particularly if loop exited normally
        if not self.stop_event.is_set():
            logger.info("Pipeline workers seem to have finished. Setting stop_event for final cleanup.")
            self.stop_event.set()

        logger.info("Joining worker threads...")
        for worker in self.workers:
            worker.join(timeout=timeout if timeout else 10.0) # Default 10s timeout per worker

        if self.pbar and not self.pbar.disable: # Check if pbar was closed by a worker
            # Correct final pbar count if needed
            if self.num_total_items is not None and self.pbar.n < self.num_total_items and not self.stop_event.is_set():
                logger.warning(f"Progress bar count ({self.pbar.n}) less than total items ({self.num_total_items}) after normal completion.")
                # self.pbar.n = self.num_total_items # Optionally force to total
                # self.pbar.refresh()
            self.pbar.close()
            logger.info("Pipeline progress bar closed by manager.")

        logger.info("Pipeline processing finished.")
        self.pipeline_started = False # Reset for potential reuse, though not typical