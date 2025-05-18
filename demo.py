# demo.py
"""
Demonstrates the use of the multithreaded PipelineManager with dummy data
and simulated processing stages.

Created on 2025-05-19 by [Abdur Rahman](https://github.com/abdur75648)
"""
import time
import random
import os
import traceback
import logging
import threading
import concurrent.futures
from queue import Queue, Empty
from threading import Event
from tqdm import tqdm
from typing import Any, Optional, Dict

# Assuming pipeline_core is in the same directory or Python path
from pipeline_core.pipeline_manager import PipelineManager, BaseWorker
from pipeline_core.utils import SENTINEL, setup_logger, robust_put

logger = setup_logger("PipelineDemo")

# --- Define Dummy Worker Classes ---

class PreProcessingWorker(BaseWorker):
    """Simulates a pre-processing stage (e.g., data loading, feature extraction)."""
    def process_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        frame_id = item.get('id', 'unknown_id')
        logger.info(f"PreProcessing: Starting frame {frame_id}")
        try:
            # Simulate some work
            time.sleep(random.uniform(0.05, 0.15)) # Simulate I/O or light CPU work
            item['preprocessed_data'] = f"preprocessed_for_frame_{frame_id}"
            item['status_pre'] = "success"
            logger.info(f"PreProcessing: Finished frame {frame_id}")
            return item
        except Exception as e:
            logger.error(f"PreProcessing: Error on frame {frame_id}: {e}")
            item['status_pre'] = "error"
            # Depending on severity, might return item with error or None to filter
            return item # Pass item along with error status

class ModelInferenceWorker(BaseWorker):
    """Simulates a model inference stage (e.g., running a neural network)."""
    def process_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        frame_id = item.get('id', 'unknown_id')
        if item.get('status_pre') == "error":
            logger.warning(f"Inference: Skipping frame {frame_id} due to pre-processing error.")
            item['status_inference'] = "skipped_due_to_pre_error"
            return item

        logger.info(f"Inference: Starting on {item.get('preprocessed_data', 'N/A')}")
        try:
            # Simulate heavier computation (e.g., GPU-bound task)
            time.sleep(random.uniform(0.2, 0.5))
            item['inference_result'] = f"inference_output_for_frame_{frame_id}"
            item['status_inference'] = "success"
            # Simulate an occasional error in inference
            if random.random() < 0.05: # 5% chance of error
                logger.error(f"Inference: Simulated error on frame {frame_id}!")
                item['status_inference'] = "simulated_error"
                # For a critical error, you might raise an exception here to test stop_event propagation
                # raise RuntimeError(f"Simulated critical inference error on frame {frame_id}")
            logger.info(f"Inference: Finished frame {frame_id}")
            return item
        except Exception as e:
            logger.error(f"Inference: Error on frame {frame_id}: {e}")
            item['status_inference'] = "error"
            return item

class PostProcessingWorker(BaseWorker):
    """
    Simulates a post-processing stage.
    Demonstrates using a ThreadPoolExecutor for parallelizing sub-tasks within this stage.
    """
    def __init__(self, name: str, input_queue: Queue, output_queue: Optional[Queue],
                 stop_event: Event, pbar: Optional[tqdm] = None, num_sub_workers: int = 2):
        super().__init__(name, input_queue, output_queue, stop_event, pbar)
        self.num_sub_workers = max(1, num_sub_workers)
        self.sub_task_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.num_sub_workers,
            thread_name_prefix=f"{self.name}SubWorker"
        )
        self.active_sub_tasks: Dict[concurrent.futures.Future, Dict[str, Any]] = {}
        self.results_buffer: Dict[int, Dict[str, Any]] = {} # For ordered output
        self.next_expected_frame_id_for_output: int = 0
        logger.info(f"PostProcessingWorker '{self.name}' initialized with {self.num_sub_workers} sub-workers.")

    def _perform_sub_task(self, sub_item: Dict[str, Any]) -> Dict[str, Any]:
        """The actual work done by sub-worker threads."""
        frame_id = sub_item.get('id', 'unknown_id')
        if self.stop_event.is_set(): # Check global stop event
            logger.warning(f"PostProcessingSubTask: Stop event set, aborting for frame {frame_id}.")
            sub_item['status_post_sub'] = "aborted_by_stop"
            return sub_item

        logger.debug(f"PostProcessingSubTask: Starting for frame {frame_id}")
        try:
            # Simulate CPU-bound work for the sub-task
            time.sleep(random.uniform(0.1, 0.3)) # e.g., image resizing, blending, saving components
            sub_item['postprocessed_data'] = f"postprocessed_final_for_frame_{frame_id}"
            sub_item['status_post_sub'] = "success"
            logger.debug(f"PostProcessingSubTask: Finished for frame {frame_id}")
            return sub_item
        except Exception as e:
            logger.error(f"PostProcessingSubTask: Error for frame {frame_id}: {e}")
            sub_item['status_post_sub'] = "error"
            # If a sub-task error is critical enough to stop the pipeline, set the main stop_event
            # self.stop_event.set() # Example of escalating a sub-task error
            return sub_item


    def process_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Receives item from inference, submits its sub-tasks to ThreadPoolExecutor.
        This method itself doesn't return the final item directly to the output queue;
        it manages futures and a results_buffer.
        The actual outputting of ordered items happens in the _run loop's extension.
        """
        frame_id = item.get('id', 'unknown_id')
        if item.get('status_inference') in ["error", "skipped_due_to_pre_error", "simulated_error"]:
            logger.warning(f"PostProcessing: Skipping frame {frame_id} due to inference status: {item['status_inference']}.")
            # We still need to pass this item through for ordered output and pbar update
            item['status_post'] = "skipped_due_to_inference_error"
            # Directly buffer it for ordered output
            with self._buffer_lock: # Assuming _buffer_lock exists if this part is complex
                 self.results_buffer[frame_id] = item
            return None # Signal to _run loop not to put anything on output_queue from this call directly

        logger.info(f"PostProcessing: Received {item.get('inference_result', 'N/A')}. Submitting sub-task.")
        try:
            future = self.sub_task_executor.submit(self._perform_sub_task, item)
            # Store the original item (or just frame_id) with the future to retrieve it later
            self.active_sub_tasks[future] = item
        except RuntimeError as e: # E.g., if executor is shutting down
            logger.error(f"PostProcessing: Failed to submit sub-task for frame {frame_id}: {e}")
            if not self.stop_event.is_set(): self.stop_event.set() # Critical failure
        return None # Results will be handled via futures and buffer

    def _handle_completed_sub_tasks(self):
        """Checks for completed sub-tasks and moves them to the results_buffer."""
        if not hasattr(self, '_buffer_lock'): # Initialize lock if not present
             self._buffer_lock = threading.Lock()

        done_futures = []
        for future, original_item in list(self.active_sub_tasks.items()): # Iterate copy for safe removal
            if future.done():
                done_futures.append(future)
                try:
                    processed_sub_item = future.result() # This is the item returned by _perform_sub_task
                    frame_id = processed_sub_item.get('id')
                    with self._buffer_lock:
                        self.results_buffer[frame_id] = processed_sub_item
                    # logger.debug(f"PostProcessing: Sub-task for frame {frame_id} completed.")
                except concurrent.futures.CancelledError:
                    logger.info(f"PostProcessing: Sub-task for frame {original_item.get('id')} was cancelled.")
                except Exception as e:
                    logger.error(f"PostProcessing: Sub-task for frame {original_item.get('id')} failed: {e}")
                    # Handle error: potentially put original_item with error status into buffer
                    original_item['status_post'] = "sub_task_error"
                    with self._buffer_lock:
                         self.results_buffer[original_item.get('id')] = original_item
                # Remove from active tasks regardless of outcome
                del self.active_sub_tasks[future]

    def _output_ordered_results(self):
        """Outputs items from results_buffer in order to the main output_queue."""
        if not hasattr(self, '_buffer_lock'): self._buffer_lock = threading.Lock()

        with self._buffer_lock:
            while self.next_expected_frame_id_for_output in self.results_buffer:
                item_to_output = self.results_buffer.pop(self.next_expected_frame_id_for_output)
                item_to_output['status_post'] = item_to_output.get('status_post_sub', 'unknown_post_status') # Final status
                
                logger.info(f"PostProcessing: Outputting final result for frame {self.next_expected_frame_id_for_output}")
                
                if self.output_queue: # Should be None for the last worker
                    if not robust_put(self.output_queue, item_to_output, self.stop_event, self.name):
                        logger.warning(f"PostProcessing: Could not put final item for frame {self.next_expected_frame_id_for_output} "
                                       "on output queue due to stop signal.")
                        # If robust_put fails, we might need to re-buffer or handle error
                        self.results_buffer[self.next_expected_frame_id_for_output] = item_to_output # Re-buffer
                        break # Stop trying to output for now if queueing failed
                
                if self.pbar: # Last worker updates the main progress bar
                    self.pbar.update(1)
                
                self.next_expected_frame_id_for_output += 1


    def _run(self): # Override BaseWorker's _run for more complex logic
        """Extended run loop for PostProcessingWorker to manage sub-tasks and ordered output."""
        logger.info(f"Worker '{self.name}' (PostProcessing with sub-workers) started.")
        success_flag = True
        items_processed_by_main_worker = 0 # Items received by this worker
        
        # Initialize lock if it wasn't during sub-task handling (e.g., no items yet)
        if not hasattr(self, '_buffer_lock'): self._buffer_lock = threading.Lock()


        try:
            input_stream_ended = False
            while not self.stop_event.is_set():
                # 1. Try to get a new item from the input queue if the stream hasn't ended
                item_from_input_queue = None
                if not input_stream_ended:
                    try:
                        item_from_input_queue = self.input_queue.get(block=False, timeout=0.01) # Short timeout or non-blocking
                    except Empty:
                        pass # No new item, proceed to check sub-tasks

                    if item_from_input_queue is SENTINEL:
                        logger.info(f"Worker '{self.name}' received SENTINEL from input queue.")
                        input_stream_ended = True
                        self.input_queue.task_done()
                        # Continue loop to process active_sub_tasks and results_buffer
                    elif item_from_input_queue is not None:
                        items_processed_by_main_worker +=1
                        # process_item now submits to ThreadPoolExecutor and returns None
                        self.process_item(item_from_input_queue)
                        self.input_queue.task_done()

                # 2. Handle completed sub-tasks
                self._handle_completed_sub_tasks()

                # 3. Output ordered results from the buffer
                self._output_ordered_results()

                # 4. Check for exit condition
                if input_stream_ended and not self.active_sub_tasks and not self.results_buffer:
                    logger.info(f"Worker '{self.name}': Input stream ended, all sub-tasks done, and buffer empty. Exiting.")
                    break

                # 5. Brief sleep if idle to prevent busy-waiting
                is_input_idle = input_stream_ended or self.input_queue.empty()
                no_active_futures_to_check_immediately = not self.active_sub_tasks or all(not f.done() for f in self.active_sub_tasks)

                if is_input_idle and no_active_futures_to_check_immediately and not self.stop_event.is_set():
                    time.sleep(0.01) # Small sleep

            if not success_flag or self.stop_event.is_set():
                logger.warning(f"Worker '{self.name}' main loop exited due to failure or stop signal.")

        except Exception as e_outer:
            logger.error(f"Worker '{self.name}' CRASHED: {e_outer}")
            traceback.print_exc()
            self.stop_event.set()
            success_flag = False
        finally:
            logger.info(f"Worker '{self.name}' shutting down sub-task executor...")
            # Shutdown ThreadPoolExecutor, wait for current sub-tasks to complete (or cancel if stop_event)
            if hasattr(self.sub_task_executor, '_shutdown'): # Check if it's initialized
                if self.stop_event.is_set():
                    logger.info(f"Worker '{self.name}': Stop event set, attempting to cancel pending sub-tasks.")
                    # For Python 3.9+ ThreadPoolExecutor has cancel_futures
                    if hasattr(self.sub_task_executor, 'shutdown') and hasattr(concurrent.futures, 'CancelledError'):
                        self.sub_task_executor.shutdown(wait=False, cancel_futures=True)
                    elif hasattr(self.sub_task_executor, 'shutdown'):
                         self.sub_task_executor.shutdown(wait=False) # Best effort for older Python
                else:
                    self.sub_task_executor.shutdown(wait=True) # Wait for all tasks to finish
            
            # One final attempt to process any stragglers from sub-tasks and output buffer
            logger.info(f"Worker '{self.name}': Final processing of completed sub-tasks and output buffer.")
            self._handle_completed_sub_tasks() # Process any just-finished futures
            self._output_ordered_results()    # Flush the buffer

            final_status = "normally" if success_flag and not self.stop_event.is_set() else "due to error/stop"
            logger.info(f"Worker '{self.name}' finished ({items_processed_by_main_worker} items received, status: {final_status}).")
            
            # Ensure SENTINEL is propagated if this worker has an output queue (it shouldn't for this demo)
            if self.output_queue and (not success_flag or self.stop_event.is_set()):
                logger.info(f"Worker '{self.name}' attempting to put final SENTINEL on output queue.")
                robust_put(self.output_queue, SENTINEL, Event(), self.name)
            logger.info(f"Worker '{self.name}' thread exit.")


# --- Demo Data Producer ---
def generate_dummy_data(output_queue: Queue, stop_event: Event, num_frames: int = 20):
    """Produces dummy data and puts it onto the first queue of the pipeline."""
    logger.info("Data Producer: Starting to generate dummy frames.")
    for i in range(num_frames):
        if stop_event.is_set():
            logger.info("Data Producer: Stop event detected. Halting data generation.")
            break
        frame_data = {
            'id': i,
            'raw_data': f"raw_frame_content_{i}",
            'timestamp': time.time()
        }
        logger.debug(f"Data Producer: Generating frame {i}")
        # Simulate some delay in data arrival
        time.sleep(random.uniform(0.01, 0.05))
        if not robust_put(output_queue, frame_data, stop_event, "DataProducer"):
            logger.warning("Data Producer: Failed to put frame on queue (pipeline likely stopping).")
            break # Stop producing if cannot put
    
    logger.info("Data Producer: Finished generating data. Putting SENTINEL.")
    robust_put(output_queue, SENTINEL, Event(), "DataProducer") # Use fresh event for final sentinel
    logger.info("Data Producer: Exiting.")


# --- Main Demo Execution ---
def run_demo_pipeline():
    """Sets up and runs the demo pipeline."""
    num_dummy_frames = 50 # Number of dummy frames to process
    
    # Initialize the pipeline manager
    # Pass num_dummy_frames for accurate progress bar, or None for indeterminate
    pipeline_mgr = PipelineManager(num_total_items=num_dummy_frames)

    try:
        # Add worker stages to the pipeline
        # Stage 1: Pre-processing
        q_preproc_out = pipeline_mgr.add_worker(
            worker_class=PreProcessingWorker,
            name="PreProcessingStage",
            # input_queue_idx=None (first worker, creates its own input queue)
            output_queue_needed=True
        )

        # Stage 2: Model Inference
        q_inference_out = pipeline_mgr.add_worker(
            worker_class=ModelInferenceWorker,
            name="ModelInferenceStage",
            input_queue_idx=len(pipeline_mgr.queues) - 1, # Use the output queue of the previous worker
            output_queue_needed=True
        )

        # Stage 3: Post-processing (with parallel sub-tasks)
        # This is the last worker, so output_queue_needed=False (implicitly handled by BaseWorker if pbar is for it)
        # The pbar from PipelineManager will be passed to this worker.
        num_cpu_for_post = os.cpu_count()
        post_processing_sub_workers = max(1, num_cpu_for_post // 2 if num_cpu_for_post else 1)
        
        pipeline_mgr.add_worker(
            worker_class=PostProcessingWorker,
            name="PostProcessingStage",
            input_queue_idx=len(pipeline_mgr.queues) - 1,
            output_queue_needed=False, # Last stage, results are "consumed" (e.g., written, logged)
            worker_args={'num_sub_workers': post_processing_sub_workers}
        )

        # Start the pipeline. The first queue (self.queues[0]) will be fed by generate_dummy_data.
        pipeline_mgr.start(initial_data_producer=lambda q, se: generate_dummy_data(q, se, num_dummy_frames))
        
        # Wait for the pipeline to complete all processing
        pipeline_mgr.wait_for_completion()

        logger.info("Demo pipeline finished successfully.")

    except Exception as e:
        logger.error(f"An error occurred during pipeline execution: {e}")
        traceback.print_exc()
        if pipeline_mgr: # If manager exists, ensure stop event is set
            pipeline_mgr.stop_event.set()
            # Optionally, try to join threads even on error for cleanup
            logger.info("Attempting to join threads after error...")
            for worker in pipeline_mgr.workers:
                worker.join(timeout=2.0)
    finally:
        if pipeline_mgr and pipeline_mgr.pbar and not pipeline_mgr.pbar.disable:
            pipeline_mgr.pbar.close() # Ensure pbar is closed
            logger.info("Progress bar closed in main finally block.")


if __name__ == "__main__":
    # Configure root logger for better visibility of all logs
    # This is a simple setup; for complex apps, consider more advanced logging config.
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger.info("Starting multithreaded pipeline demo...")
    run_demo_pipeline()
    logger.info("Multithreaded pipeline demo finished.")