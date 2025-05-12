#!/usr/bin/env python3
"""
Three-stage demo pipeline
------------------------

ReaderStage      – produces random strings
UpperCaseStage   – converts them to UPPER-CASE
ReverseStage     – reverses each string
WriterStage      – prints result to stdout

The demo stops automatically after --duration seconds *or* can be
terminated with Ctrl-C.  Use --error-rate > 0 to see robust shutdown
when a processor raises an exception.

Run:
    python examples/three_stage_demo.py --duration 8 --queue-size 4
"""

import argparse
import random
import string
import time
from queue import Queue
from threading import Event

from pipeline_manager import SENTINEL, PipelineStage, PipelineManager

# --------------------------------------------------------------------------- #
# Command-line arguments
# --------------------------------------------------------------------------- #
parser = argparse.ArgumentParser(description="Run demo multithreaded pipeline.")
parser.add_argument("--duration", type=int, default=10, help="seconds to run")
parser.add_argument(
    "--queue-size", type=int, default=5, help="max items per internal queue"
)
parser.add_argument(
    "--error-rate",
    type=float,
    default=0.0,
    help="probability [0-1] that Processor #1 throws to simulate failure",
)
args = parser.parse_args()


# --------------------------------------------------------------------------- #
# Shared objects
# --------------------------------------------------------------------------- #
stop_event = Event()  # one flag for the whole pipeline
q_read_to_proc1: Queue = Queue(maxsize=args.queue_size)
q_proc1_to_proc2: Queue = Queue(maxsize=args.queue_size)
q_proc2_to_write: Queue = Queue(maxsize=args.queue_size)

end_time = time.time() + args.duration


# --------------------------------------------------------------------------- #
# Stage business-logic callables
# --------------------------------------------------------------------------- #
def reader_fn(_: None) -> str:
    """
    Generate a new random 8-char string every 0.05 s until duration expires,
    then return SENTINEL exactly once to end the stream.
    """
    now = time.time()
    if now >= end_time:
        return SENTINEL

    time.sleep(0.05)
    return "".join(random.choices(string.ascii_letters, k=8))


def upper_case_fn(s: str) -> str:
    """
    Convert to upper case; optionally raise to demonstrate robustness.
    """
    # Simulated CPU work
    time.sleep(0.02)

    if args.error_rate and random.random() < args.error_rate:
        raise RuntimeError("💥 Simulated processor failure")

    return s.upper()


def reverse_fn(s: str) -> str:
    """Reverse the string (simulated second compute stage)."""
    time.sleep(0.01)
    return s[::-1]


def writer_fn(s: str) -> None:
    """Final sink – print output string."""
    print(f"RESULT → {s}")
    # simulate I/O latency
    time.sleep(0.02)
    # Returning None keeps PipelineStage from enqueuing downstream.


# --------------------------------------------------------------------------- #
# Build stages
# --------------------------------------------------------------------------- #
reader = PipelineStage(
    name="ReaderStage",
    stop_event=stop_event,
    process_fn=reader_fn,
    in_q=None,
    out_q=q_read_to_proc1,
)

proc1 = PipelineStage(
    name="UpperCaseStage",
    stop_event=stop_event,
    process_fn=upper_case_fn,
    in_q=q_read_to_proc1,
    out_q=q_proc1_to_proc2,
)

proc2 = PipelineStage(
    name="ReverseStage",
    stop_event=stop_event,
    process_fn=reverse_fn,
    in_q=q_proc1_to_proc2,
    out_q=q_proc2_to_write,
)

writer = PipelineStage(
    name="WriterStage",
    stop_event=stop_event,
    process_fn=writer_fn,
    in_q=q_proc2_to_write,
    out_q=None,
)

# --------------------------------------------------------------------------- #
# Run the pipeline
# --------------------------------------------------------------------------- #
manager = PipelineManager([reader, proc1, proc2, writer])
manager.start()
manager.join()

print("\nDemo finished ✓")
