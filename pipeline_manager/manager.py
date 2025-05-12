"""
Tiny orchestration helper - Not strictly required, but convenient.
"""

from __future__ import annotations

import signal
import time
from threading import Event
from typing import Sequence

from .stage import PipelineStage
from .utils import logger


class PipelineManager:
    """
    Start, monitor, and stop a collection of PipelineStage objects.

    Usage
    -----
    >>> mgr = PipelineManager(stages)
    >>> mgr.start()
    >>> mgr.join()
    """

    def __init__(self, stages: Sequence[PipelineStage]) -> None:
        self.stop_event: Event = stages[0].stop_event if stages else Event()
        self.stages = list(stages)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def start(self) -> None:
        logger.info("Starting %d stages…", len(self.stages))
        for t in self.stages:
            t.start()

        # Optional: trap SIGINT just once for the whole pipeline
        signal.signal(signal.SIGINT, self._sigint_handler)

    def join(self, timeout_per_thread: float = 5.0) -> None:
        """
        Wait for every stage to finish (or until timeout).

        If the stop_event is set (due to error or Ctrl-C) we still join with
        a timeout to avoid hanging forever on a misbehaving thread.
        """
        for t in self.stages:
            logger.info("joining %s …", t.name)
            t.join(timeout=timeout_per_thread)
        logger.info("all stages joined – shutdown complete.")

    # ------------------------------------------------------------------ #
    # Signal handler
    # ------------------------------------------------------------------ #
    def _sigint_handler(self, signum, frame):  # noqa: D401
        """Handle Ctrl-C by setting the shared stop_event."""
        logger.warning("SIGINT received – shutting down pipeline.")
        self.stop_event.set()
