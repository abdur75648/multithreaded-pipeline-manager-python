"""
Generic, fault-tolerant pipeline stage implemented as a Thread.
"""

from __future__ import annotations

import traceback
from queue import Empty, Full, Queue
from threading import Event, Thread
from typing import Callable, Optional, Any

from .utils import SENTINEL, logger, robust_put


class PipelineStage(Thread):
    """
    A single producer/consumer stage.

    The stage calls process_fn on every item it receives from in_q
    (or on `None` for a pure source).  The return value (unless it is
    `SENTINEL` or `None`) is forwarded to out_q.

    Parameters
    ----------
    name : str
        Thread name (appears in logs).
    stop_event : Event
        Shared shutdown flag set by *any* stage that encounters an error.
    process_fn : Callable[[Any], Any]
        Business logic of this stage.
    in_q : Queue | None, default None
        Upstream queue.  Use `None` to mark a pure source stage.
    out_q : Queue | None, default None
        Downstream queue.  Use `None` to mark a sink stage.
    timeout : float, default 0.1
        Seconds for each blocking Queue operation before re-checking
        *stop_event*.
    daemon : bool, default True
        Keep threads as daemons so a crashed main program still exits.
    """

    def __init__(
        self,
        *,
        name: str,
        stop_event: Event,
        process_fn: Callable[[Any], Any],
        in_q: Optional[Queue] = None,
        out_q: Optional[Queue] = None,
        timeout: float = 0.1,
        daemon: bool = True,
    ) -> None:
        super().__init__(name=name, daemon=daemon)
        self.in_q = in_q
        self.out_q = out_q
        self.stop_event = stop_event
        self.process_fn = process_fn
        self.timeout = timeout
        self._success = True

    # --------------------------------------------------------------------- #
    # Internal helpers
    # --------------------------------------------------------------------- #
    def _robust_get(self) -> Optional[Any]:
        """Interruptible Queue.get(); returns None if shutdown requested."""
        if self.in_q is None:
            return None  # source stage
        while not self.stop_event.is_set():
            try:
                return self.in_q.get(timeout=self.timeout)
            except Empty:
                continue
        return None

    # --------------------------------------------------------------------- #
    # Main run-loop
    # --------------------------------------------------------------------- #
    def run(self) -> None:  # noqa: C901  (complexity ok for single point)
        try:
            logger.info("started.")
            while not self.stop_event.is_set():
                item: Any

                # ----------------------------------------------------------------
                # 1. Fetch
                # ----------------------------------------------------------------
                if self.in_q is None:
                    # Source stage calls process_fn to *generate* data.
                    item = self.process_fn(None)
                else:
                    item = self._robust_get()
                    if item is None:  # shutdown
                        break

                # Sentinel means upstream finished – pass it downstream & quit.
                if item is SENTINEL:
                    break

                # ----------------------------------------------------------------
                # 2. Process
                # ----------------------------------------------------------------
                try:
                    result = self.process_fn(item)
                except Exception:
                    logger.error("exception in process_fn", exc_info=True)
                    raise

                # If process_fn wants to end the stream early
                if result is SENTINEL:
                    break
                if result is None:
                    # Some processors legitimately yield no output
                    continue

                # ----------------------------------------------------------------
                # 3. Push downstream (if any)
                # ----------------------------------------------------------------
                if self.out_q is not None:
                    if not robust_put(self.out_q, result, self.stop_event, self.timeout):
                        break

                # Tell upstream we’re done with the task
                if self.in_q is not None:
                    self.in_q.task_done()

        except Exception:  # ← any unhandled error
            self._success = False
            self.stop_event.set()
            logger.error("fatal error; propagating shutdown.", exc_info=True)

        finally:
            # Notify downstream even on error or manual stop.
            if self.out_q is not None:
                try:
                    # If we failed, drain at least one slot before pushing sentinel
                    if not self._success:
                        try:
                            self.out_q.get_nowait()
                        except Empty:
                            pass
                    self.out_q.put_nowait(SENTINEL)
                except Full:
                    # Downstream jammed – best effort
                    pass
            logger.info("terminated (%s).", "OK" if self._success else "ERROR")
