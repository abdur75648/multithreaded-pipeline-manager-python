"""
Shared helpers for the pipeline manager.
"""

import logging
import time
from queue import Full, Queue
from threading import Event
from typing import Any

# --------------------------------------------------------------------------- #
# Globally unique sentinel object (identity-based comparison only)
# --------------------------------------------------------------------------- #
SENTINEL: object = object()

# --------------------------------------------------------------------------- #
# Logger setup – kept minimal; library code never configures handlers.
# --------------------------------------------------------------------------- #
logger = logging.getLogger("pipeline_manager")
if not logger.handlers:
    # Users can override this from the outside; fall back to stderr.
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(threadName)s: %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


# --------------------------------------------------------------------------- #
# Robust put:   convert a blocking Queue.put into an *interruptible* loop.
# --------------------------------------------------------------------------- #
def robust_put(
    q: Queue,
    item: Any,
    stop_event: Event,
    timeout: float = 0.1,
) -> bool:
    """
    Try to put *item* onto *q*, looping with a timeout so that we can check
    *stop_event* between attempts. Returns True when the put succeeds,
    False if the surrounding pipeline is shutting down.

    Rationale:
        If `Queue.put(block=True)` waits forever while *q* is full and
        a downstream consumer has crashed, the entire pipeline deadlocks.
        This helper avoids that by polling.

    Parameters
    ----------
    q : Queue
        Destination queue.
    item : Any
        The object to enqueue.
    stop_event : threading.Event
        Shared shutdown flag for the whole pipeline.
    timeout : float, default 0.1
        Seconds to wait per `put()` attempt.

    Returns
    -------
    bool
        True  – put succeeded.  
        False – stop_event became set before the put could complete.
    """
    while not stop_event.is_set():
        try:
            q.put(item, timeout=timeout)
            return True
        except Full:
            # Downstream congestion – brief sleep to avoid busy spin.
            time.sleep(timeout * 0.5)
            continue
    return False
