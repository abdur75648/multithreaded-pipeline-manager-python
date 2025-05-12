# Robust Multithreaded Pipeline Manager in Python

A minimal, deadlock-proof scaffold you can copy-paste into any data-processing or model-inference project.

---

## Table of Contents

1. [Why pipelines?](#why-pipelines)
2. [Core design concepts](#core-design-concepts)
3. [Repository tour](#repository-tour)
4. [Quick-start (run the demo)](#quick-start-run-the-demo)
5. [How the pipeline works – code walkthrough](#how-the-pipeline-works--code-walkthrough)
   5.1 [Stage anatomy](#51-pipelinestage-anatomy)
   5.2 [Safe queue I/O with `robust_put`](#52-safe-queue-io-with-robust_put)
   5.3 [Manager orchestration](#53-manager-orchestration)
6. [Extending to real workloads](#extending-to-real-workloads)
7. [Troubleshooting cheatsheet](#troubleshooting-cheatsheet)
8. [License](#license)

---

## Why pipelines?

Real-world ML and multimedia applications rarely do everything in one step.
Typical flow:

```
decode → preprocess → model_inference → postprocess → encode/write
```

Running those steps **sequentially** means idle CPU/GPU time and wasted I/O latency.
Running them **in separate threads with small bounded queues** lets every stage work concurrently while still limiting memory and providing back-pressure.

The tricky part: **robust shutdown.**
If a downstream consumer crashes while an upstream producer blocks on `Queue.put()`, you hit the classic bounded-queue deadlock.
This repo shows a bullet-proof pattern that prevents that failure mode and gracefully shuts down on errors **or Ctrl-C**.

---

## Core design concepts

| Concept                           | Take-away                                                                                                                                                                      |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Threading vs. multiprocessing** | Threads are ideal when stages spend time in C/CUDA libraries that release Python’s GIL (e.g., OpenCV, PyTorch GPU). For heavy pure-Python CPU work, you’d switch to processes. |
| **Bounded queues**                | `Queue(maxsize=N)` keeps RAM in check *and* provides built-in back-pressure.                                                                                                   |
| **`robust_put()`**                | Wraps a blocking `Queue.put()` in a timeout loop so the caller can notice a global `stop_event` and bail out instead of hanging forever.                                       |
| **`SENTINEL` object**             | A unique token passed through the pipeline to say “no more data.” Each stage forwards it exactly once, allowing every thread to exit naturally.                                |
| **`threading.Event()`**           | A single flag shared by all threads. Any fatal exception sets the flag, triggering an orderly shutdown everywhere else.                                                        |
| **try / except / finally**        | Every stage catches its own exceptions, empties / unblocks its output queue, forwards the sentinel, and *then* dies—ensuring no one upstream is left hanging.                  |
| **Modular stages**                | Each stage owns only its logic; the infrastructure code lives in `PipelineStage`, so plugging in new stages is trivial and unit-testable.                                      |

---

## Repository tour

```
multithreaded-pipeline-manager-python/
│
├── demo.py                      # One-file runnable showcase
│
├── pipeline_manager/            # Tiny helper library (≈200 LOC)
│   ├── __init__.py              # Re-exports public symbols
│   ├── utils.py                 # SENTINEL, robust_put, logger
│   ├── stage.py                 # PipelineStage (thread implementation)
│   └── manager.py               # PipelineManager (start/stop wrapper)
└── README.md                    # ← you are here
```

*No packaging or installation ceremony required—just clone and run.*

---

## Quick-start (run the demo)

```bash
git clone https://github.com/abdur75648/multithreaded-pipeline-manager-python.git
cd multithreaded-pipeline-manager-python
python3 demo.py
```

You’ll see four threads (`Reader`, `UpperCase`, `Reverse`, `Writer`) working in parallel and a clean, coordinated shutdown when either **(a)** the reader finishes after the chosen duration **(b)** you hit *Ctrl +C* or **(c)** a simulated processor error occurs.

---

## How the pipeline works – code walkthrough

### High-level diagram

```
           Queue(maxsize=N)     Queue(maxsize=N)     Queue(maxsize=N)
Reader ─────────► Proc#1 ─────────► Proc#2 ─────────► Writer
  │                │                │                  │
  │  generates     │ transforms     │ transforms       │ prints /
  └─ SENTINEL──────┴─ SENTINEL──────┴─ SENTINEL────────┴  finalises
```

Each arrow is a bounded queue; each box is a `PipelineStage` thread.
A single `stop_event` and a unique `SENTINEL` coordinate shutdown.

### 5.1 `PipelineStage` anatomy

```python
stage = PipelineStage(
    name="UpperCaseStage",
    stop_event=stop_event,
    process_fn=upper_case_fn,  # your business logic here
    in_q=q_read_to_proc1,
    out_q=q_proc1_to_proc2,
)
stage.start()
```

`PipelineStage` handles **all** the boilerplate:

* timed `Queue.get()` ➜ checks `stop_event`
* calls your `process_fn`
* uses `robust_put()` to push to `out_q`
* catches any exception, sets `stop_event`, drains `out_q`, sends sentinel, logs, exits

You write only the tiny `process_fn`:

```python
def upper_case_fn(text: str) -> str:
    return text.upper()
```

Return `SENTINEL` if the stage itself decides the stream is finished. Return `None` to “drop” an item.

### 5.2 Safe queue I/O with `robust_put`

```python
from queue import Full
from threading import Event
def robust_put(q, item, stop_event: Event, timeout=0.1) -> bool:
    while not stop_event.is_set():
        try:
            q.put(item, timeout=timeout)  # may block if full
            return True
        except Full:
            continue           # queue full → retry / check flag
    return False               # gave up because stop_event was set
```

Without this helper, an upstream producer would block forever if the queue is full and the consumer thread has died.

### 5.3 Manager orchestration

```python
manager = PipelineManager([reader, proc1, proc2, writer])
manager.start()  # starts all threads + installs SIGINT handler
manager.join()   # waits for everything to finish (with timeouts)
```

`PipelineManager` is only convenience; you can `.start()` / `.join()` the stages yourself if you prefer.

---

## Extending to real workloads

* **Video or audio streams:**
  Replace `reader_fn` with a frame grabber (`cv2.VideoCapture`), `proc_fn` with your ML model (`torch.no_grad()`), `writer_fn` with encoder output.

* **Batching:**
  Accumulate a list in `process_fn` and emit a batch; upstream/downstream code is unaffected.

* **Multiprocessing:**
  You can swap `threading.Thread` for `multiprocessing.Process` inside `PipelineStage` if your work is CPU-bound (keep the queue/ sentinel logic identical).

* **Metrics / logging:**
  Hook Prometheus counters in `process_fn` or extend `PipelineStage` with call-backs—no architectural changes needed.

---

## Troubleshooting cheatsheet

| Symptom                               | Likely cause                                                    | Fix                                                                                                 |
| ------------------------------------- | --------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| **Producer thread hangs on `.put()`** | Downstream consumer crashed and queue is full                   | Ensure all puts go through `robust_put()`; verify consumer always forwards `SENTINEL` in `finally`. |
| **Writer never receives sentinel**    | Some intermediate stage swallowed or failed to forward sentinel | Check that every `finally` block does `out_q.put(SENTINEL)` even on error.                          |
| **Memory shoots up**                  | Unbounded queues or large back-pressure gap                     | Set `maxsize` small (1–8) and verify upstream rate ≤ downstream rate.                               |
| **Ctrl-C doesn’t exit**               | Threads not checking `stop_event`                               | Make sure all `Queue.get()` / `put()` are timeout-based and loops break when `stop_event.is_set()`. |

---

## License

[MIT](LICENSE) – free for personal or commercial use.
PRs, issues, and suggestions are welcome!
