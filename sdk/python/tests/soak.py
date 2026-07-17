"""Opt-in sustained producer soak using one long-lived SDK connection."""

from __future__ import annotations

import json
import os
import time
import tracemalloc

from harness import Server, unique_name

from bunqueue import Queue


def main() -> None:
    seconds = max(1, int(os.getenv("BUNQUEUE_SDK_SOAK_SECONDS", "300")))
    batch_size = max(1, int(os.getenv("BUNQUEUE_SDK_SOAK_BATCH", "100")))
    server = Server().start()
    queue = Queue(unique_name("python-soak"), port=server.port)
    deadline = time.monotonic() + seconds
    iterations = 0
    jobs = 0
    tracemalloc.start()
    try:
        while time.monotonic() < deadline:
            ids = queue.add_bulk(
                [
                    {
                        "name": "soak",
                        "data": {"iteration": iterations, "index": index},
                    }
                    for index in range(batch_size)
                ]
            )
            assert len(ids) == batch_size
            assert queue.count() == batch_size
            assert queue.get_job(ids[0]) is not None
            assert queue.get_job(ids[-1]) is not None
            queue.obliterate()
            iterations += 1
            jobs += len(ids)
        current, peak = tracemalloc.get_traced_memory()
        print(
            json.dumps(
                {
                    "profile": "python-soak",
                    "seconds": seconds,
                    "batchSize": batch_size,
                    "iterations": iterations,
                    "jobs": jobs,
                    "tracedCurrent": current,
                    "tracedPeak": peak,
                }
            )
        )
    finally:
        tracemalloc.stop()
        queue.close()
        server.stop()


if __name__ == "__main__":
    main()
