"""
Cloud Tasks Emulator Runner.
Wraps and manages the local gcloud-tasks-emulator server for the testing ground.
Configures default attendance queues and routes dispatched tasks to the dispatcher worker.
"""

import argparse
import logging
import os
import signal
import sys
import threading
import time
from concurrent import futures
from typing import List, Optional

import grpc
from gcloud_tasks_emulator.cloudtasks_pb2_grpc import add_CloudTasksServicer_to_server
from gcloud_tasks_emulator.server import APIThread, Greeter, Processor, QueueState, Server
from google.cloud.tasks_v2 import Queue

# Configuration
DEFAULT_HOST = os.environ.get("TASKS_EMULATOR_HOST", "127.0.0.1")
DEFAULT_PORT = int(os.environ.get("TASKS_EMULATOR_PORT", "9090"))
DEFAULT_TARGET_HOST = os.environ.get("DISPATCHER_HOST", "127.0.0.1")
DEFAULT_TARGET_PORT = int(os.environ.get("DISPATCHER_PORT", "8081"))
DEFAULT_QUEUE = os.environ.get(
    "ATTENDANCE_QUEUE_NAME",
    "projects/local-dev/locations/us-central1/queues/student-attendance-queue",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [TasksEmulator] %(message)s",
)
logger = logging.getLogger("emulator_runner")


class HighConcurrencyAPIThread(threading.Thread):
    """API thread with configurable gRPC worker pool to handle concurrent load spikes."""

    def __init__(self, state: QueueState, host: str, port: int, max_workers: int = 32):
        super().__init__(daemon=True)
        self._state = state
        self._host = host
        self._port = port
        self._max_workers = max_workers
        self._is_running = threading.Event()
        self._grpc_server = None

    def run(self):
        self._is_running.set()
        self._grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=self._max_workers))
        add_CloudTasksServicer_to_server(Greeter(self._state), self._grpc_server)

        interface = f"{self._host}:{self._port}"
        self._grpc_server.add_insecure_port(interface)
        logger.info(f"gRPC Cloud Tasks Emulator listening at {interface} (workers={self._max_workers})")
        self._grpc_server.start()

        while self._is_running.is_set():
            time.sleep(0.1)

    def stop(self):
        self._is_running.clear()
        if self._grpc_server:
            self._grpc_server.stop(grace=0)
            logger.info("gRPC Cloud Tasks Emulator stopped.")


class ConcurrentProcessor(threading.Thread):
    """
    Queue processor with concurrent task dispatching to maximize local test throughput.
    """

    def __init__(self, state: QueueState, max_dispatch_workers: int = 16):
        super().__init__(daemon=True)
        self._state = state
        self._max_dispatch_workers = max_dispatch_workers
        self._dispatch_pool = futures.ThreadPoolExecutor(max_workers=max_dispatch_workers)
        self._is_running = threading.Event()
        self._known_queues = set()
        self._queue_threads = {}

    def run(self):
        self._is_running.set()
        logger.info(f"Task processor started (dispatch_workers={self._max_dispatch_workers})")
        while self._is_running.is_set():
            for queue in self._state.queue_names():
                self._ensure_queue_worker(queue)
            time.sleep(0.05)

    def _ensure_queue_worker(self, queue_name: str):
        if queue_name not in self._known_queues:
            self._known_queues.add(queue_name)
            t = threading.Thread(target=self._process_queue, args=[queue_name], daemon=True)
            self._queue_threads[queue_name] = t
            t.start()

    def _process_queue(self, queue: str):
        while self._is_running.is_set():
            if queue not in self._state._queues or queue not in self._state._queue_tasks:
                break

            if self._state.queue(queue).state == Queue.State.RUNNING:
                tasks_snapshot = self._state._queue_tasks[queue][:]
                if tasks_snapshot:
                    futures_list = []
                    for task in tasks_snapshot:
                        # Submit task via thread pool for concurrent dispatch
                        fut = self._dispatch_pool.submit(self._safe_submit_task, task.name)
                        futures_list.append(fut)
                    # Wait briefly for batch completion
                    futures.wait(futures_list, timeout=1.0)
            time.sleep(0.05)

    def _safe_submit_task(self, task_name: str):
        try:
            self._state.submit_task(task_name)
        except Exception as e:
            logger.debug(f"Task submission info for {task_name}: {e}")

    def stop(self):
        self._is_running.clear()
        self._dispatch_pool.shutdown(wait=False)
        logger.info("Task processor stopped.")


class EmulatorManager:
    """Manages the full lifecycle of the Cloud Tasks emulator instance."""

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        target_host: str = DEFAULT_TARGET_HOST,
        target_port: int = DEFAULT_TARGET_PORT,
        default_queue: str = DEFAULT_QUEUE,
    ):
        self.host = host
        self.port = port
        self.target_host = target_host
        self.target_port = target_port
        self.default_queue = default_queue

        self.state = QueueState(self.target_host, self.target_port, max_retries=3)
        self.api_thread = HighConcurrencyAPIThread(self.state, self.host, self.port)
        self.processor = ConcurrentProcessor(self.state)

        # Initialize default queue
        parent = self.default_queue.rsplit("/", 2)[0]
        self.state.create_queue(parent, Queue(name=self.default_queue))
        logger.info(f"Initialized default queue: {self.default_queue}")

    def start(self):
        logger.info(f"Starting gcloud-tasks-emulator on {self.host}:{self.port} -> target {self.target_host}:{self.target_port}")
        self.api_thread.start()
        self.processor.start()
        logger.info("Cloud Tasks Emulator is ACTIVE and ready to accept tasks.")

    def stop(self):
        logger.info("Shutting down Cloud Tasks Emulator...")
        self.processor.stop()
        self.api_thread.stop()
        logger.info("Cloud Tasks Emulator shut down cleanly.")

    def get_queue_depth(self) -> int:
        tasks = self.state._queue_tasks.get(self.default_queue, [])
        return len(tasks)


def run_standalone():
    parser = argparse.ArgumentParser(description="Run the local Cloud Tasks Emulator")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Host to bind emulator gRPC server")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port to bind emulator gRPC server")
    parser.add_argument("--target-host", default=DEFAULT_TARGET_HOST, help="Dispatcher worker target host")
    parser.add_argument("--target-port", type=int, default=DEFAULT_TARGET_PORT, help="Dispatcher worker target port")
    parser.add_argument("--queue", default=DEFAULT_QUEUE, help="Default queue name")
    args = parser.parse_args()

    emulator = EmulatorManager(
        host=args.host,
        port=args.port,
        target_host=args.target_host,
        target_port=args.target_port,
        default_queue=args.queue,
    )
    emulator.start()

    print("\n" + "=" * 60)
    print(f"  GCLOUD TASKS EMULATOR RUNNING")
    print(f"  gRPC Endpoint:  {args.host}:{args.port}")
    print(f"  Target Worker:  http://{args.target_host}:{args.target_port}")
    print(f"  Active Queue:   {args.queue}")
    print("=" * 60 + "\n")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        emulator.stop()
        sys.exit(0)


if __name__ == "__main__":
    run_standalone()
