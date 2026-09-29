"""
Queue Dispatcher Worker.
Receives and processes HTTP tasks dispatched by the local Cloud Tasks emulator.
Validates student attendance and commits records to the mock database.
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, Any

from flask import Flask, jsonify, request
import db

DISPATCHER_HOST = os.environ.get("DISPATCHER_HOST", "127.0.0.1")
DISPATCHER_PORT = int(os.environ.get("DISPATCHER_PORT", "8081"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Dispatcher] %(message)s",
)
logger = logging.getLogger("dispatcher")

app = Flask(__name__)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "HEALTHY",
        "service": "attendance-queue-dispatcher",
        "port": DISPATCHER_PORT,
    }), 200


@app.route("/stats", methods=["GET"])
def stats():
    return jsonify(db.get_stats()), 200


@app.route("/dispatch/attendance", methods=["POST"])
def dispatch_attendance():
    """
    HTTP endpoint targeted by Cloud Tasks emulator.
    Processes attendance check-in task asynchronously.
    """
    # Extract Cloud Tasks headers
    queue_name = request.headers.get("X-CloudTasks-QueueName") or request.headers.get("X-AppEngine-QueueName", "unknown-queue")
    task_name = request.headers.get("X-CloudTasks-TaskName") or request.headers.get("X-AppEngine-TaskName", "unknown-task")
    retry_count_hdr = request.headers.get("X-CloudTasks-TaskRetryCount") or request.headers.get("X-AppEngine-TaskRetryCount", "0")
    try:
        retry_count = int(retry_count_hdr)
    except ValueError:
        retry_count = 0

    try:
        payload = request.get_json(force=True, silent=True)
        if not payload:
            raw_data = request.get_data(as_text=True)
            payload = json.loads(raw_data) if raw_data else {}
    except Exception as e:
        logger.error(f"Malformed JSON payload for task {task_name}: {e}")
        return jsonify({"error": "Invalid JSON payload", "details": str(e)}), 400

    student_id = payload.get("student_id")
    session_id = payload.get("session_id")
    device_id = payload.get("device_id", "simulated-scanner")
    sign_in_time = payload.get("sign_in_time")
    task_id = payload.get("task_id") or task_name

    if not student_id or not session_id or not sign_in_time:
        logger.error(f"Missing required fields in payload: {payload}")
        return jsonify({"error": "Missing required fields (student_id, session_id, sign_in_time)"}), 400

    # Record attendance in database
    result = db.record_attendance(
        task_id=task_id,
        student_id=student_id,
        session_id=session_id,
        device_id=device_id,
        sign_in_time=sign_in_time,
        queue_name=queue_name,
        retry_count=retry_count,
    )

    if not result.get("success"):
        logger.warning(f"Attendance rejected for {student_id}: {result.get('message')}")
        # Note: 200 is returned to acknowledge Cloud Task delivery even if business validation rejected student
        return jsonify(result), 200

    status = result.get("status")
    student_name = result.get("student_name")
    logger.info(f"Recorded attendance for {student_id} ({student_name}) -> status={status}, task={task_name}")

    return jsonify(result), 200


def run_standalone():
    parser = argparse.ArgumentParser(description="Run Attendance Queue Dispatcher")
    parser.add_argument("--host", default=DISPATCHER_HOST, help="Dispatcher bind host")
    parser.add_argument("--port", type=int, default=DISPATCHER_PORT, help="Dispatcher bind port")
    args = parser.parse_args()

    # Ensure DB is ready
    db.init_db()
    db.seed_data()

    print("\n" + "=" * 60)
    print(f"  ATTENDANCE QUEUE DISPATCHER WORKER")
    print(f"  Listening on: http://{args.host}:{args.port}/dispatch/attendance")
    print("=" * 60 + "\n")

    # Flask threaded server handles concurrent incoming tasks from the emulator
    app.run(host=args.host, port=args.port, threaded=True, debug=False)


if __name__ == "__main__":
    run_standalone()
