"""
Testing Ground Orchestrator.
Spawns the local mock database, gcloud-tasks-emulator, queue dispatcher, and serverless ingest API
in a single unified process runner with coordinated startup and graceful shutdown.
"""

import argparse
import logging
import os
import signal
import subprocess
import sys
import threading
import time

import db
from emulator_runner import EmulatorManager
from dispatcher import app as dispatcher_app
from ingest_api import app as ingest_app, set_shutdown_hook

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [TestingGround] %(message)s",
)
logger = logging.getLogger("orchestrator")


def run_flask_thread(app, host, port, name):
    from werkzeug.serving import make_server

    server = make_server(host, port, app, threaded=True)
    logger.info(f"{name} running at http://{host}:{port}")

    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


def main():
    parser = argparse.ArgumentParser(description="Start Student Attendance Local Testing Ground")
    parser.add_argument("--emulator-port", type=int, default=9090, help="Cloud Tasks Emulator gRPC port")
    parser.add_argument("--dispatcher-port", type=int, default=8081, help="Queue Dispatcher port")
    parser.add_argument("--ingest-port", type=int, default=8080, help="Serverless Ingest API port")
    parser.add_argument("--run-load-test", action="store_true", help="Automatically trigger 300 uniform load test after startup")
    parser.add_argument("--simulate-realistic", action="store_true", help="Automatically trigger realistic staggered simulation (present, late, absent, duplicates)")
    parser.add_argument("--exit-after-test", action="store_true", help="Exit automatically after running test/simulation")
    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("      STUDENT ATTENDANCE LOCAL TESTING GROUND")
    print("=" * 70)

    # 1. Initialize & Seed Database
    logger.info("Initializing SQLite Mock Database with WAL mode & 350 seeded students...")
    db.init_db()
    db.seed_data()
    db_stats = db.get_stats()
    logger.info(f"Database ready: {db_stats['total_registered_students']} students enrolled.")

    # 2. Start Cloud Tasks Emulator
    logger.info(f"Starting gcloud-tasks-emulator on port {args.emulator_port}...")
    emulator = EmulatorManager(
        host="127.0.0.1",
        port=args.emulator_port,
        target_host="127.0.0.1",
        target_port=args.dispatcher_port,
    )
    emulator.start()

    # 3. Start Queue Dispatcher HTTP Worker
    logger.info(f"Starting Queue Dispatcher Worker on port {args.dispatcher_port}...")
    dispatcher_server = run_flask_thread(dispatcher_app, "127.0.0.1", args.dispatcher_port, "Queue Dispatcher")

    # 4. Start Serverless Ingest API
    logger.info(f"Starting Serverless Ingest API on port {args.ingest_port}...")
    ingest_server = run_flask_thread(ingest_app, "127.0.0.1", args.ingest_port, "Serverless Ingest API")

    def shutdown_all_services():
        logger.info("Remote shutdown received: stopping all services...")
        try:
            ingest_server.shutdown()
        except Exception:
            pass
        try:
            dispatcher_server.shutdown()
        except Exception:
            pass
        try:
            emulator.stop()
        except Exception:
            pass
        logger.info("All local services stopped cleanly via shutdown hook.")
        os._exit(0)

    set_shutdown_hook(shutdown_all_services)

    time.sleep(1.0)

    print("\n" + "*" * 70)
    print("  ALL SERVICES ARE ONLINE & CONNECTED LOCALLY:")
    print(f"  [1] Interactive Dashboard:      http://127.0.0.1:{args.ingest_port}/")
    print(f"  [2] Serverless Ingest API:     POST http://127.0.0.1:{args.ingest_port}/api/attendance/signin")
    print(f"  [3] Cloud Tasks Emulator:      127.0.0.1:{args.emulator_port} (gRPC)")
    print(f"  [4] Queue Dispatcher Worker:   POST http://127.0.0.1:{args.dispatcher_port}/dispatch/attendance")
    print(f"  [5] Mock Database:             attendance.db (WAL mode)")
    print("*" * 70 + "\n")

    test_passed = True
    if args.run_load_test:
        logger.info("Executing 300 simultaneous sign-in load test...")
        import asyncio
        from load_test import run_load_test

        test_passed = asyncio.run(run_load_test(
            url=f"http://127.0.0.1:{args.ingest_port}/api/attendance/signin",
            total_requests=300,
            stats_url=f"http://127.0.0.1:{args.ingest_port}/api/stats",
        ))

    elif args.simulate_realistic:
        logger.info("Executing realistic attendance simulation (Present, Late, Absent, Duplicates)...")
        import asyncio
        from simulate_realistic_attendance import run_simulation

        stats = asyncio.run(run_simulation(
            fast_mode=True,
            reset_first=True,
            ingest_url=f"http://127.0.0.1:{args.ingest_port}/api/attendance/signin",
            stats_url=f"http://127.0.0.1:{args.ingest_port}/api/stats",
            reconcile_url=f"http://127.0.0.1:{args.ingest_port}/api/attendance/reconcile",
            reset_url=f"http://127.0.0.1:{args.ingest_port}/api/reset",
        ))
        test_passed = bool(stats)

    if (args.run_load_test or args.simulate_realistic) and args.exit_after_test:
        print("\nShutting down services after automated test...")
        ingest_server.shutdown()
        dispatcher_server.shutdown()
        emulator.stop()
        logger.info("Automated testing finished successfully.")
        sys.exit(0 if test_passed else 1)

    print("\nPress Ctrl+C to shut down all local services.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping services...")
        ingest_server.shutdown()
        dispatcher_server.shutdown()
        emulator.stop()
        logger.info("All local services stopped cleanly.")
        sys.exit(0)


if __name__ == "__main__":
    main()
