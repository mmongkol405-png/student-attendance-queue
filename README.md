# Student Attendance System - Local Testing Ground

A high-performance local simulation and testing ground for a serverless student attendance architecture powered by Google Cloud Tasks. It isolates all messaging, ingestion, queue processing, and persistence to your local machine without connecting to Google Cloud production.

## System Architecture

```
                                          +-------------------------+
                                          |                         |
               +------------------------> |   Mock Database (WAL)   |
               | (Record Attendance)      |     attendance.db       |
               |                          |                         |
               |                          +-------------------------+
               |                                       ^
               |                                       | (Verify enrollment)
               |                                       |
+--------------+----------+               +------------+------------+
|  Queue Dispatcher HTTP  | <------------ |   Cloud Tasks Emulator  |
|       Port 8081         | (POST Task)   |     Port 9090 (gRPC)    |
+-------------------------+               +-------------------------+
                                                       ^
                                                       | (CreateTask gRPC)
                                          +------------+------------+
                                          |  Serverless Ingest API  |
                                          |       Port 8080         |
                                          +-------------------------+
                                                       ^
                                                       | (POST /signin)
                                          +------------+------------+
                                          |    Load Testing Script  |
                                          |      (300 requests)     |
                                          +-------------------------+
```

---

## Components

1. **Mock Database (`db.py`)**:
   - SQLite backed store with Write-Ahead Logging (`WAL`) mode for high-concurrency read/write operations.
   - Schema: `students`, `courses`, `sessions`, `attendance_records`, `task_logs`.
   - Seeded with 350 active students (`STU001` to `STU350`) and course sessions.
   - Handles idempotency: duplicate sign-ins for the same student and session are identified and flagged.

2. **Cloud Tasks Emulator (`emulator_runner.py`)**:
   - Uses `gcloud-tasks-emulator` (v0.6.2) to run a local gRPC server on port `9090`.
   - Configures default queue: `projects/local-dev/locations/us-central1/queues/student-attendance-queue`.
   - Dispatches tasks asynchronously via HTTP POST to the Queue Dispatcher.

3. **Serverless Ingest API (`ingest_api.py`)**:
   - Simulates a serverless function / API Gateway endpoint on port `8080`.
   - Connects to the local Cloud Tasks emulator using the official `google-cloud-tasks` client.
   - Enqueues sign-in requests and returns immediate `202 Accepted` with task references for ultra-fast response times.
   - Hosts the real-time interactive testing dashboard at `http://127.0.0.1:8080/`.

4. **Queue Dispatcher Worker (`dispatcher.py`)**:
   - HTTP worker service running on port `8081`.
   - Receives dispatched tasks (`POST /dispatch/attendance`).
   - Verifies student registration, checks start times (`PRESENT` vs `LATE`), and writes records to `attendance.db`.

5. **Load-Testing Script (`load_test.py`)**:
   - Asynchronously fires 300 simultaneous student sign-ins using `asyncio` and `httpx`.
   - Collects latency percentiles (Min, Mean, P50, P90, P95, P99, Max) and throughput (RPS).
   - Monitors queue draining and verifies database persistence.

---

## Quick Start

### 1. Start the Full Testing Ground
Run all services simultaneously with a single command:

```powershell
python run_all.py
```

You will see:
- Dashboard: `http://127.0.0.1:8080/`
- Ingest API: `POST http://127.0.0.1:8080/api/attendance/signin`
- Cloud Tasks gRPC: `127.0.0.1:9090`
- Queue Dispatcher: `http://127.0.0.1:8081/dispatch/attendance`

### 2. Run Simulations & Real-Time Scenarios
In a separate terminal (or pass flags to `run_all.py`):

**2-Hour Real-Time Simulation Engine (with speed dilation):**
```powershell
# Run in terminal with live progress bar (default 60x speed = 2 hours in 2 minutes)
python simulation_engine.py --speed 60.0

# Run in exact real-time (1x speed = 2 wall-clock hours)
python simulation_engine.py --speed 1.0
```

**Recurring Cron Job & Automated Server Wake-Up:**
The cron job automatically verifies the server is online and awake before monitoring or triggering actions. If targeting a cold-sleeping server (e.g. Render, Cloud Run, Heroku) it waits for it to wake up; if local services are offline, it can auto-spawn `run_all.py`:
```powershell
# Schedule to fire at exactly 5:00 PM real time (with countdown, keep-alive heartbeat, and T-30s pre-warming):
python cron_job.py --at 17:00

# Quick test with 15s relative countdown:
python cron_job.py --in 15s

# Ping and wake up the server immediately, report status, and exit:
python cron_job.py --wake-only

# Target a remote or cloud deployment:
python cron_job.py --base-url https://my-attendance-app.onrender.com --at 17:00

# Continuous telemetry monitoring loop:
python cron_job.py --interval 10
```

**Uniform Load Test (300 Simultaneous Requests):**
```powershell
python load_test.py --concurrency 300
```

### 3. Interactive Web Dashboard
Open [http://127.0.0.1:8080/](http://127.0.0.1:8080/) in your browser to view:
- **Live 2-Hour Session Clock**: Digital timer (`00:42:15 / 02:00:00`), campus time, and active lecture phase.
- **Cloud Tasks Queue Buffer**: Real-time gauge of tasks buffered in the local emulator waiting to be processed.
- **Live Scan Queue & State Transitions Feed**: Shows animated student arrival transitions:
  `[T+00:04:04 • 08:49:04 AM] STU017 (scanner-north-2) -> QUEUED -> PRESENT`
- **Speed Dilation Selector**: One-click toggle between `1x (2h Real)`, `10x (12m)`, `30x (4m)`, `60x (2m)`, and `120x (1m)`.
- **Session Controls**: One-click buttons to Start, Pause, Resume, and Stop the 2-hour simulation.
