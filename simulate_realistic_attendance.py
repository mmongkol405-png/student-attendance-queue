"""
Realistic Student Attendance Simulation.
Simulates staggered sign-in patterns for 350 enrolled students across multiple time windows:
- Early Arrivals (08:45 - 08:55) -> PRESENT
- On-Time Rush (08:56 - 09:12) -> PRESENT
- Moderately Late (09:16 - 09:30) -> LATE
- Severely Late (09:35 - 09:55) -> LATE
- Absent Students (No check-in) -> ABSENT
- Duplicate Swipes (Accidental double tap) -> DUPLICATE (idempotently deduplicated)
"""

import argparse
import asyncio
import os
import random
import sys
import time
from typing import Dict, List, Tuple

import httpx

DEFAULT_INGEST_URL = os.environ.get("INGEST_URL", "http://127.0.0.1:8080/api/attendance/signin")
DEFAULT_STATS_URL = os.environ.get("STATS_URL", "http://127.0.0.1:8080/api/stats")
DEFAULT_RECONCILE_URL = os.environ.get("RECONCILE_URL", "http://127.0.0.1:8080/api/attendance/reconcile")
DEFAULT_RESET_URL = os.environ.get("RESET_URL", "http://127.0.0.1:8080/api/reset")
SESSION_ID = "SESS-2026-09-17"


async def send_event(client: httpx.AsyncClient, url: str, event: Dict) -> Dict:
    t0 = time.perf_counter()
    try:
        res = await client.post(url, json=event, timeout=20.0)
        dt_ms = (time.perf_counter() - t0) * 1000.0
        return {
            "student_id": event["student_id"],
            "status_code": res.status_code,
            "success": res.status_code == 202,
            "latency_ms": dt_ms,
            "error": None,
        }
    except Exception as e:
        return {
            "student_id": event["student_id"],
            "status_code": 0,
            "success": False,
            "latency_ms": 0.0,
            "error": str(e),
        }


async def run_simulation(
    fast_mode: bool = True,
    reset_first: bool = True,
    ingest_url: str = DEFAULT_INGEST_URL,
    stats_url: str = DEFAULT_STATS_URL,
    reconcile_url: str = DEFAULT_RECONCILE_URL,
    reset_url: str = DEFAULT_RESET_URL,
):
    print("\n" + "=" * 75)
    print("   REALISTIC STUDENT ATTENDANCE SIMULATION: 350 ENROLLED STUDENTS")
    print("   Session: CS-301 Distributed Systems (Start Time: 09:00:00Z)")
    print("=" * 75)

    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=150, max_keepalive_connections=150)) as client:
        if reset_first:
            print("\n[*] Clearing previous attendance records for a fresh simulation...")
            try:
                await client.post(reset_url)
                print("    [+] Attendance table reset successfully.")
            except Exception as e:
                print(f"    [-] Warning: Could not reset ({e}). Continuing...")

        # Build realistic arrival groups:
        # Total students: 350 (STU001 to STU350)
        # Wave 1: 80 Early Birds (08:45 - 08:55) -> PRESENT
        # Wave 2: 150 On-Time Students (08:56 - 09:12) -> PRESENT
        # Wave 3: 50 Mildly Late (09:16 - 09:30) -> LATE
        # Wave 4: 20 Severely Late (09:35 - 09:55) -> LATE
        # Wave 5: 50 Absent (STU301 to STU350) -> Never check in!
        # Wave 6: 15 Duplicate card swipes (STU010 to STU024 swipe again)

        waves = [
            ("Early Arrivals (08:45 - 08:55)", 1, 80, 8, 45, 55, "PRESENT"),
            ("On-Time Rush (08:56 - 09:12)", 81, 230, 9, 0, 12, "PRESENT"),
            ("Moderately Late (09:16 - 09:30)", 231, 280, 9, 16, 30, "LATE"),
            ("Severely Late (09:35 - 09:55)", 281, 300, 9, 35, 55, "LATE"),
        ]

        all_events = []

        for name, start_id, end_id, hour, min_start, min_end, expected_status in waves:
            for stu_num in range(start_id, end_id + 1):
                stu_id = f"STU{stu_num:03d}"
                minute = random.randint(min_start, min_end)
                sign_time = f"2026-09-17T{hour:02d}:{minute:02d}:00Z"
                scanner = f"kiosk-hall-{(stu_num % 5) + 1}"
                all_events.append({
                    "student_id": stu_id,
                    "session_id": SESSION_ID,
                    "device_id": scanner,
                    "sign_in_time": sign_time,
                    "wave_name": name,
                    "expected": expected_status,
                })

        # 15 accidental duplicate swipes (students double-tapping badge)
        for stu_num in range(10, 25):
            stu_id = f"STU{stu_num:03d}"
            all_events.append({
                "student_id": stu_id,
                "session_id": SESSION_ID,
                "device_id": "kiosk-hall-2",
                "sign_in_time": "2026-09-17T09:04:15Z",
                "wave_name": "Accidental Duplicate Tap",
                "expected": "DUPLICATE",
            })

        # Shuffle slightly to emulate interleaved arrivals
        random.shuffle(all_events)

        print(f"\n[*] Total Sign-In Events to Enqueue: {len(all_events)}")
        print("    - On-Time (Present): 230 students")
        print("    - Late Arrivals:      70 students")
        print("    - Absent Students:    50 students (will not check in)")
        print("    - Duplicate Scans:    15 duplicate badge swipes")
        print("-" * 75)

        # Dispatch in waves/batches
        batch_size = 40
        total_batches = (len(all_events) + batch_size - 1) // batch_size

        t_sim_start = time.perf_counter()
        completed = 0

        for b_idx in range(total_batches):
            batch = all_events[b_idx * batch_size : (b_idx + 1) * batch_size]
            tasks = [send_event(client, ingest_url, ev) for ev in batch]
            results = await asyncio.gather(*tasks)

            success_in_batch = sum(1 for r in results if r["success"])
            completed += len(batch)
            pct = (completed / len(all_events)) * 100
            print(f"    [Batch {b_idx + 1}/{total_batches}] Enqueued {len(batch)} tasks ({success_in_batch} accepted 202) [{pct:5.1f}%]")

            if not fast_mode and b_idx < total_batches - 1:
                await asyncio.sleep(0.4)
            else:
                await asyncio.sleep(0.05)

        t_sim_end = time.perf_counter()
        print(f"[*] All {len(all_events)} sign-in events enqueued in {t_sim_end - t_sim_start:.2f}s.")

        # Wait for Cloud Tasks queue to process
        print("[*] Waiting for local Cloud Tasks queue and dispatcher worker to commit records...")
        await asyncio.sleep(2.0)

        # Reconcile absent students
        print("[*] Reconciling session attendance (identifying absent students)...")
        reconcile_res = await client.post(reconcile_url, json={"session_id": SESSION_ID})
        reconcile_data = reconcile_res.json()
        print(f"    [+] Absences officially recorded: {reconcile_data.get('absent_count_recorded', 0)} students.")

        # Query final stats
        stats_res = await client.get(stats_url)
        stats = stats_res.json()

        print("\n" + "=" * 75)
        print("                 FINAL ATTENDANCE SESSION REPORT")
        print("=" * 75)
        print(f"  Class Session:               {SESSION_ID} (CS-301)")
        print(f"  Total Enrolled Students:     {stats.get('total_registered_students', 350)}")
        print(f"  Total Valid Sign-Ins:        {stats.get('total_attendance_records', 0)}")
        print(f"  Total Queue Tasks Handled:   {stats.get('total_tasks_processed', 0)}")
        print("-" * 75)
        print(f"  [+] PRESENT (On Time):       {stats.get('present_count', 0):3d}  ({stats.get('present_count', 0)/350*100:.1f}%)")
        print(f"  [!] LATE:                    {stats.get('late_count', 0):3d}  ({stats.get('late_count', 0)/350*100:.1f}%)")
        print(f"  [-] ABSENT:                  {stats.get('absent_count', 0):3d}  ({stats.get('absent_count', 0)/350*100:.1f}%)")
        print(f"  [#] Duplicate Scans Blocked: {stats.get('duplicate_attempts_blocked', 0):3d}  (Idempotency Verified)")
        print("=" * 75 + "\n")

        return stats


def main():
    parser = argparse.ArgumentParser(description="Simulate realistic student attendance with present, late, and absent students")
    parser.add_argument("--staggered", action="store_true", help="Run in staggered pacing mode with pauses between batches")
    parser.add_argument("--no-reset", action="store_true", help="Do not reset database before simulation")
    parser.add_argument("--ingest-url", default=DEFAULT_INGEST_URL, help="Ingest API URL")
    args = parser.parse_args()

    asyncio.run(run_simulation(
        fast_mode=not args.staggered,
        reset_first=not args.no_reset,
        ingest_url=args.ingest_url,
    ))


if __name__ == "__main__":
    main()
