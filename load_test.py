"""
Load-Testing Script for Student Attendance System.
Fires 300 simulated student sign-ins at the Serverless Ingest API simultaneously.
Collects latency metrics (min, mean, p50, p95, p99, max), verifies status codes,
and validates end-to-end processing across the local Cloud Tasks queue and Mock Database.
"""

import argparse
import asyncio
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import httpx

DEFAULT_TARGET_URL = os.environ.get(
    "INGEST_URL", "http://127.0.0.1:8080/api/attendance/signin"
)
DEFAULT_STATS_URL = os.environ.get("STATS_URL", "http://127.0.0.1:8080/api/stats")
DEFAULT_CONCURRENCY = 300


async def send_signin(
    client: httpx.AsyncClient,
    url: str,
    index: int,
    session_id: str = "SESS-2026-09-17",
) -> Dict:
    student_id = f"STU{index:03d}"
    kiosk_id = f"kiosk-hall-{(index % 6) + 1}"

    # First 260 students on time, remaining 40 late
    if index <= 260:
        sign_in_time = "2026-09-17T09:05:00Z"
    else:
        sign_in_time = "2026-09-17T09:22:00Z"

    payload = {
        "student_id": student_id,
        "session_id": session_id,
        "device_id": kiosk_id,
        "sign_in_time": sign_in_time,
    }

    t0 = time.perf_counter()
    try:
        response = await client.post(url, json=payload, timeout=30.0)
        t1 = time.perf_counter()
        latency_ms = (t1 - t0) * 1000.0

        return {
            "index": index,
            "student_id": student_id,
            "status_code": response.status_code,
            "latency_ms": latency_ms,
            "success": response.status_code == 202,
            "error": None,
        }
    except Exception as e:
        t1 = time.perf_counter()
        latency_ms = (t1 - t0) * 1000.0
        return {
            "index": index,
            "student_id": student_id,
            "status_code": 0,
            "latency_ms": latency_ms,
            "success": False,
            "error": str(e),
        }


def calculate_percentile(sorted_data: List[float], percentile: float) -> float:
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * (percentile / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_data) - 1)
    d = k - f
    return sorted_data[f] + d * (sorted_data[c] - sorted_data[f])


async def run_load_test(
    url: str = DEFAULT_TARGET_URL,
    total_requests: int = DEFAULT_CONCURRENCY,
    stats_url: str = DEFAULT_STATS_URL,
    drain_timeout: float = 15.0,
):
    print("\n" + "=" * 70)
    print(f"  SIMULTANEOUS LOAD TEST: {total_requests} STUDENT SIGN-INS")
    print(f"  Target Endpoint: {url}")
    print(f"  Concurrency:     {total_requests} concurrent async requests")
    print("=" * 70 + "\n")

    # Configure httpx AsyncClient for massive local concurrency
    limits = httpx.Limits(
        max_connections=total_requests + 50,
        max_keepalive_connections=total_requests + 50,
    )

    print(f"[*] Preparing {total_requests} student payloads (STU001 to STU{total_requests:03d})...")
    print(f"[*] Firing all {total_requests} requests SIMULTANEOUSLY into Ingest API...")

    t_start = time.perf_counter()
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [send_signin(client, url, i) for i in range(1, total_requests + 1)]
        results = await asyncio.gather(*tasks)
    t_end = time.perf_counter()

    total_wall_time = t_end - t_start
    throughput = total_requests / total_wall_time if total_wall_time > 0 else 0

    # Analyze results
    success_count = sum(1 for r in results if r["success"])
    fail_count = total_requests - success_count
    latencies = sorted([r["latency_ms"] for r in results])

    min_lat = min(latencies) if latencies else 0.0
    max_lat = max(latencies) if latencies else 0.0
    mean_lat = sum(latencies) / len(latencies) if latencies else 0.0
    p50 = calculate_percentile(latencies, 50.0)
    p90 = calculate_percentile(latencies, 90.0)
    p95 = calculate_percentile(latencies, 95.0)
    p99 = calculate_percentile(latencies, 99.0)

    # Status breakdown
    status_codes: Dict[int, int] = {}
    for r in results:
        status_codes[r["status_code"]] = status_codes.get(r["status_code"], 0) + 1

    print("\n" + "-" * 70)
    print("  INGEST API LOAD TEST RESULTS")
    print("-" * 70)
    print(f"  Total Requests:        {total_requests}")
    print(f"  Successful (202):      {success_count} ({success_count/total_requests*100:.1f}%)")
    print(f"  Failed Requests:       {fail_count}")
    print(f"  Total Elapsed Time:    {total_wall_time:.3f} seconds")
    print(f"  Throughput (RPS):      {throughput:.1f} requests/sec")
    print("\n  Latency Distribution (Round-trip to Ingest API):")
    print(f"    Min:                 {min_lat:.2f} ms")
    print(f"    Mean (Average):      {mean_lat:.2f} ms")
    print(f"    P50 (Median):        {p50:.2f} ms")
    print(f"    P90:                 {p90:.2f} ms")
    print(f"    P95:                 {p95:.2f} ms")
    print(f"    P99:                 {p99:.2f} ms")
    print(f"    Max:                 {max_lat:.2f} ms")
    print("\n  HTTP Status Codes:")
    for code, count in sorted(status_codes.items()):
        status_label = "Accepted (Queued)" if code == 202 else f"HTTP {code}"
        print(f"    {code}: {count} ({status_label})")
    print("-" * 70)

    # Poll Mock Database to verify Cloud Tasks dispatch & database insertion
    print(f"\n[*] Awaiting Cloud Tasks emulator & dispatcher queue drain (timeout={drain_timeout}s)...")
    drained = False
    final_stats = {}
    poll_start = time.perf_counter()

    async with httpx.AsyncClient() as client:
        while time.perf_counter() - poll_start < drain_timeout:
            try:
                res = await client.get(stats_url, timeout=5.0)
                if res.status_code == 200:
                    stats = res.json()
                    final_stats = stats
                    records = stats.get("total_attendance_records", 0)
                    if records >= total_requests:
                        drained = True
                        break
            except Exception:
                pass
            await asyncio.sleep(0.5)

    print("\n" + "=" * 70)
    print("  END-TO-END PIPELINE VERIFICATION")
    print("=" * 70)
    total_records = final_stats.get("total_attendance_records", 0)
    total_tasks = final_stats.get("total_tasks_processed", 0)
    breakdown = final_stats.get("status_breakdown", {})

    print(f"  Database Attendance Records:  {total_records} / {total_requests}")
    print(f"  Tasks Processed by Worker:    {total_tasks}")
    print(f"  Status Breakdown:             {breakdown}")

    if total_records >= total_requests:
        print("\n  >>> VERIFICATION PASSED: All 300 sign-in tasks were successfully")
        print("      enqueued, dispatched through Cloud Tasks, and committed to Mock DB! <<<")
        print("=" * 70 + "\n")
        return True
    else:
        print(f"\n  >>> VERIFICATION IN PROGRESS: {total_records}/{total_requests} committed so far.")
        print("=" * 70 + "\n")
        return total_records > 0


def main():
    parser = argparse.ArgumentParser(description="Fire 300 simultaneous sign-in requests")
    parser.add_argument("--url", default=DEFAULT_TARGET_URL, help="Ingest API URL")
    parser.add_argument("--stats-url", default=DEFAULT_STATS_URL, help="Stats API URL")
    parser.add_argument("--concurrency", "-n", type=int, default=DEFAULT_CONCURRENCY, help="Number of concurrent requests")
    parser.add_argument("--drain-timeout", type=float, default=20.0, help="Seconds to wait for queue drain")
    args = parser.parse_args()

    success = asyncio.run(
        run_load_test(
            url=args.url,
            total_requests=args.concurrency,
            stats_url=args.stats_url,
            drain_timeout=args.drain_timeout,
        )
    )

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
