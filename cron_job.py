"""
Cron Telemetry & Queue Monitor Job with Automated Server Wake-Up.
Runs as a recurring background cron job to inspect Cloud Tasks queue depth,
monitor attendance state transitions, and audit discrepancies across simulations.
Includes cold-start wake-up, heartbeat keep-alive, and local testing ground auto-spawning.
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
from urllib.parse import urlparse
import httpx

DEFAULT_BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8080")
DEFAULT_STATS_URL = f"{DEFAULT_BASE_URL}/api/stats"
DEFAULT_SIM_STATUS_URL = f"{DEFAULT_BASE_URL}/api/simulation/status"
DEFAULT_QUEUE_STATUS_URL = f"{DEFAULT_BASE_URL}/api/queue/status"
DEFAULT_START_SIM_URL = f"{DEFAULT_BASE_URL}/api/simulation/start"
DEFAULT_WAKE_URL = f"{DEFAULT_BASE_URL}/api/wakeup"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CRON-JOB] %(message)s")
logger = logging.getLogger("cron_job")


def wake_up_server(
    wake_url: str = DEFAULT_WAKE_URL,
    timeout_seconds: float = 45.0,
    retry_interval: float = 2.0,
    auto_start_local: bool = True,
    silent: bool = False,
) -> bool:
    """
    Ensure the target server is awake, warm, and ready to receive requests.
    
    1. Sends a ping request to the wake-up endpoint (/api/wakeup).
    2. If the server is in 'cold sleep' (Render, Cloud Run, Heroku, or free-tier serverless),
       it continuously polls with backoff until HTTP 200 OK is received.
    3. If targeting a local address (127.0.0.1 or localhost) and the server is offline,
       it automatically starts `run_all.py` in the background (or in a new console on Windows).
    """
    parsed = urlparse(wake_url)
    is_local = parsed.hostname in ("127.0.0.1", "localhost", "0.0.0.0")

    # Step 1: Immediate check
    try:
        with httpx.Client(timeout=4.0) as client:
            t0 = time.perf_counter()
            res = client.get(wake_url)
            latency_ms = (time.perf_counter() - t0) * 1000.0
            if res.status_code == 200:
                if not silent:
                    data = {}
                    try:
                        data = res.json()
                    except Exception:
                        pass
                    uptime = data.get("uptime_seconds", 0)
                    wake_cnt = data.get("wakeup_count", 1)
                    print(f"  [SERVER AWAKE] Online and ready ({latency_ms:.1f}ms) | Uptime: {uptime}s | Wakeups: {wake_cnt}")
                return True
    except Exception:
        pass

    # Server did not respond immediately.
    if not silent:
        print(f"\n>> [SERVER ASLEEP OR STARTING] Connecting to {wake_url}...")

    # Step 2: If local and auto_start_local is requested, spawn run_all.py
    if is_local and auto_start_local:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        run_all_path = os.path.join(script_dir, "run_all.py")
        if os.path.exists(run_all_path):
            if not silent:
                print(f"  [LOCAL AUTO-WAKE] Local server is offline. Spawning '{run_all_path}'...")
            try:
                flags = 0
                if sys.platform == "win32":
                    flags = subprocess.CREATE_NEW_CONSOLE | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x200)
                subprocess.Popen(
                    [sys.executable, run_all_path],
                    cwd=script_dir,
                    creationflags=flags,
                )
                if not silent:
                    print("  [LOCAL AUTO-WAKE] Spawned run_all.py! Waiting for local services to boot...")
                time.sleep(1.5)
            except Exception as e:
                try:
                    subprocess.Popen([sys.executable, run_all_path], cwd=script_dir)
                    time.sleep(1.5)
                except Exception as inner_e:
                    logger.warning(f"Could not automatically spawn run_all.py: {inner_e}")

    # Step 3: Polling loop until awake or timeout
    start_time = time.time()
    attempt = 0
    while (time.time() - start_time) < timeout_seconds:
        attempt += 1
        elapsed = time.time() - start_time
        if not silent:
            sys.stdout.write(
                f"\r>> [WAKING SERVER] Ping attempt #{attempt} | Elapsed: {int(elapsed)}s / {int(timeout_seconds)}s...   "
            )
            sys.stdout.flush()

        try:
            with httpx.Client(timeout=4.0) as client:
                res = client.get(wake_url)
                if res.status_code == 200:
                    if not silent:
                        print(f"\n  [SUCCESS] Server successfully AWAKE and responsive after {elapsed:.1f}s!")
                    return True
        except Exception:
            pass

        time.sleep(retry_interval)

    if not silent:
        print(f"\n  [TIMEOUT] Could not wake up server at {wake_url} after {timeout_seconds:.0f}s.")
    return False


def trigger_scheduled_action(action: str = "start-sim", speed: float = 60.0, start_sim_url: str = DEFAULT_START_SIM_URL):
    """Trigger real action on the testing ground when the cron target time arrives."""
    if action == "start-sim":
        print(f"\n>> [DISPATCHING ACTION] Launching live attendance simulation on website (speed: {speed}x)...")
        with httpx.Client(timeout=10.0) as client:
            try:
                res = client.post(start_sim_url, json={"speed": speed, "reset_db": True})
                if res.status_code == 200:
                    parsed = urlparse(start_sim_url)
                    base = f"{parsed.scheme}://{parsed.netloc}"
                    print(f"  [SUCCESS] Simulation activated! Dashboard at {base}/ is now running.")
                else:
                    print(f"  [WARNING] API returned status {res.status_code}: {res.text}")
            except Exception as e:
                print(f"  [ERROR] Could not start simulation: {e}")


def execute_cron_check(
    stats_url: str = DEFAULT_STATS_URL,
    sim_url: str = DEFAULT_SIM_STATUS_URL,
    queue_url: str = DEFAULT_QUEUE_STATUS_URL,
):
    now_local = datetime.now()
    now_local_str = now_local.strftime("%Y-%m-%d %I:%M:%S %p")
    now_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    with httpx.Client(timeout=5.0) as client:
        try:
            stats = client.get(stats_url).json()
            sim = client.get(sim_url).json()
            queue = client.get(queue_url).json()

            print("-" * 72)
            print(f"  [CRON TELEMETRY REPORT] - {now_local_str} ({now_utc_str})")
            print(f"  Session Clock:         {sim.get('sim_clock', '00:00:00')} / 02:00:00 ({sim.get('virtual_time_str', 'N/A')})")
            print(f"  Simulation Phase:      {sim.get('current_phase', 'IDLE')}")
            print(f"  Simulation Speed:      {sim.get('speed', 1.0)}x | Progress: {sim.get('progress_percent', 0)}%")
            print(f"  Cloud Tasks Queue:     {queue.get('queue_depth', 0)} pending tasks in buffer")
            print(f"  State Counts:          PRESENT: {stats.get('present_count', 0)} | "
                  f"LATE: {stats.get('late_count', 0)} | "
                  f"ABSENT: {stats.get('absent_count', 0)} | "
                  f"DUPLICATES: {stats.get('duplicate_attempts_blocked', 0)}")
            print(f"  Total Tasks Processed: {stats.get('total_tasks_processed', 0)}")
            print("-" * 72)

        except Exception as e:
            logger.warning(f"Could not connect to testing ground API ({e}). Is run_all.py running?")


def run_cron_loop(
    interval_seconds: int = 10,
    stats_url: str = DEFAULT_STATS_URL,
    sim_url: str = DEFAULT_SIM_STATUS_URL,
    queue_url: str = DEFAULT_QUEUE_STATUS_URL,
    wake_url: str = DEFAULT_WAKE_URL,
    auto_start_local: bool = True,
):
    print("\n>> [CRON INITIALIZATION] Checking server status before monitoring...")
    wake_up_server(wake_url=wake_url, auto_start_local=auto_start_local, silent=False)

    logger.info(f"Starting recurring cron monitor loop (interval={interval_seconds}s). Press Ctrl+C to stop.")
    try:
        while True:
            execute_cron_check(stats_url=stats_url, sim_url=sim_url, queue_url=queue_url)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Cron monitor stopped.")


def parse_target_time(time_str: str) -> datetime:
    """Parse a time string (e.g., '17:15', '17:15:00', '5:15 PM', '5pm') into today's datetime."""
    clean_str = time_str.strip().upper()
    formats = ["%H:%M", "%H:%M:%S", "%I:%M %p", "%I:%M%p", "%I %p", "%I%p"]
    for fmt in formats:
        try:
            parsed = datetime.strptime(clean_str, fmt)
            now = datetime.now()
            target = now.replace(
                hour=parsed.hour,
                minute=parsed.minute,
                second=parsed.second,
                microsecond=0,
            )
            # If target has already passed by more than 5 seconds, schedule for tomorrow
            if (target - now).total_seconds() < -5:
                target += timedelta(days=1)
            return target
        except ValueError:
            continue
    raise ValueError(
        f"Could not parse time format: '{time_str}'. Expected formats: '17:15', '17:15:00', '5:15 PM', '5pm'"
    )


def parse_relative_time(in_str: str) -> datetime:
    """Parse relative duration like '15s', '30s', '1m', '5m', or raw seconds into target datetime."""
    clean = in_str.strip().lower()
    seconds = 0
    if clean.endswith("s"):
        seconds = int(clean[:-1])
    elif clean.endswith("m"):
        seconds = int(clean[:-1]) * 60
    elif clean.endswith("h"):
        seconds = int(clean[:-1]) * 3600
    else:
        seconds = int(clean)
    return datetime.now() + timedelta(seconds=seconds)


def wait_until_target_time(
    target_dt: datetime,
    action: str = "start-sim",
    speed: float = 60.0,
    stats_url: str = DEFAULT_STATS_URL,
    sim_url: str = DEFAULT_SIM_STATUS_URL,
    queue_url: str = DEFAULT_QUEUE_STATUS_URL,
    wake_url: str = DEFAULT_WAKE_URL,
    start_sim_url: str = DEFAULT_START_SIM_URL,
    auto_start_local: bool = True,
    repeat_interval: Optional[int] = 3,
):
    target_str = target_dt.strftime("%I:%M:%S %p (%H:%M:%S)")
    now_str = datetime.now().strftime("%I:%M:%S %p")
    print(f"\n[CRON SCHEDULER] Current local time:       {now_str}")
    print(f"[CRON SCHEDULER] Target scheduled time:    {target_str}")
    print(f"[CRON SCHEDULER] Target action on trigger: {action} (speed: {speed}x)")
    print(f"[CRON SCHEDULER] Target wake-up endpoint:  {wake_url}")

    # Initial check: verify server is awake or automatically boot it up
    print("\n>> [INITIAL SERVER CHECK] Verifying server status before starting countdown...")
    wake_up_server(wake_url=wake_url, auto_start_local=auto_start_local, silent=False)

    print(f"\n[CRON SCHEDULER] Listening engaged. Countdown ticking down to target hour...")

    last_heartbeat = time.time()
    pre_warmed = False

    try:
        while True:
            now = datetime.now()
            remaining = (target_dt - now).total_seconds()

            if remaining <= 0:
                break

            # 1. Pre-warm at T-minus 30s to prevent cold-start latency at target time
            if remaining <= 30 and not pre_warmed:
                pre_warmed = True
                sys.stdout.write(
                    f"\n\n>> [PRE-TRIGGER WARM-UP] T-30s until trigger! Pre-warming server connections to guarantee 0ms latency...\n"
                )
                sys.stdout.flush()
                wake_up_server(wake_url=wake_url, auto_start_local=auto_start_local, silent=False)
                print(">> [PRE-TRIGGER WARM-UP] Server warm & primed! Resuming countdown...")

            # 2. Heartbeat keep-alive every 4 minutes (240s) during long waits to keep cloud instances from sleeping
            elif (time.time() - last_heartbeat) >= 240 and remaining > 45:
                last_heartbeat = time.time()
                wake_up_server(wake_url=wake_url, auto_start_local=False, silent=True)

            mins, secs = divmod(int(remaining), 60)
            hours, mins = divmod(mins, 60)
            countdown_str = f"{hours:02d}:{mins:02d}:{secs:02d}"

            sys.stdout.write(
                f"\r>> [WAITING FOR CRON] Target: {target_dt.strftime('%I:%M:%S %p')} | "
                f"Now: {now.strftime('%H:%M:%S')} | "
                f"T-minus: {countdown_str}   "
            )
            sys.stdout.flush()

            time.sleep(min(1.0, max(0.1, remaining)))

        print("\n\n" + "=" * 72)
        print(f">> [CRON FIRED @ {datetime.now().strftime('%I:%M:%S %p')}] Target Hour Reached! Triggering Action!")
        print("=" * 72)

        # Final wake-up confirmation
        wake_up_server(wake_url=wake_url, auto_start_local=auto_start_local, silent=False)

        # Execute scheduled action
        trigger_scheduled_action(action=action, speed=speed, start_sim_url=start_sim_url)

        # Immediate telemetry check
        time.sleep(0.5)
        execute_cron_check(stats_url=stats_url, sim_url=sim_url, queue_url=queue_url)

        if repeat_interval:
            print(f"\n[CRON SCHEDULER] Action launched! Actively monitoring queue telemetry every {repeat_interval}s...")
            run_cron_loop(
                interval_seconds=repeat_interval,
                stats_url=stats_url,
                sim_url=sim_url,
                queue_url=queue_url,
                wake_url=wake_url,
                auto_start_local=False,
            )

    except KeyboardInterrupt:
        print("\n[CRON SCHEDULER] Scheduled cron cancelled by user.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run attendance queue telemetry cron job with server wake-up")
    parser.add_argument(
        "--at",
        type=str,
        help="Schedule execution at specific real-world time (e.g., '17:15', '5:15 PM', '18:00')",
    )
    parser.add_argument(
        "--in",
        dest="in_time",
        type=str,
        help="Quick test schedule in relative time (e.g. '15s', '30s', '1m')",
    )
    parser.add_argument(
        "--action",
        choices=["start-sim", "report"],
        default="start-sim",
        help="Action to fire at target time (default: 'start-sim' to start the live website simulation)",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=60.0,
        help="Simulation speed if action is start-sim (e.g. 60.0 for 2m run, 1.0 for 2h real time)",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=3,
        help="Telemetry monitoring interval in seconds after action is triggered (default: 3s)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Interval in seconds for continuous report-only loop mode (default: 10s)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single cron check immediately and exit",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default=DEFAULT_BASE_URL,
        help=f"Base server URL (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--wake-url",
        type=str,
        default=None,
        help="Custom wake-up endpoint URL (defaults to '<base-url>/api/wakeup')",
    )
    parser.add_argument(
        "--wake-only",
        action="store_true",
        help="Ping and wake up the server immediately, report status, and exit",
    )
    parser.add_argument(
        "--wake-timeout",
        type=float,
        default=45.0,
        help="Max seconds to wait for sleeping/cold-starting server to respond (default: 45s)",
    )
    parser.add_argument(
        "--no-auto-start",
        action="store_true",
        help="Do not automatically launch run_all.py if local server is down",
    )
    parser.add_argument(
        "--stats-url",
        default=None,
        help="Stats endpoint URL (defaults to '<base-url>/api/stats')",
    )
    args = parser.parse_args()

    # Resolve URLs from base_url
    base = args.base_url.rstrip("/")
    stats_url = args.stats_url or f"{base}/api/stats"
    sim_url = f"{base}/api/simulation/status"
    queue_url = f"{base}/api/queue/status"
    start_sim_url = f"{base}/api/simulation/start"
    wake_url = args.wake_url or f"{base}/api/wakeup"
    auto_start = not args.no_auto_start

    if args.wake_only:
        print(f">> [WAKE-UP CHECK] Sending wake-up signal to: {wake_url}")
        success = wake_up_server(
            wake_url=wake_url,
            timeout_seconds=args.wake_timeout,
            auto_start_local=auto_start,
            silent=False,
        )
        sys.exit(0 if success else 1)

    if args.once:
        wake_up_server(wake_url=wake_url, timeout_seconds=args.wake_timeout, auto_start_local=auto_start, silent=False)
        execute_cron_check(stats_url=stats_url, sim_url=sim_url, queue_url=queue_url)
    elif args.in_time is not None:
        target_dt = parse_relative_time(args.in_time)
        wait_until_target_time(
            target_dt=target_dt,
            action=args.action,
            speed=args.speed,
            stats_url=stats_url,
            sim_url=sim_url,
            queue_url=queue_url,
            wake_url=wake_url,
            start_sim_url=start_sim_url,
            auto_start_local=auto_start,
            repeat_interval=args.repeat,
        )
    elif args.at is not None:
        target_dt = parse_target_time(args.at)
        wait_until_target_time(
            target_dt=target_dt,
            action=args.action,
            speed=args.speed,
            stats_url=stats_url,
            sim_url=sim_url,
            queue_url=queue_url,
            wake_url=wake_url,
            start_sim_url=start_sim_url,
            auto_start_local=auto_start,
            repeat_interval=args.repeat,
        )
    else:
        run_cron_loop(
            interval_seconds=args.interval,
            stats_url=stats_url,
            sim_url=sim_url,
            queue_url=queue_url,
            wake_url=wake_url,
            auto_start_local=auto_start,
        )
