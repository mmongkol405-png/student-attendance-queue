"""
Real-Time 2-Hour Student Attendance Simulation Engine.
Distributes 350 student sign-in events over a realistic 2-hour lecture session (00:00:00 to 02:00:00).
Supports real-time pacing (1x speed = 2 hours) or accelerated visual testing (10x, 30x, 60x, 120x).
Tracks live queue state transitions and reconciles absent students upon session completion.
"""

import argparse
import asyncio
import json
import logging
import random
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger("simulation_engine")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [SimEngine] %(message)s")

DEFAULT_INGEST_URL = "http://127.0.0.1:8080/api/attendance/signin"
DEFAULT_RECONCILE_URL = "http://127.0.0.1:8080/api/attendance/reconcile"
DEFAULT_STATS_URL = "http://127.0.0.1:8080/api/stats"
DEFAULT_RESET_URL = "http://127.0.0.1:8080/api/reset"
SESSION_ID = "SESS-2026-09-17"
TOTAL_SESSION_SECONDS = 7200  # 2 hours = 120 minutes = 7200 seconds


class TwoHourSessionSimulator:
    """
    Manages stateful progression of a 2-hour attendance simulation.
    """

    def __init__(
        self,
        ingest_url: str = DEFAULT_INGEST_URL,
        reconcile_url: str = DEFAULT_RECONCILE_URL,
        stats_url: str = DEFAULT_STATS_URL,
        reset_url: str = DEFAULT_RESET_URL,
    ):
        self.ingest_url = ingest_url
        self.reconcile_url = reconcile_url
        self.stats_url = stats_url
        self.reset_url = reset_url

        self.is_running = False
        self.is_paused = False
        self.speed = 1.0  # Multiplier: 1.0 = 2 hours real-time, 60.0 = 2 hours in 2 minutes
        self.elapsed_sim_seconds = 0.0
        self.start_wall_time: Optional[float] = None
        self.thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # State tracking
        self.scheduled_events: List[Dict[str, Any]] = []
        self.dispatched_events: List[Dict[str, Any]] = []
        self.recent_activity_log: List[Dict[str, Any]] = []
        self.current_phase = "IDLE"
        self.reconciled = False

        self._lock = threading.Lock()
        self._build_schedule()

    def _build_schedule(self):
        """
        Builds realistic arrival timestamps across the 7,200-second (2-hour) lecture.
        Session start: T = 900s (09:00 AM, 15 min into room open).
        Grace period ends: T = 1800s (09:15 AM).
        Class ends: T = 7200s (11:00 AM).
        """
        random.seed(42)  # Deterministic schedule for reproducible runs
        events = []

        # Wave 1: 60 Early arrivals (0 to 900 simulated seconds: 08:45 - 09:00 AM) -> PRESENT
        for i in range(1, 61):
            t_sec = random.uniform(60, 890)
            events.append({
                "student_id": f"STU{i:03d}",
                "sim_time_sec": t_sec,
                "virtual_time_str": self._sec_to_clock(t_sec, start_hour=8, start_min=45),
                "kiosk_id": f"scanner-north-{(i % 4) + 1}",
                "expected_status": "PRESENT",
                "phase": "Early Arrivals",
            })

        # Wave 2: 170 On-Time Bell Rush (900 to 1790 simulated seconds: 09:00 - 09:14:50 AM) -> PRESENT
        for i in range(61, 231):
            # Beta distribution clustered around class bell (T=900s to 1200s)
            t_sec = 900 + (random.betavariate(1.5, 3.5) * 890)
            events.append({
                "student_id": f"STU{i:03d}",
                "sim_time_sec": t_sec,
                "virtual_time_str": self._sec_to_clock(t_sec, start_hour=8, start_min=45),
                "kiosk_id": f"scanner-hall-{(i % 6) + 1}",
                "expected_status": "PRESENT",
                "phase": "Peak Class Rush",
            })

        # Wave 3: 45 Tardy Students (1810 to 2700 simulated seconds: 09:15:10 - 09:30 AM) -> LATE
        for i in range(231, 276):
            t_sec = random.uniform(1810, 2700)
            events.append({
                "student_id": f"STU{i:03d}",
                "sim_time_sec": t_sec,
                "virtual_time_str": self._sec_to_clock(t_sec, start_hour=8, start_min=45),
                "kiosk_id": f"scanner-south-{(i % 4) + 1}",
                "expected_status": "LATE",
                "phase": "Tardy Grace Period Exceeded",
            })

        # Wave 4: 25 Stragglers (2701 to 4500 simulated seconds: 09:30 - 10:00 AM) -> LATE
        for i in range(276, 301):
            t_sec = random.uniform(2710, 4500)
            events.append({
                "student_id": f"STU{i:03d}",
                "sim_time_sec": t_sec,
                "virtual_time_str": self._sec_to_clock(t_sec, start_hour=8, start_min=45),
                "kiosk_id": "scanner-late-desk",
                "expected_status": "LATE",
                "phase": "Mid-Lecture Stragglers",
            })

        # Wave 5: 15 Accidental Duplicate Swipes (scattered double taps)
        dup_students = [12, 18, 45, 77, 88, 102, 115, 140, 165, 199, 210, 222, 240, 255, 270]
        for s_idx in dup_students:
            # Tap again 2-5 seconds after original
            orig_t = next(e["sim_time_sec"] for e in events if e["student_id"] == f"STU{s_idx:03d}")
            events.append({
                "student_id": f"STU{s_idx:03d}",
                "sim_time_sec": orig_t + random.uniform(2.0, 5.0),
                "virtual_time_str": self._sec_to_clock(orig_t + 3, start_hour=8, start_min=45),
                "kiosk_id": "scanner-hall-2",
                "expected_status": "DUPLICATE",
                "phase": "Accidental Double Swipe",
            })

        # Note: Students STU301 to STU350 (50 students) will NEVER check in -> ABSENT

        # Sort schedule chronologically by sim_time_sec
        events.sort(key=lambda x: x["sim_time_sec"])
        self.scheduled_events = events

    @staticmethod
    def _sec_to_clock(sim_sec: float, start_hour: int = 8, start_min: int = 45) -> str:
        base_time = datetime(2026, 9, 17, start_hour, start_min, 0, tzinfo=timezone.utc)
        target = base_time + timedelta(seconds=int(sim_sec))
        return target.strftime("%Y-%m-%dT%H:%M:%SZ")

    def start(self, speed: float = 1.0, reset_db: bool = True):
        with self._lock:
            if self.is_running:
                logger.warning("Simulation is already running.")
                return

            self.speed = max(0.1, float(speed))
            self.elapsed_sim_seconds = 0.0
            self.dispatched_events = []
            self.recent_activity_log = []
            self.reconciled = False
            self.is_running = True
            self.is_paused = False
            self._stop_event.clear()
            self._build_schedule()

        if reset_db:
            try:
                httpx.post(self.reset_url, timeout=5.0)
                logger.info("Reset database prior to starting 2-hour simulation.")
            except Exception as e:
                logger.warning(f"Could not reset db: {e}")

        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        logger.info(f"2-Hour Attendance Simulator started at speed {self.speed}x (Total: 7,200s).")

    def pause(self):
        with self._lock:
            self.is_paused = True
            logger.info("Simulation paused.")

    def resume(self):
        with self._lock:
            self.is_paused = False
            logger.info("Simulation resumed.")

    def stop(self):
        self._stop_event.set()
        self.is_running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=1.0)
        logger.info("Simulation stopped.")

    def set_speed(self, new_speed: float):
        with self._lock:
            self.speed = max(0.1, float(new_speed))
            logger.info(f"Simulation speed adjusted to {self.speed}x.")

    def _determine_phase(self, sim_sec: float) -> str:
        if sim_sec < 900:
            return "Phase 1: Pre-Class Arrival Trickle (08:45 - 09:00)"
        elif sim_sec < 1800:
            return "Phase 2: Peak Class Start Rush (09:00 - 09:15)"
        elif sim_sec < 2700:
            return "Phase 3: Tardy / Grace Period Exceeded (09:15 - 09:30)"
        elif sim_sec < 4500:
            return "Phase 4: Mid-Lecture Stragglers (09:30 - 10:00)"
        elif sim_sec < 7200:
            return "Phase 5: Lecture Active (Scanning Inactive, 10:00 - 11:00)"
        else:
            return "Phase 6: Session Concluded (Absences Reconciled)"

    def _run_loop(self):
        last_wall_time = time.perf_counter()
        next_event_idx = 0
        total_events = len(self.scheduled_events)

        client = httpx.Client(timeout=15.0)

        try:
            while not self._stop_event.is_set() and self.elapsed_sim_seconds <= TOTAL_SESSION_SECONDS:
                now_wall = time.perf_counter()
                dt_wall = now_wall - last_wall_time
                last_wall_time = now_wall

                if not self.is_paused:
                    self.elapsed_sim_seconds += dt_wall * self.speed
                    self.current_phase = self._determine_phase(self.elapsed_sim_seconds)

                    # Check for events that have matured
                    while next_event_idx < total_events:
                        ev = self.scheduled_events[next_event_idx]
                        if ev["sim_time_sec"] <= self.elapsed_sim_seconds:
                            # Fire sign-in request to Ingest API
                            self._dispatch_event(client, ev)
                            next_event_idx += 1
                        else:
                            break

                time.sleep(0.05)

            # Session over: reached 7200 simulated seconds (2 hours)
            if not self._stop_event.is_set() and not self.reconciled:
                logger.info("2-Hour mark reached. Officially reconciling 50 absent students...")
                self._reconcile_session(client)
                self.reconciled = True
                self.current_phase = "Phase 6: Session Concluded (All Records Finalized)"
                self.is_running = False

        finally:
            client.close()

    def _dispatch_event(self, client: httpx.Client, ev: Dict[str, Any]):
        payload = {
            "student_id": ev["student_id"],
            "session_id": SESSION_ID,
            "device_id": ev["kiosk_id"],
            "sign_in_time": ev["virtual_time_str"],
        }
        try:
            res = client.post(self.ingest_url, json=payload)
            task_id = res.json().get("task_id", "task-local") if res.status_code == 202 else "err"

            log_entry = {
                "sim_time": self._format_sim_clock(self.elapsed_sim_seconds),
                "virtual_time": ev["virtual_time_str"].split("T")[1][:8],
                "student_id": ev["student_id"],
                "kiosk": ev["kiosk_id"],
                "status": ev["expected_status"],
                "task_id": task_id,
                "phase": ev["phase"],
            }

            with self._lock:
                self.dispatched_events.append(log_entry)
                self.recent_activity_log.insert(0, log_entry)
                if len(self.recent_activity_log) > 50:
                    self.recent_activity_log.pop()

        except Exception as e:
            logger.error(f"Error dispatching student {ev['student_id']}: {e}")

    def _reconcile_session(self, client: httpx.Client):
        try:
            res = client.post(self.reconcile_url, json={"session_id": SESSION_ID})
            logger.info(f"Reconciliation complete: {res.json()}")
        except Exception as e:
            logger.error(f"Failed to reconcile session: {e}")

    @staticmethod
    def _format_sim_clock(seconds: float) -> str:
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            elapsed_sec = min(self.elapsed_sim_seconds, float(TOTAL_SESSION_SECONDS))
            pct = (elapsed_sec / TOTAL_SESSION_SECONDS) * 100.0
            remaining_sec = max(0, TOTAL_SESSION_SECONDS - int(elapsed_sec))

            # Virtual time on clock (from 08:45 AM)
            virtual_dt = datetime(2026, 9, 17, 8, 45, 0, tzinfo=timezone.utc) + timedelta(seconds=int(elapsed_sec))

            return {
                "is_running": self.is_running,
                "is_paused": self.is_paused,
                "speed": self.speed,
                "elapsed_sim_seconds": int(elapsed_sec),
                "total_session_seconds": TOTAL_SESSION_SECONDS,
                "progress_percent": round(pct, 1),
                "sim_clock": self._format_sim_clock(elapsed_sec),
                "remaining_clock": self._format_sim_clock(remaining_sec),
                "virtual_time_str": virtual_dt.strftime("%I:%M:%S %p"),
                "current_phase": self.current_phase,
                "total_scheduled": len(self.scheduled_events),
                "total_dispatched": len(self.dispatched_events),
                "reconciled": self.reconciled,
                "recent_activity": self.recent_activity_log[:15],
            }


# Global singleton simulator instance for the application
simulator = TwoHourSessionSimulator()


def run_standalone():
    parser = argparse.ArgumentParser(description="Run 2-Hour Real-Time Attendance Simulator")
    parser.add_argument("--speed", type=float, default=60.0, help="Simulation speed multiplier (1.0 = real-time, 60.0 = 2 min)")
    parser.add_argument("--no-reset", action="store_true", help="Do not reset database before simulation")
    args = parser.parse_args()

    print("\n" + "=" * 75)
    print("   2-HOUR REAL-TIME STUDENT ATTENDANCE SIMULATION ENGINE")
    print(f"   Speed: {args.speed}x (Estimated duration: {7200 / args.speed:.1f}s)")
    print("=" * 75)

    simulator.start(speed=args.speed, reset_db=not args.no_reset)

    try:
        while simulator.is_running:
            st = simulator.get_status()
            bar_len = 30
            filled = int((st["progress_percent"] / 100.0) * bar_len)
            bar = "#" * filled + "-" * (bar_len - filled)
            print(
                f"\r  [{bar}] {st['progress_percent']:5.1f}% | "
                f"Clock: {st['sim_clock']} / 02:00:00 (Time: {st['virtual_time_str']}) | "
                f"Dispatched: {st['total_dispatched']:3d}/315 | "
                f"{st['current_phase'][:35]}",
                end="",
                flush=True,
            )
            time.sleep(0.5)

        print("\n\n[*] Simulation completed successfully!")
        print(f"    Final Status: {simulator.get_status()['current_phase']}")
    except KeyboardInterrupt:
        print("\nStopping simulator...")
        simulator.stop()


if __name__ == "__main__":
    run_standalone()
