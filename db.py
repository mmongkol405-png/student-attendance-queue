"""
Database module for the Student Attendance System.
Uses SQLite with WAL mode for high concurrent throughput during load testing.
"""

import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

DB_FILE = os.environ.get("ATTENDANCE_DB_PATH", "attendance.db")


def get_connection(db_path: str = DB_FILE) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Enable WAL mode and synchronous=NORMAL for high concurrency
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(db_path: str = DB_FILE) -> None:
    """Creates tables if they do not exist."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS courses (
                course_id TEXT PRIMARY KEY,
                course_name TEXT NOT NULL,
                instructor TEXT NOT NULL
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS students (
                student_id TEXT PRIMARY KEY,
                full_name TEXT NOT NULL,
                email TEXT NOT NULL,
                course_id TEXT NOT NULL,
                FOREIGN KEY (course_id) REFERENCES courses(course_id)
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                course_id TEXT NOT NULL,
                session_name TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                FOREIGN KEY (course_id) REFERENCES courses(course_id)
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS attendance_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                student_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                device_id TEXT,
                sign_in_time TEXT NOT NULL,
                status TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                UNIQUE(student_id, session_id),
                FOREIGN KEY (student_id) REFERENCES students(student_id),
                FOREIGN KEY (session_id) REFERENCES sessions(session_id)
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS task_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                queue_name TEXT,
                retry_count INTEGER DEFAULT 0,
                event_type TEXT NOT NULL,
                payload TEXT,
                logged_at TEXT NOT NULL
            );
            """
        )
        conn.commit()


def seed_data(db_path: str = DB_FILE, student_count: int = 350) -> None:
    """Pre-populates the database with courses, sessions, and students."""
    init_db(db_path)
    with get_connection(db_path) as conn:
        cursor = conn.cursor()

        # Seed Course
        cursor.execute(
            """
            INSERT OR IGNORE INTO courses (course_id, course_name, instructor)
            VALUES ('CS-301', 'Cloud-Native Distributed Systems', 'Dr. Sarah Connor');
            """
        )

        # Seed Session
        cursor.execute(
            """
            INSERT OR IGNORE INTO sessions (session_id, course_id, session_name, start_time, end_time, status)
            VALUES (
                'SESS-2026-09-17',
                'CS-301',
                'Lecture 14: Serverless Queues & Event-Driven Architecture',
                '2026-09-17T09:00:00Z',
                '2026-09-17T11:00:00Z',
                'ACTIVE'
            );
            """
        )

        # Seed Students (at least 350 students)
        students = []
        for i in range(1, student_count + 1):
            student_id = f"STU{i:03d}"
            name = f"Student {i:03d}"
            email = f"student{i:03d}@university.edu"
            students.append((student_id, name, email, "CS-301"))

        cursor.executemany(
            """
            INSERT OR IGNORE INTO students (student_id, full_name, email, course_id)
            VALUES (?, ?, ?, ?);
            """,
            students,
        )
        conn.commit()


def record_attendance(
    task_id: str,
    student_id: str,
    session_id: str,
    device_id: Optional[str],
    sign_in_time: str,
    queue_name: Optional[str] = None,
    retry_count: int = 0,
    db_path: str = DB_FILE,
) -> Dict[str, Any]:
    """
    Processes attendance check-in.
    Ensures idempotency: duplicate sign-ins for same student+session are recognized.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    with get_connection(db_path) as conn:
        cursor = conn.cursor()

        # Check if student exists
        cursor.execute("SELECT student_id, full_name FROM students WHERE student_id = ?", (student_id,))
        student = cursor.fetchone()
        if not student:
            return {
                "success": False,
                "status": "STUDENT_NOT_FOUND",
                "message": f"Student {student_id} is not registered in the database.",
            }

        # Check session
        cursor.execute("SELECT session_id, start_time FROM sessions WHERE session_id = ?", (session_id,))
        session = cursor.fetchone()
        if not session:
            return {
                "success": False,
                "status": "SESSION_NOT_FOUND",
                "message": f"Session {session_id} not found.",
            }

        # Determine attendance status (PRESENT vs LATE) based on session start
        status = "PRESENT"
        try:
            sign_dt = datetime.fromisoformat(sign_in_time.replace("Z", "+00:00"))
            sess_dt = datetime.fromisoformat(session["start_time"].replace("Z", "+00:00"))
            diff_min = (sign_dt - sess_dt).total_seconds() / 60.0
            if diff_min > 15.0:
                status = "LATE"
            else:
                status = "PRESENT"
        except Exception:
            if "T09:15" in sign_in_time or "T09:2" in sign_in_time or "T09:3" in sign_in_time or "T09:4" in sign_in_time or "T10:" in sign_in_time:
                status = "LATE"

        # Insert record or detect duplicate
        try:
            cursor.execute(
                """
                INSERT INTO attendance_records (task_id, student_id, session_id, device_id, sign_in_time, status, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (task_id, student_id, session_id, device_id, sign_in_time, status, now_iso),
            )
            record_id = cursor.lastrowid
            is_duplicate = False
        except sqlite3.IntegrityError:
            # Duplicate sign-in attempt
            cursor.execute(
                """
                SELECT id, status, processed_at FROM attendance_records
                WHERE student_id = ? AND session_id = ?
                """,
                (student_id, session_id),
            )
            existing = cursor.fetchone()
            record_id = existing["id"]
            status = "DUPLICATE"
            is_duplicate = True

        # Log the task processing event
        cursor.execute(
            """
            INSERT INTO task_logs (task_id, queue_name, retry_count, event_type, payload, logged_at)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (
                task_id,
                queue_name,
                retry_count,
                "TASK_PROCESSED" if not is_duplicate else "TASK_DUPLICATE_IGNORED",
                f"student={student_id},session={session_id},status={status}",
                now_iso,
            ),
        )
        conn.commit()

        return {
            "success": True,
            "record_id": record_id,
            "student_id": student_id,
            "student_name": student["full_name"],
            "session_id": session_id,
            "status": status,
            "is_duplicate": is_duplicate,
            "processed_at": now_iso,
        }


def reconcile_absent_students(session_id: str = "SESS-2026-09-17", db_path: str = DB_FILE) -> Dict[str, Any]:
    """
    Scans enrolled students who never submitted a sign-in task for this session
    and officially records them as ABSENT.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    with get_connection(db_path) as conn:
        cursor = conn.cursor()

        # Find students who have not signed in
        cursor.execute(
            """
            SELECT s.student_id, s.full_name
            FROM students s
            WHERE s.student_id NOT IN (
                SELECT a.student_id FROM attendance_records a WHERE a.session_id = ?
            );
            """,
            (session_id,),
        )
        absent_students = cursor.fetchall()
        absent_count = len(absent_students)

        records_to_insert = [
            (
                f"reconciliation-absent-{row['student_id']}",
                row["student_id"],
                session_id,
                "system-audit",
                "UNRECORDED",
                "ABSENT",
                now_iso,
            )
            for row in absent_students
        ]

        if records_to_insert:
            cursor.executemany(
                """
                INSERT OR IGNORE INTO attendance_records
                (task_id, student_id, session_id, device_id, sign_in_time, status, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                records_to_insert,
            )
            conn.commit()

        return {
            "session_id": session_id,
            "absent_count_recorded": absent_count,
            "reconciled_at": now_iso,
        }


def get_stats(db_path: str = DB_FILE, session_id: str = "SESS-2026-09-17") -> Dict[str, Any]:
    """Returns real-time statistics of the attendance database."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM students")
        total_students = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM attendance_records WHERE status != 'ABSENT'")
        total_attendance = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM task_logs")
        total_tasks_processed = cursor.fetchone()[0]

        cursor.execute("SELECT status, COUNT(*) FROM attendance_records WHERE session_id = ? GROUP BY status", (session_id,))
        status_counts = dict(cursor.fetchall())

        # Duplicate counts from task logs
        cursor.execute("SELECT COUNT(*) FROM task_logs WHERE event_type = 'TASK_DUPLICATE_IGNORED'")
        duplicate_count = cursor.fetchone()[0]

        present_count = status_counts.get("PRESENT", 0)
        late_count = status_counts.get("LATE", 0)
        absent_count = status_counts.get("ABSENT", 0)

        # If absent records haven't been finalized in DB yet, calculate pending absent
        if absent_count == 0 and (present_count + late_count) > 0:
            absent_count = max(0, total_students - (present_count + late_count))

        return {
            "total_registered_students": total_students,
            "total_attendance_records": total_attendance,
            "total_tasks_processed": total_tasks_processed,
            "present_count": present_count,
            "late_count": late_count,
            "absent_count": absent_count,
            "duplicate_attempts_blocked": duplicate_count,
            "status_breakdown": {
                "PRESENT": present_count,
                "LATE": late_count,
                "ABSENT": absent_count,
            },
        }


def get_recent_records(limit: int = 25, db_path: str = DB_FILE) -> List[Dict[str, Any]]:
    """Returns recent attendance records with student details."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT a.id, a.task_id, a.student_id, s.full_name, a.session_id, a.device_id,
                   a.sign_in_time, a.status, a.processed_at
            FROM attendance_records a
            JOIN students s ON a.student_id = s.student_id
            ORDER BY a.id DESC
            LIMIT ?;
            """,
            (limit,),
        )
        return [dict(row) for row in cursor.fetchall()]


def clear_attendance(db_path: str = DB_FILE) -> None:
    """Clears attendance records and task logs for re-running tests."""
    with get_connection(db_path) as conn:
        conn.execute("DELETE FROM attendance_records;")
        conn.execute("DELETE FROM task_logs;")
        conn.commit()


if __name__ == "__main__":
    init_db()
    seed_data()
    print("Database initialized and seeded successfully!")
    print(get_stats())
