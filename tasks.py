"""
tasks.py — Celery async task definitions
=========================================
Offloads every write-heavy or fan-out operation out of the HTTP request cycle.

Setup:
    pip install celery[redis]

Start worker (dev):
    celery -A tasks worker --loglevel=info --concurrency=4

Start worker (production, with systemd or Supervisor):
    celery -A tasks worker --loglevel=info \
           --concurrency=8 --max-tasks-per-child=500 \
           -Q notifications,default

Beat scheduler (for periodic tasks):
    celery -A tasks beat --loglevel=info

Required env vars:
    CELERY_BROKER_URL   redis://localhost:6379/1
    CELERY_RESULT_URL   redis://localhost:6379/2   (optional)
"""

from __future__ import annotations

import logging
import os
import time
from typing import List

from celery import Celery
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

# ---------------------------------------------------------------------------
# Celery app
# ---------------------------------------------------------------------------
BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/1")
RESULT_URL = os.getenv("CELERY_RESULT_URL", "redis://localhost:6379/2")

celery_app = Celery(
    "jkuat_scheduler",
    broker=BROKER_URL,
    backend=RESULT_URL,
)

celery_app.conf.update(
    # Serialisation
    task_serializer          = "json",
    result_serializer        = "json",
    accept_content           = ["json"],
    # Reliability
    task_acks_late           = True,    # ack only after task completes, not on receipt
    task_reject_on_worker_lost = True,  # re-queue if worker dies mid-task
    # Retries
    task_max_retries         = 3,
    # Routing: put notification fan-out on its own queue
    # so it never starves other tasks
    task_routes              = {
        "tasks.dispatch_lecture_notifications": {"queue": "notifications"},
        "tasks.bulk_delete_notifications":      {"queue": "notifications"},
        "tasks.warm_room_cache":                {"queue": "default"},
        "tasks.warm_course_cache":              {"queue": "default"},
    },
    # Beat schedule (periodic tasks)
    beat_schedule            = {
        "warm-room-cache-every-5-min": {
            "task":     "tasks.warm_room_cache",
            "schedule": 300,   # seconds
        },
    },
    timezone                 = "Africa/Nairobi",
)


# ---------------------------------------------------------------------------
# Flask app context helper
# ---------------------------------------------------------------------------
def _get_flask_app():
    """Lazy-import Flask app to avoid circular imports."""
    from app import app as flask_app
    return flask_app


# ---------------------------------------------------------------------------
# TASK 1 — Notification fan-out  (the main offender)
# ---------------------------------------------------------------------------
@celery_app.task(
    bind=True,
    name="tasks.dispatch_lecture_notifications",
    queue="notifications",
    max_retries=3,
    default_retry_delay=10,
)
def dispatch_lecture_notifications(self, lecture_id: int) -> dict:
    """
    Insert one Notification row per enrolled student for the given lecture.

    Uses a single bulk INSERT instead of per-row ORM adds, and chunks the
    work so very large cohorts (500+ students) don't hold a DB connection
    open for a long time.

    Returns a summary dict that Celery stores in its result backend.
    """
    flask_app = _get_flask_app()
    with flask_app.app_context():
        from models import db, Lecture, Enrollment, Notification, Course

        try:
            lecture = db.session.get(Lecture, lecture_id)
            if not lecture:
                logger.warning("dispatch_lecture_notifications: lecture %s not found", lecture_id)
                return {"notified": 0, "skipped": True}

            course = db.session.get(Course, lecture.course_id)

            # Fetch all enrolled student IDs — scalar query, no ORM hydration
            from sqlalchemy import text
            student_ids: List[int] = db.session.execute(
                text("SELECT student_id FROM enrollment WHERE course_id = :cid"),
                {"cid": lecture.course_id},
            ).scalars().all()

            if not student_ids:
                return {"notified": 0}

            CHUNK = 200   # rows per bulk insert
            total  = 0
            now    = __import__("datetime").datetime.utcnow()

            message = (
                f"New lecture scheduled: {course.name} — "
                f"{lecture.day} {lecture.start_time.strftime('%H:%M')}–"
                f"{lecture.end_time.strftime('%H:%M')}"
            )

            for i in range(0, len(student_ids), CHUNK):
                chunk = student_ids[i : i + CHUNK]
                rows = [
                    {
                        "user_id":    sid,
                        "lecture_id": lecture_id,
                        "message":    message,
                        "is_read":    False,
                        "created_at": now,
                    }
                    for sid in chunk
                ]
                # Core Engine bulk insert — bypasses ORM overhead entirely
                db.session.execute(
                    Notification.__table__.insert(),
                    rows,
                )
                db.session.commit()
                total += len(chunk)
                logger.info(
                    "Notified chunk %d–%d for lecture %d",
                    i, i + len(chunk), lecture_id,
                )

            logger.info("dispatch_lecture_notifications: %d students notified", total)
            return {"notified": total}

        except Exception as exc:
            logger.exception("dispatch_lecture_notifications failed: %s", exc)
            db.session.rollback()
            raise self.retry(exc=exc)


# ---------------------------------------------------------------------------
# TASK 2 — Bulk-delete notifications when a lecture is removed
# ---------------------------------------------------------------------------
@celery_app.task(
    bind=True,
    name="tasks.bulk_delete_notifications",
    queue="notifications",
    max_retries=3,
    default_retry_delay=10,
)
def bulk_delete_notifications(self, lecture_id: int) -> dict:
    flask_app = _get_flask_app()
    with flask_app.app_context():
        from models import db, Notification
        from sqlalchemy import text
        try:
            result = db.session.execute(
                text("DELETE FROM notification WHERE lecture_id = :lid"),
                {"lid": lecture_id},
            )
            db.session.commit()
            deleted = result.rowcount
            logger.info("bulk_delete_notifications: removed %d rows for lecture %d", deleted, lecture_id)
            return {"deleted": deleted}
        except Exception as exc:
            db.session.rollback()
            raise self.retry(exc=exc)


# ---------------------------------------------------------------------------
# TASK 3 — Pre-warm the room-availability cache for common time slots
# ---------------------------------------------------------------------------
@celery_app.task(
    name="tasks.warm_room_cache",
    queue="default",
)
def warm_room_cache() -> dict:
    """
    Runs on a beat schedule every 5 minutes.
    Pre-populates Redis with available-room data for all standard time slots
    so the first user after a cache expiry never hits the DB cold.
    """
    flask_app = _get_flask_app()
    with flask_app.app_context():
        from scheduler import get_available_rooms
        from datetime import datetime
        import json

        try:
            from app import _redis, _cache_set
        except ImportError:
            return {"warmed": 0}

        if _redis is None:
            return {"warmed": 0}

        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        slots = [
            ("08:00", "10:00"), ("10:00", "12:00"),
            ("12:00", "14:00"), ("14:00", "16:00"),
            ("16:00", "18:00"),
        ]

        warmed = 0
        for day in days:
            for start_str, end_str in slots:
                start = datetime.strptime(start_str, "%H:%M").time()
                end   = datetime.strptime(end_str,   "%H:%M").time()
                rooms = get_available_rooms(day, start, end)
                key   = f"rooms:avail:{day}:{start_str}:{end_str}"
                _cache_set(key, [r.to_dict() for r in rooms], ttl=360)
                warmed += 1

        logger.info("warm_room_cache: warmed %d slots", warmed)
        return {"warmed": warmed}


# ---------------------------------------------------------------------------
# TASK 4 — Pre-warm course-list cache per programme
# ---------------------------------------------------------------------------
@celery_app.task(
    name="tasks.warm_course_cache",
    queue="default",
)
def warm_course_cache() -> dict:
    flask_app = _get_flask_app()
    with flask_app.app_context():
        from models import db, Course, Programme
        from app import _cache_set

        programmes = Programme.query.with_entities(Programme.id).all()
        warmed = 0
        for (prog_id,) in programmes:
            courses = (
                Course.query
                .filter_by(programme_id=prog_id)
                .order_by(Course.year, Course.semester, Course.code)
                .all()
            )
            raw = [
                {"id": c.id, "code": c.code, "name": c.name,
                 "year": c.year, "semester": c.semester}
                for c in courses
            ]
            _cache_set(f"courses:prog:{prog_id}", raw, ttl=600)
            warmed += 1

        logger.info("warm_course_cache: warmed %d programmes", warmed)
        return {"warmed": warmed}