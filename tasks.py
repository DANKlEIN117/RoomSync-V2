from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from typing import List

from celery import Celery
from celery.utils.log import get_task_logger

from dotenv import load_dotenv
load_dotenv()

logger = get_task_logger(__name__)


# Ensure the project root is always on sys.path, regardless of where the
# Celery worker process was launched from.
_PROJECT_ROOT = os.path.dirname(os.path.realpath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


# Celery app 
BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/1")
RESULT_URL = os.getenv("CELERY_RESULT_URL", "redis://localhost:6379/2")

celery_app = Celery(
    "jkuat_scheduler",
    broker=BROKER_URL,
    backend=RESULT_URL,
)

celery_app.conf.update(
    task_serializer            = "json",
    result_serializer          = "json",
    accept_content             = ["json"],
    task_acks_late             = True,
    task_reject_on_worker_lost = True,
    task_max_retries           = 3,
    task_routes                = {
        "tasks.dispatch_lecture_notifications": {"queue": "notifications"},
        "tasks.send_lecture_emails":            {"queue": "notifications"},
        "tasks.bulk_delete_notifications":      {"queue": "notifications"},
        "tasks.purge_expired_lectures":         {"queue": "default"},
        "tasks.warm_room_cache":                {"queue": "default"},
        "tasks.warm_course_cache":              {"queue": "default"},
    },
    beat_schedule = {
        # Runs every 5 minutes — deletes any lecture whose end_time has passed.
        # Africa/Nairobi (EAT = UTC+3) is used so wall-clock times are correct.
        "purge-expired-lectures-every-5-min": {
            "task":     "tasks.purge_expired_lectures",
            "schedule": 300,
        },
        "warm-room-cache-every-5-min": {
            "task":     "tasks.warm_room_cache",
            "schedule": 300,
        },
    },
    timezone = "Africa/Nairobi",
)


# Helpers 
def _get_flask_app():
    """
    Load Flask app by absolute file path — works regardless of cwd,
    sys.path, or how the Celery worker process was launched.
    """
    import importlib.util
    if _PROJECT_ROOT not in sys.path:
        sys.path.insert(0, _PROJECT_ROOT)
    if "app" not in sys.modules:
        _app_path = os.path.join(_PROJECT_ROOT, "app.py")
        spec = importlib.util.spec_from_file_location("app", _app_path)
        mod  = importlib.util.module_from_spec(spec)
        sys.modules["app"] = mod
        spec.loader.exec_module(mod)
    return sys.modules["app"].app


# TASK 1 — In-app notification fan-out 
@celery_app.task(
    bind=True,
    name="tasks.dispatch_lecture_notifications",
    queue="notifications",
    max_retries=3,
    default_retry_delay=10,
)
def dispatch_lecture_notifications(self, lecture_id: int) -> dict:
    """
    Insert one Notification row per enrolled student (bulk INSERT in chunks).
    Then fires send_lecture_emails as a follow-up task.
    """
    flask_app = _get_flask_app()
    with flask_app.app_context():
        from models import db, Lecture, Notification, Course
        from sqlalchemy import text

        try:
            lecture = db.session.get(Lecture, lecture_id)
            if not lecture:
                logger.warning("dispatch_lecture_notifications: lecture %s not found", lecture_id)
                return {"notified": 0, "skipped": True}

            course = db.session.get(Course, lecture.course_id)

            student_ids: List[int] = db.session.execute(
                text("SELECT student_id FROM enrollment WHERE course_id = :cid"),
                {"cid": lecture.course_id},
            ).scalars().all()

            if not student_ids:
                logger.info(
                    "dispatch_lecture_notifications: no enrolled students for course %s",
                    lecture.course_id,
                )
                return {"notified": 0}

            # Build location string — room for in-person, "Online" for virtual
            if lecture.online_link:
                location_str = "Online"
            elif lecture.room:
                location_str = lecture.room.name
            else:
                location_str = "TBC"

            CHUNK   = 200
            total   = 0
            now     = datetime.utcnow()
            message = (
                f"New lecture scheduled: {course.name} ({course.code}) — "
                f"{lecture.day} {lecture.start_time.strftime('%H:%M')}–"
                f"{lecture.end_time.strftime('%H:%M')} · {location_str}"
            )

            for i in range(0, len(student_ids), CHUNK):
                chunk = student_ids[i: i + CHUNK]
                db.session.execute(
                    Notification.__table__.insert(),
                    [
                        {
                            "user_id":    sid,
                            "lecture_id": lecture_id,
                            "message":    message,
                            "is_read":    False,
                            "created_at": now,
                        }
                        for sid in chunk
                    ],
                )
                db.session.commit()
                total += len(chunk)
                logger.info(
                    "Notified chunk %d–%d for lecture %d", i, i + len(chunk), lecture_id
                )

            logger.info(
                "dispatch_lecture_notifications: %d in-app notifications sent", total
            )

            # Fire email task separately so a mail failure never rolls back notifications
            send_lecture_emails.delay(lecture_id)

            return {"notified": total}

        except Exception as exc:
            logger.exception("dispatch_lecture_notifications failed: %s", exc)
            db.session.rollback()
            raise self.retry(exc=exc)


# TASK 2 — Email fan-out 
@celery_app.task(
    bind=True,
    name="tasks.send_lecture_emails",
    queue="notifications",
    max_retries=3,
    default_retry_delay=30,
)
def send_lecture_emails(self, lecture_id: int) -> dict:
    flask_app = _get_flask_app()
    with flask_app.app_context():
        from models import db, Lecture, Enrollment, User, Course
        from flask_mail import Mail, Message

        if not flask_app.config.get("MAIL_USERNAME"):
            logger.info("send_lecture_emails: MAIL_USERNAME not set, skipping emails")
            return {"sent": 0, "skipped": True}

        mail = Mail(flask_app)

        try:
            lecture = db.session.get(Lecture, lecture_id)
            if not lecture:
                return {"sent": 0, "skipped": True}

            course   = db.session.get(Course, lecture.course_id)
            lecturer = lecture.lecturer

            if lecture.online_link:
                location_line = f"  Link     : {lecture.online_link}"
            elif lecture.room:
                location_line = f"  Venue    : {lecture.room.name}, {lecture.room.building}"
            else:
                location_line = "  Venue    : TBC"

            students = (
                db.session.query(User)
                .join(Enrollment, Enrollment.student_id == User.id)
                .filter(Enrollment.course_id == lecture.course_id)
                .all()
            )

            if not students:
                return {"sent": 0}

            sent = failed = 0

            for student in students:
                try:
                    msg = Message(
                        subject=f"[JKUAT] New lecture: {course.code} — {lecture.day}",
                        sender=flask_app.config["MAIL_USERNAME"],
                        recipients=[student.email],
                        reply_to=lecturer.email,
                    )
                    msg.body = (
                        f"Hello {student.name},\n\n"
                        f"A new lecture has been scheduled for your course.\n\n"
                        f"  Course   : {course.name} ({course.code})\n"
                        f"  Lecturer : {lecturer.name}\n"
                        f"{location_line}\n"
                        f"  Day      : {lecture.day}\n"
                        f"  Time     : {lecture.start_time.strftime('%H:%M')} – "
                        f"{lecture.end_time.strftime('%H:%M')}\n\n"
                        f"Log in to your student dashboard to view all your lectures.\n\n"
                        f"Regards,\n"
                        f"JKUAT Lecture Scheduler"
                    )
                    mail.send(msg)
                    sent += 1
                except Exception as mail_exc:
                    logger.error("Email failed for %s: %s", student.email, mail_exc)
                    failed += 1

            logger.info(
                "send_lecture_emails: sent=%d failed=%d for lecture %d",
                sent, failed, lecture_id,
            )
            return {"sent": sent, "failed": failed}

        except Exception as exc:
            logger.exception("send_lecture_emails failed: %s", exc)
            raise self.retry(exc=exc)


# TASK 3 — Bulk-delete notifications when a lecture is removed
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
        from models import db
        from sqlalchemy import text
        try:
            result = db.session.execute(
                text("DELETE FROM notification WHERE lecture_id = :lid"),
                {"lid": lecture_id},
            )
            db.session.commit()
            deleted = result.rowcount
            logger.info(
                "bulk_delete_notifications: removed %d rows for lecture %d",
                deleted, lecture_id,
            )
            return {"deleted": deleted}
        except Exception as exc:
            db.session.rollback()
            raise self.retry(exc=exc)


# TASK 4 — Purge lectures whose end_time has passed 
@celery_app.task(
    bind=True,
    name="tasks.purge_expired_lectures",
    queue="default",
    max_retries=3,
    default_retry_delay=30,
)
def purge_expired_lectures(self) -> dict:
    
    flask_app = _get_flask_app()
    with flask_app.app_context():
        from models import db
        from sqlalchemy import text, bindparam
        import cache as _cache

        try:
            # Current EAT time (UTC+3, no pytz needed)
            now_eat  = datetime.utcnow() + timedelta(hours=3)
            today_wd = now_eat.weekday()   # 0=Mon … 6=Sun
            now_time = now_eat.time()      # e.g. datetime.time(10, 5, 0)

            day_order = {
                "Monday": 0, "Tuesday": 1, "Wednesday": 2,
                "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6,
            }

            # Load all lectures: only the columns we need 
            rows = db.session.execute(
                text("SELECT id, day, start_time, end_time, room_id FROM lecture")
            ).fetchall()

            expired_ids        = []
            room_slots_to_bust = []  # (day_str, start_str, end_str)

            for lec_id, lec_day, lec_start, lec_end, lec_room_id in rows:
                lec_wd = day_order.get(lec_day)
                if lec_wd is None:
                    continue  # unknown day value — defensive skip

                # Normalise end_time to a datetime.time object
                # (SQLite returns strings; PostgreSQL returns datetime.time)
                if hasattr(lec_end, "hour"):
                    end_t = lec_end
                else:
                    end_t = datetime.strptime(str(lec_end)[:5], "%H:%M").time()

                if hasattr(lec_start, "hour"):
                    start_t = lec_start
                else:
                    start_t = datetime.strptime(str(lec_start)[:5], "%H:%M").time()

                if lec_wd < today_wd:
                    # Was earlier this week — definitely expired
                    expired_ids.append(lec_id)
                    if lec_room_id:
                        room_slots_to_bust.append((
                            lec_day,
                            start_t.strftime("%H:%M"),
                            end_t.strftime("%H:%M"),
                        ))

                elif lec_wd == today_wd and end_t <= now_time:
                    # Today but the end time has already passed
                    expired_ids.append(lec_id)
                    if lec_room_id:
                        room_slots_to_bust.append((
                            lec_day,
                            start_t.strftime("%H:%M"),
                            end_t.strftime("%H:%M"),
                        ))
                # else: future

            if not expired_ids:
                logger.info("purge_expired_lectures: nothing to purge at %s EAT", now_eat.strftime("%H:%M"))
                return {
                    "purged": 0,
                    "notifications_deleted": 0,
                    "eat_time": now_eat.strftime("%Y-%m-%d %H:%M"),
                }


            from sqlalchemy import delete
            from models import Notification, Lecture as LectureModel

            notif_result = db.session.execute(
                delete(Notification).where(Notification.lecture_id.in_(expired_ids))
            )

            lec_result = db.session.execute(
                delete(LectureModel).where(LectureModel.id.in_(expired_ids))
            )

            db.session.commit()

            # Bust room cache for every freed slot
            if _cache.is_available():
                for day, start_str, end_str in set(room_slots_to_bust):
                    _cache.cache_delete(f"rooms:avail:{day}:{start_str}:{end_str}")
                    logger.debug(
                        "Cache busted: rooms:avail:%s:%s:%s", day, start_str, end_str
                    )

            return {
                "purged": lec_result.rowcount,
                "notifications_deleted": notif_result.rowcount,
                "eat_time": now_eat.strftime("%Y-%m-%d %H:%M"),
            }

        except Exception as exc:
            logger.exception("purge_expired_lectures failed: %s", exc)
            db.session.rollback()
            raise self.retry(exc=exc)


# TASK 5 — Pre-warm room availability cache 
@celery_app.task(
    name="tasks.warm_room_cache",
    queue="default",
)
def warm_room_cache() -> dict:
    flask_app = _get_flask_app()
    with flask_app.app_context():
        from scheduler import get_available_rooms
        import cache as _cache

        if not _cache.is_available():
            return {"warmed": 0}

        days  = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
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
                _cache.cache_set(
                    f"rooms:avail:{day}:{start_str}:{end_str}",
                    [r.to_dict() for r in rooms],
                    ttl=360,
                )
                warmed += 1

        logger.info("warm_room_cache: warmed %d slots", warmed)
        return {"warmed": warmed}


# TASK 6 — Pre-warm course list cache per programme 
@celery_app.task(
    name="tasks.warm_course_cache",
    queue="default",
)
def warm_course_cache() -> dict:
    flask_app = _get_flask_app()
    with flask_app.app_context():
        from models import db, Course, Programme
        import cache as _cache

        if not _cache.is_available():
            return {"warmed": 0}

        programmes = Programme.query.with_entities(Programme.id).all()
        warmed = 0

        for (prog_id,) in programmes:
            courses = (
                Course.query
                .filter_by(programme_id=prog_id)
                .order_by(Course.year, Course.semester, Course.code)
                .all()
            )
            _cache.cache_set(
                f"courses:prog:{prog_id}",
                [
                    {"id": c.id, "code": c.code, "name": c.name,
                     "year": c.year, "semester": c.semester}
                    for c in courses
                ],
                ttl=600,
            )
            warmed += 1

        logger.info("warm_course_cache: warmed %d programmes", warmed)
        return {"warmed": warmed}