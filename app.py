"""
JKUAT Lecture Scheduler — Production-Ready Flask App  (v2)
==========================================================
Changes from v1:
  - Notification fan-out moved entirely to Celery (tasks.py)
  - Stampede-safe cache via XFetch / Probabilistic Early Recomputation (cache.py)
  - Per-user enrollment cache with explicit invalidation on enroll/unenroll
  - Distributed Redis lock on room booking to close the race-condition window
  - Bulk-delete of notifications delegated to a Celery task on lecture delete
  - DB connection pool tuned for Gunicorn multi-worker deployment

Run with Gunicorn:
    gunicorn -c gunicorn.conf.py "app:app"

Start Celery worker:
    celery -A tasks worker -Q notifications,default --concurrency=4 --loglevel=info

Required env vars:
    DATABASE_URL            postgresql://user:pass@host:5432/dbname
    REDIS_URL               redis://localhost:6379/0
    CELERY_BROKER_URL       redis://localhost:6379/1
    FLASK_SECRET_KEY        some-long-random-string
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from functools import wraps

from dotenv import load_dotenv
from flask import (Flask, jsonify, redirect, render_template,
                   request, session, url_for, flash)
from flask_login import (LoginManager, current_user, login_required,
                         login_user, logout_user)
from flask_migrate import Migrate
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload
from werkzeug.security import check_password_hash, generate_password_hash

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
logger = logging.getLogger(__name__)

from models import (Course, Enrollment, Lecture, Notification,
                    Programme, Room, School, User, db)
from scheduler import (check_lecturer_conflict, get_available_rooms,
                       seed_rooms, seed_sample_courses, seed_schools_and_programmes)
import cache as _cache

# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------
app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ["FLASK_SECRET_KEY"]

_db_url = os.getenv("DATABASE_URL", "sqlite:///db.sqlite3")
if _db_url.startswith("postgres://"):
    _db_url = _db_url.replace("postgres://", "postgresql://", 1)

app.config["SQLALCHEMY_DATABASE_URI"] = _db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_size": 5,
    "max_overflow": 10,
    "pool_pre_ping": True,
    "pool_recycle": 1800,
}
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(minutes=30)

db.init_app(app)
migrate = Migrate(app, db)
_cache.init_cache(os.getenv("REDIS_URL"))

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"
login_manager.login_message = None


@login_manager.user_loader
def load_user(user_id: str):
    return db.session.get(User, int(user_id))


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PAGE_SIZE_LECTURES       = 20
PAGE_SIZE_NOTIFICATIONS  = 15
PAGE_SIZE_ADMIN_STUDENTS = 50
PAGE_SIZE_ADMIN_LECTURES = 30


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
@app.before_request
def refresh_session():
    if current_user.is_authenticated:
        session.permanent = True


def _role_redirect(role: str):
    dest = {"admin": "admin_dashboard", "lecturer": "lecturer_dashboard"}.get(
        role, "student_dashboard"
    )
    return redirect(url_for(dest))


def _admin_only(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role != "admin":
            flash("Access denied.", "error")
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper


def _enrolled_ids_for(user_id: int):
    """Return cached list of enrolled course_ids for a student."""
    return _cache.get_enrolled_ids(
        user_id,
        fallback_fn=lambda: db.session.execute(
            text("SELECT course_id FROM enrollment WHERE student_id = :uid"),
            {"uid": user_id},
        ).scalars().all(),
    )


# ===========================================================================
# LANDING
# ===========================================================================
@app.route("/")
def landing():
    return render_template("landing.html")


# ===========================================================================
# AUTH
# ===========================================================================
@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        first_name = request.form.get("first_name", "").strip()
        last_name  = request.form.get("last_name",  "").strip()
        email      = request.form.get("email",      "").strip().lower()
        password   = request.form.get("password",   "")

        if not all([first_name, last_name, email, password]):
            flash("All fields are required.", "error")
            return render_template("register.html")

        if User.query.filter_by(email=email).first():
            flash("That email is already registered.", "error")
            return render_template("register.html")

        if email.endswith("@students.jkuat.ac.ke"):
            role = "student"
        elif email.endswith("@jkuat.ac.ke"):
            role = "lecturer"
        else:
            flash("Use a valid JKUAT email address.", "error")
            return render_template("register.html")

        user = User(
            name=f"{first_name} {last_name}".strip(),
            email=email,
            password=generate_password_hash(password),
            role=role,
        )
        db.session.add(user)
        db.session.commit()
        flash("Account created! Please log in.", "success")
        return redirect(url_for("login"))

    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return _role_redirect(current_user.role)

    if request.method == "POST":
        email    = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        user     = User.query.filter_by(email=email).first()

        if user and check_password_hash(user.password, password):
            login_user(user)
            session.permanent = True
            return _role_redirect(user.role)

        flash("Invalid email or password.", "error")

    return render_template("login.html")


@app.route("/logout")
@login_required
def logout():
    logout_user()
    session.clear()
    flash("You have been logged out.", "success")
    return redirect(url_for("login"))


# ===========================================================================
# STUDENT DASHBOARD
# ===========================================================================
@app.route("/student")
@login_required
def student_dashboard():
    if current_user.role != "student":
        return redirect(url_for("login"))

    enrolled_ids = _enrolled_ids_for(current_user.id)
    page         = request.args.get("page", 1, type=int)

    lectures_pag = None
    if enrolled_ids:
        lectures_pag = (
            Lecture.query
            .options(joinedload(Lecture.room), joinedload(Lecture.course))
            .filter(Lecture.course_id.in_(enrolled_ids))
            .order_by(Lecture.day, Lecture.start_time)
            .paginate(page=page, per_page=PAGE_SIZE_LECTURES, error_out=False)
        )

    notif_page = request.args.get("notif_page", 1, type=int)
    notifications_pag = (
        Notification.query
        .filter_by(user_id=current_user.id)
        .order_by(Notification.created_at.desc())
        .paginate(page=notif_page, per_page=PAGE_SIZE_NOTIFICATIONS, error_out=False)
    )

    return render_template(
        "student_dashboard.html",
        user=current_user,
        lectures_pag=lectures_pag,
        notifications_pag=notifications_pag,
        unread=current_user.unread_count(),
    )


# ===========================================================================
# NOTIFICATIONS — mark read
# ===========================================================================
@app.route("/notifications/mark-read", methods=["POST"])
@login_required
def mark_notifications_read():
    Notification.query.filter_by(
        user_id=current_user.id, is_read=False
    ).update({"is_read": True}, synchronize_session=False)
    db.session.commit()
    return jsonify({"ok": True})


# ===========================================================================
# STUDENT PROFILE
# ===========================================================================
@app.route("/profile")
@login_required
def profile():
    if current_user.role != "student":
        return redirect(url_for("login"))

    schools  = School.query.order_by(School.name).all()
    enrolled = set(_enrolled_ids_for(current_user.id))

    enroll_page = request.args.get("page", 1, type=int)
    enrollments_pag = (
        Enrollment.query
        .options(joinedload(Enrollment.course).joinedload(Course.programme))
        .filter_by(student_id=current_user.id)
        .order_by(Enrollment.enrolled_at.desc())
        .paginate(page=enroll_page, per_page=20, error_out=False)
    )

    return render_template(
        "student_profile.html",
        user=current_user,
        schools=schools,
        enrolled=enrolled,
        enrollments_pag=enrollments_pag,
    )


@app.route("/enroll/<int:course_id>", methods=["POST"])
@login_required
def enroll(course_id: int):
    if current_user.role != "student":
        return jsonify({"error": "Only students can enroll."}), 403

    course = db.session.get(Course, course_id)
    if not course:
        return jsonify({"error": "Course not found."}), 404

    try:
        db.session.add(Enrollment(student_id=current_user.id, course_id=course_id))
        db.session.commit()
        _cache.invalidate_enrollment_cache(current_user.id)
        return jsonify({"ok": True, "message": f"Enrolled in {course.code} — {course.name}"})
    except IntegrityError:
        db.session.rollback()
        return jsonify({"ok": False, "message": "Already enrolled in this course."})


@app.route("/unenroll/<int:course_id>", methods=["POST"])
@login_required
def unenroll(course_id: int):
    if current_user.role != "student":
        return jsonify({"error": "Forbidden"}), 403

    e = Enrollment.query.filter_by(
        student_id=current_user.id, course_id=course_id
    ).first_or_404()
    db.session.delete(e)
    db.session.commit()
    _cache.invalidate_enrollment_cache(current_user.id)
    return jsonify({"ok": True, "message": "Unenrolled successfully."})


# ===========================================================================
# API — courses by programme  (stampede-safe)
# ===========================================================================
@app.route("/api/courses/<int:programme_id>")
@login_required
def api_courses(programme_id: int):
    enrolled_ids = set(_enrolled_ids_for(current_user.id))

    raw = _cache.stampede_safe_get(
        key=f"courses:prog:{programme_id}",
        recompute_fn=lambda: [
            {"id": c.id, "code": c.code, "name": c.name,
             "year": c.year, "semester": c.semester}
            for c in Course.query
            .filter_by(programme_id=programme_id)
            .order_by(Course.year, Course.semester, Course.code)
            .all()
        ],
        ttl=300,
    )

    for item in raw:
        item["enrolled"] = item["id"] in enrolled_ids

    return jsonify(raw)


# ===========================================================================
# LECTURER DASHBOARD
# ===========================================================================
@app.route("/lecturer")
@login_required
def lecturer_dashboard():
    if current_user.role != "lecturer":
        return redirect(url_for("login"))

    page = request.args.get("page", 1, type=int)
    lectures_pag = (
        Lecture.query
        .options(joinedload(Lecture.room), joinedload(Lecture.course))
        .filter_by(lecturer_id=current_user.id)
        .order_by(Lecture.day, Lecture.start_time)
        .paginate(page=page, per_page=PAGE_SIZE_LECTURES, error_out=False)
    )

    courses = (
        Course.query
        .filter_by(lecturer_id=current_user.id)
        .order_by(Course.programme_id, Course.year, Course.code)
        .all()
    )

    return render_template(
        "lecturer_dashboard.html",
        user=current_user,
        lectures_pag=lectures_pag,
        courses=courses,
    )


# ===========================================================================
# ROOMS AVAILABLE — stampede-safe
# ===========================================================================
@app.route("/rooms/available", methods=["POST"])
@login_required
def available_rooms():
    day       = request.form.get("day",        "").strip()
    start_str = request.form.get("start_time", "").strip()
    end_str   = request.form.get("end_time",   "").strip()

    if not all([day, start_str, end_str]):
        return jsonify({"error": "Missing fields."}), 400

    try:
        start = datetime.strptime(start_str, "%H:%M").time()
        end   = datetime.strptime(end_str,   "%H:%M").time()
    except ValueError:
        return jsonify({"error": "Invalid time format."}), 400

    if start >= end:
        return jsonify({"error": "Start must be before end."}), 400

    rooms = _cache.stampede_safe_get(
        key=f"rooms:avail:{day}:{start_str}:{end_str}",
        recompute_fn=lambda: [r.to_dict() for r in get_available_rooms(day, start, end)],
        ttl=30,
    )
    return jsonify({"rooms": rooms})


# ===========================================================================
# CREATE LECTURE
# ===========================================================================
@app.route("/create-lecture", methods=["POST"])
@login_required
def create_lecture():
    if current_user.role != "lecturer":
        flash("Access denied.", "error")
        return redirect(url_for("login"))

    course_id = request.form.get("course_id", "").strip()
    room_id   = request.form.get("room_id",   "").strip()
    day       = request.form.get("day",       "").strip()
    start_str = request.form.get("start_time","").strip()
    end_str   = request.form.get("end_time",  "").strip()

    if not all([course_id, room_id, day, start_str, end_str]):
        flash("All fields are required.", "error")
        return redirect(url_for("lecturer_dashboard"))

    try:
        start     = datetime.strptime(start_str, "%H:%M").time()
        end       = datetime.strptime(end_str,   "%H:%M").time()
        course_id = int(course_id)
        room_id   = int(room_id)
    except ValueError:
        flash("Invalid data submitted.", "error")
        return redirect(url_for("lecturer_dashboard"))

    if start >= end:
        flash("Start time must be before end time.", "error")
        return redirect(url_for("lecturer_dashboard"))

    course = db.session.get(Course, course_id)
    if not course or course.lecturer_id != current_user.id:
        flash("You are not assigned to that course.", "error")
        return redirect(url_for("lecturer_dashboard"))

    if check_lecturer_conflict(current_user.id, day, start, end):
        flash("You already have a lecture during this time slot.", "error")
        return redirect(url_for("lecturer_dashboard"))

    # ── Distributed lock: atomic Redis SET NX prevents double-booking ─────────
    if not _cache.acquire_room_lock(day, start_str, end_str, room_id):
        flash("That room is being booked right now. Please try again in a moment.", "error")
        return redirect(url_for("lecturer_dashboard"))

    try:
        # DB is still the final source of truth — check again inside the lock
        free_ids = {r.id for r in get_available_rooms(day, start, end)}
        if room_id not in free_ids:
            flash("That room was just taken. Please select another.", "error")
            return redirect(url_for("lecturer_dashboard"))

        lecture = Lecture(
            course_id=course_id, room_id=room_id,
            day=day, start_time=start, end_time=end,
            lecturer_id=current_user.id,
        )
        db.session.add(lecture)
        db.session.commit()

    finally:
        _cache.release_room_lock(day, start_str, end_str, room_id)

    # ── Offload fan-out to Celery — response returns in ~5 ms ─────────────────
    from tasks import dispatch_lecture_notifications
    dispatch_lecture_notifications.delay(lecture.id)

    _cache.cache_delete(f"rooms:avail:{day}:{start_str}:{end_str}")

    room = db.session.get(Room, room_id)
    flash(
        f'"{course.name}" scheduled in {room.name} — {day} {start_str}–{end_str}. '
        "Students will be notified shortly.",
        "success",
    )
    return redirect(url_for("lecturer_dashboard"))


# ===========================================================================
# DELETE LECTURE
# ===========================================================================
@app.route("/delete-lecture/<int:lecture_id>", methods=["POST"])
@login_required
def delete_lecture(lecture_id: int):
    lecture = db.session.get(Lecture, lecture_id)
    if not lecture:
        flash("Lecture not found.", "error")
        return redirect(url_for("lecturer_dashboard"))

    if lecture.lecturer_id != current_user.id:
        flash("You can only delete your own lectures.", "error")
        return redirect(url_for("lecturer_dashboard"))

    start_str = lecture.start_time.strftime("%H:%M")
    end_str   = lecture.end_time.strftime("%H:%M")
    day       = lecture.day

    db.session.delete(lecture)
    db.session.commit()

    # Bulk-delete notifications in the background — not in the request cycle
    from tasks import bulk_delete_notifications
    bulk_delete_notifications.delay(lecture_id)

    _cache.cache_delete(f"rooms:avail:{day}:{start_str}:{end_str}")
    flash("Lecture deleted.", "success")
    return redirect(url_for("lecturer_dashboard"))


# ===========================================================================
# ADMIN DASHBOARD
# ===========================================================================
@app.route("/admin/dashboard")
@login_required
def admin_dashboard():
    if current_user.role != "admin":
        flash("Access denied.", "error")
        return redirect(url_for("login"))

    rooms      = Room.query.all()
    schools    = School.query.order_by(School.name).all()
    programmes = Programme.query.order_by(Programme.name).all()
    courses    = Course.query.order_by(Course.code).all()
    lecturers  = User.query.filter_by(role="lecturer").order_by(User.name).all()

    stud_page = request.args.get("stud_page", 1, type=int)
    students_pag = (
        User.query.filter_by(role="student")
        .order_by(User.name)
        .paginate(page=stud_page, per_page=PAGE_SIZE_ADMIN_STUDENTS, error_out=False)
    )

    total_lectures     = db.session.execute(text("SELECT COUNT(*) FROM lecture")).scalar()
    assigned_courses   = db.session.execute(
        text("SELECT COUNT(*) FROM course WHERE lecturer_id IS NOT NULL")
    ).scalar()
    unassigned_courses = db.session.execute(
        text("SELECT COUNT(*) FROM course WHERE lecturer_id IS NULL")
    ).scalar()

    lect_page = request.args.get("lect_page", 1, type=int)
    recent_lectures_pag = (
        Lecture.query
        .options(joinedload(Lecture.course), joinedload(Lecture.room))
        .order_by(Lecture.id.desc())
        .paginate(page=lect_page, per_page=PAGE_SIZE_ADMIN_LECTURES, error_out=False)
    )

    return render_template(
        "admin_dashboard.html",
        rooms=rooms, schools=schools, programmes=programmes,
        courses=courses, lecturers=lecturers,
        students_pag=students_pag,
        total_lectures=total_lectures,
        assigned_courses=assigned_courses,
        unassigned_courses=unassigned_courses,
        recent_lectures_pag=recent_lectures_pag,
    )


@app.route("/admin/seed-rooms")
@login_required
@_admin_only
def admin_seed_rooms():
    seed_rooms()
    flash("Rooms seeded successfully.", "success")
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/seed-schools")
@login_required
@_admin_only
def admin_seed_schools():
    seed_schools_and_programmes()
    flash("Schools and programmes seeded.", "success")
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/seed-courses")
@login_required
@_admin_only
def admin_seed_courses():
    seed_sample_courses()
    flash("Sample courses seeded.", "success")
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/assign-lecturer", methods=["POST"])
@login_required
@_admin_only
def admin_assign_lecturer():
    course_id   = int(request.form.get("course_id"))
    lecturer_id = int(request.form.get("lecturer_id"))
    course   = db.session.get(Course, course_id)
    lecturer = db.session.get(User, lecturer_id)

    if not course or not lecturer:
        flash("Course or lecturer not found.", "error")
        return redirect(url_for("admin_dashboard"))

    if lecturer.role != "lecturer":
        flash("Selected user is not a lecturer.", "error")
    else:
        course.lecturer_id = lecturer_id
        db.session.commit()
        _cache.cache_delete(f"courses:prog:{course.programme_id}")
        flash(f"{lecturer.name} assigned to {course.code} — {course.name}.", "success")

    return redirect(url_for("admin_dashboard"))


# ===========================================================================
# Health check
# ===========================================================================
@app.route("/health")
def health():
    db_ok = False
    try:
        db.session.execute(text("SELECT 1"))
        db_ok = True
    except Exception:  # noqa: BLE001
        pass

    redis_ok = False
    if _cache.is_available():
        try:
            from cache import _redis as _r
            _r.ping()
            redis_ok = True
        except Exception:  # noqa: BLE001
            pass

    return jsonify({"db": db_ok, "redis": redis_ok}), (200 if db_ok else 503)


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 5000)),
        debug=os.getenv("FLASK_DEBUG", "false").lower() == "true",
    )