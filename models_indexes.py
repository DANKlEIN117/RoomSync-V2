from models import db  # noqa: F401 (import for context)

# User
# class User(db.Model):
#     __table_args__ = (
#         db.Index("ix_user_email",  "email",  unique=True),
#         db.Index("ix_user_role",   "role"),
#     )

# Enrollment
# class Enrollment(db.Model):
#     __table_args__ = (
#         db.UniqueConstraint("student_id", "course_id", name="uq_enrollment"),
#         db.Index("ix_enrollment_student", "student_id"),
#         db.Index("ix_enrollment_course",  "course_id"),
#     )

# Lecture 
# class Lecture(db.Model):
#     __table_args__ = (
#         db.Index("ix_lecture_lecturer",    "lecturer_id"),
#         db.Index("ix_lecture_course",      "course_id"),
#         db.Index("ix_lecture_day_time",    "day", "start_time", "end_time"),
#     )

# Notification 
# class Notification(db.Model):
#     __table_args__ = (
#         db.Index("ix_notif_user_read", "user_id", "is_read"),
#         db.Index("ix_notif_lecture",   "lecture_id"),
#     )

# Course 
# class Course(db.Model):
#     __table_args__ = (
#         db.Index("ix_course_programme", "programme_id"),
#         db.Index("ix_course_lecturer",  "lecturer_id"),
#     )
