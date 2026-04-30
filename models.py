from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from datetime import datetime

db = SQLAlchemy()


# USER
class User(UserMixin, db.Model):
    __tablename__ = 'user'
    __table_args__ = (
        db.Index('ix_user_email', 'email', unique=True),
        db.Index('ix_user_role',  'role'),
    )

    id       = db.Column(db.Integer, primary_key=True)
    name     = db.Column(db.String(120), nullable=False)
    email    = db.Column(db.String(120), unique=True, nullable=False)
    password = db.Column(db.String(256), nullable=False)
    role     = db.Column(db.String(20),  nullable=False)

    lectures       = db.relationship('Lecture',      backref='lecturer',
                                     foreign_keys='Lecture.lecturer_id', lazy=True)
    enrollments    = db.relationship('Enrollment',   backref='student',  lazy=True)
    notifications  = db.relationship('Notification', backref='user',     lazy=True,
                                     order_by='Notification.created_at.desc()')
    taught_courses = db.relationship('Course',       backref='lecturer',
                                     foreign_keys='Course.lecturer_id', lazy=True)

    def unread_count(self):
        return Notification.query.filter_by(user_id=self.id, is_read=False).count()

    def __repr__(self):
        return f'<User {self.email} ({self.role})>'


# SCHOOL
class School(db.Model):
    __tablename__ = 'school'

    id          = db.Column(db.Integer, primary_key=True)
    name        = db.Column(db.String(120), unique=True, nullable=False)
    code        = db.Column(db.String(20),  unique=True, nullable=False)
    description = db.Column(db.String(255))

    programmes  = db.relationship('Programme', backref='school', lazy=True)

    def __repr__(self):
        return f'<School {self.code}>'


# PROGRAMME
class Programme(db.Model):
    __tablename__ = 'programme'

    id        = db.Column(db.Integer, primary_key=True)
    name      = db.Column(db.String(120), nullable=False)
    code      = db.Column(db.String(20),  nullable=False)
    duration  = db.Column(db.Integer, default=4)
    school_id = db.Column(db.Integer, db.ForeignKey('school.id'), nullable=False)

    courses   = db.relationship('Course', backref='programme', lazy=True)

    def __repr__(self):
        return f'<Programme {self.code}>'


# COURSE
class Course(db.Model):
    __tablename__ = 'course'
    __table_args__ = (
        db.Index('ix_course_programme', 'programme_id'),
        db.Index('ix_course_lecturer',  'lecturer_id'),
    )

    id           = db.Column(db.Integer, primary_key=True)
    code         = db.Column(db.String(20),  nullable=False)
    name         = db.Column(db.String(120), nullable=False)
    year         = db.Column(db.Integer, nullable=False)
    semester     = db.Column(db.Integer, nullable=False)

    programme_id = db.Column(db.Integer, db.ForeignKey('programme.id'), nullable=False)
    lecturer_id  = db.Column(db.Integer, db.ForeignKey('user.id'),      nullable=True)

    lectures    = db.relationship('Lecture',    backref='course', lazy=True)
    enrollments = db.relationship('Enrollment', backref='course', lazy=True)

    def display_name(self):
        return (
            f"{self.code} — {self.name} "
            f"({self.programme.code} Yr {self.year} Sem {self.semester})"
        )

    def __repr__(self):
        return f'<Course {self.code}>'


# ROOM
class Room(db.Model):
    __tablename__ = 'room'

    id        = db.Column(db.Integer, primary_key=True)
    name      = db.Column(db.String(50),  unique=True, nullable=False)
    capacity  = db.Column(db.Integer, nullable=False)
    room_type = db.Column(db.String(30),  nullable=False)
    building  = db.Column(db.String(60),  nullable=False)
    is_active = db.Column(db.Boolean, default=True, nullable=False)

    lectures  = db.relationship('Lecture', backref='room', lazy=True)

    def to_dict(self):
        return {
            'id':       self.id,
            'name':     self.name,
            'capacity': self.capacity,
            'type':     self.room_type,
            'building': self.building,
        }

    def __repr__(self):
        return f'<Room {self.name}>'


# LECTURE
class Lecture(db.Model):
    __tablename__ = 'lecture'
    __table_args__ = (
        db.Index('ix_lecture_lecturer', 'lecturer_id'),
        db.Index('ix_lecture_course',   'course_id'),
        db.Index('ix_lecture_day_time', 'day', 'start_time', 'end_time'),
    )

    id         = db.Column(db.Integer, primary_key=True)
    day        = db.Column(db.String(10), nullable=False)
    start_time = db.Column(db.Time, nullable=False)
    end_time   = db.Column(db.Time, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Optional per-lecture communication fields
    class_message = db.Column(db.Text,          nullable=True)   # prep notes / expectations
    online_link   = db.Column(db.String(512),   nullable=True)   # Zoom / Meet URL

    lecturer_id = db.Column(db.Integer, db.ForeignKey('user.id'),   nullable=False)
    room_id     = db.Column(db.Integer, db.ForeignKey('room.id'),   nullable=True)   # NULL for online lectures
    course_id   = db.Column(db.Integer, db.ForeignKey('course.id'), nullable=False)

    notifications = db.relationship('Notification', backref='lecture', lazy=True)

    def __repr__(self):
        return f'<Lecture course={self.course_id} {self.day} {self.start_time}>'


# ENROLLMENT
class Enrollment(db.Model):
    __tablename__ = 'enrollment'
    __table_args__ = (
        db.UniqueConstraint('student_id', 'course_id', name='uq_student_course'),
        db.Index('ix_enrollment_student', 'student_id'),
        db.Index('ix_enrollment_course',  'course_id'),
    )

    id          = db.Column(db.Integer, primary_key=True)
    student_id  = db.Column(db.Integer, db.ForeignKey('user.id'),   nullable=False)
    course_id   = db.Column(db.Integer, db.ForeignKey('course.id'), nullable=False)
    enrolled_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f'<Enrollment s={self.student_id} c={self.course_id}>'


# NOTIFICATION
class Notification(db.Model):
    __tablename__ = 'notification'
    __table_args__ = (
        db.Index('ix_notif_user_read', 'user_id', 'is_read'),
        db.Index('ix_notif_lecture',   'lecture_id'),
    )

    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey('user.id'),    nullable=False)
    lecture_id = db.Column(db.Integer, db.ForeignKey('lecture.id'), nullable=True)
    message    = db.Column(db.String(300), nullable=False)
    is_read    = db.Column(db.Boolean, default=False, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f'<Notification u={self.user_id} read={self.is_read}>'