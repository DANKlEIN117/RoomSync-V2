from models import db, Lecture, Room, School, Programme, Course, Notification, Enrollment

def get_available_rooms(day: str, start_time, end_time, exclude_lecture_id: int = None):

    conflict_q = db.session.query(Lecture.room_id).filter(
        Lecture.day        == day,
        Lecture.start_time <  end_time,
        Lecture.end_time   >  start_time,
    )
    if exclude_lecture_id:
        conflict_q = conflict_q.filter(Lecture.id != exclude_lecture_id)

    booked_ids = [r[0] for r in conflict_q.all()]

    return Room.query.filter(
        Room.is_active == True,
        ~Room.id.in_(booked_ids) if booked_ids else True,
    ).order_by(Room.capacity.asc()).all()


def check_lecturer_conflict(lecturer_id: int, day: str, start_time, end_time,
                             exclude_lecture_id: int = None) -> bool:
    q = Lecture.query.filter(
        Lecture.lecturer_id == lecturer_id,
        Lecture.day         == day,
        Lecture.start_time  <  end_time,
        Lecture.end_time    >  start_time,
    )
    if exclude_lecture_id:
        q = q.filter(Lecture.id != exclude_lecture_id)
    return q.first() is not None


def notify_enrolled_students(lecture):
    """
    Create a Notification row for every student enrolled in the lecture's course.
    Called immediately after a Lecture is saved (before final commit).
    """
    course    = lecture.course
    room      = lecture.room_ref
    start_str = lecture.start_time.strftime('%H:%M')
    end_str   = lecture.end_time.strftime('%H:%M')

    message = (
        f"📅 New lecture scheduled: {course.code} — {course.name} | "
        f"{lecture.day} {start_str}–{end_str} | Room: {room.name}"
    )

    enrollments = Enrollment.query.filter_by(course_id=course.id).all()

    notifications = [
        Notification(
            user_id    = e.student_id,
            lecture_id = lecture.id,
            message    = message,
            is_read    = False,
        )
        for e in enrollments
    ]

    db.session.add_all(notifications)
    return len(notifications)   # return count for flash message



# SEED — ROOMS
def seed_rooms():
    rooms = [
        ("LT1", 200, "lecture_hall", "Main Block"),
        ("LT2", 200, "lecture_hall", "Main Block"),
        ("LT3", 150, "lecture_hall", "Science Block"),
        ("LT4", 150, "lecture_hall", "Science Block"),
        ("LT5", 100, "lecture_hall", "Engineering Block"),
        ("LT6", 100, "lecture_hall", "Engineering Block"),
        ("Lab 1", 40, "lab", "ICT Block"),
        ("Lab 2", 40, "lab", "ICT Block"),
        ("Lab 3", 35, "lab", "ICT Block"),
        ("Lab 4", 35, "lab", "Science Block")
    ]

    for name, capacity, room_type, building in rooms:
        existing = Room.query.filter_by(name=name).first()

        if existing:
            continue

        db.session.add(Room(
            name=name,
            capacity=capacity,
            room_type=room_type,
            building=building
        ))

    db.session.commit()

# SEED — SCHOOLS & PROGRAMMES

def seed_schools_and_programmes():

    data = [
        {
            'name': 'College Of Engineering and Technology',
            'code': 'CoETEC',
            'description': '',
            'programmes': [
                {'name': 'School of Biosystems and Environmental Engineering', 'code': 'SOBEE', 'duration': 4},
                {'name': 'School of Civil, Environmental and Geospatial Engineering', 'code': 'SCEGE', 'duration': 4},
                {'name': 'School of Electrical Electronic and Information Engineering', 'code': 'SEEIE', 'duration': 4},
                {'name': 'School of Mechanical, Manufacturing and Material Engineering', 'code': 'SOMMME', 'duration': 4},
            ],
        },
        {
            'name': 'College Of Health Sciences',
            'code': 'CoHES',
            'description': '',
            'programmes': [
                {'name': 'School of Biomedical Sciences', 'code': 'SBS', 'duration': 5},
                {'name': 'School of Nursing', 'code': 'SON', 'duration': 5},
                {'name': 'School of Medicine', 'code': 'SOM', 'duration': 5},
                {'name': 'School of Pharmacy', 'code': 'SOP', 'duration': 5},
                {'name': 'School of Public Health', 'code': 'SPH', 'duration': 5},
            ],
        },
        {
            'name': 'College of Applied Sciences',
            'code': 'COPAS',
            'description': '',
            'programmes': [
                {'name': 'School of Computing and Information Technology', 'code': 'SCIT', 'duration': 4},
                {'name': 'School of Biological Sciences', 'code': 'SBS2', 'duration': 4},
                {'name': 'School of Medical and Physical Sciences', 'code': 'SMPS', 'duration': 4},
            ],
        },
        {
            'name': 'College of Human Resource and Development',
            'code': 'CoHRED',
            'description': '',
            'programmes': [
                {'name': 'School of Business and Entrepreneurship', 'code': 'SOBE', 'duration': 4},
                {'name': 'School of Communication and Development Studies', 'code': 'SCDC', 'duration': 4},
            ],
        },
        {
            'name': 'College of Agriculture and Natural Resources',
            'code': 'COANRE',
            'description': '',
            'programmes': [
                {'name': 'School of Food Science and Nutritional Sciences', 'code': 'SOFNUS', 'duration': 4},
                {'name': 'School of Agricultural and Environmental Sciences', 'code': 'SOAES', 'duration': 4},
                {'name': 'School of Natural Resource and Animal Science', 'code': 'SONRAS', 'duration': 4},
            ],
        },
        {
            'name': 'School of Law (Karen Campus)',
            'code': 'LAW',
            'description': 'Standalone school located at Karen Campus.',
            'programmes': [
                {'name': 'Bachelor of Laws', 'code': 'LLB', 'duration': 4},
            ],
        },
    ]
    

    for s in data:
        school = School(name=s['name'], code=s['code'], description=s['description'])
        db.session.add(school)
        db.session.flush()

        for p in s['programmes']:
            existing = Programme.query.filter_by(code=p['code']).first()

            if existing:
                continue

            prog = Programme(
                name=p['name'],
                code=p['code'],
                duration=p['duration'],
                school_id=school.id
            )
            db.session.add(prog)

    db.session.commit()

    total_progs = sum(len(s['programmes']) for s in data)
    print(f"✅ Seeded {len(data)} colleges and {total_progs} schools.")

# SEED — SAMPLE COURSES  (BIT only as example)
def seed_sample_courses():

    # fetch programmes safely
    scit = Programme.query.filter_by(code='SCIT').first()
    sobe = Programme.query.filter_by(code='SOBE').first()
    seeie = Programme.query.filter_by(code='SEEIE').first()
    sommme = Programme.query.filter_by(code='SOMMME').first()

    if not scit:
        print("SCIT programme not found. Run seed_schools_and_programmes() first.")
        return

    courses = [
        Course(code='SCIT 1101', name='Introduction to Programming', year=1, semester=1, programme_id=scit.id),
        Course(code='SCIT 1102', name='Computer Fundamentals', year=1, semester=1, programme_id=scit.id),
        Course(code='SCIT 1201', name='Data Structures', year=1, semester=2, programme_id=scit.id),

        Course(code='SCIT 2101', name='Object Oriented Programming', year=2, semester=1, programme_id=scit.id),
        Course(code='SCIT 2102', name='Database Systems', year=2, semester=1, programme_id=scit.id),
        Course(code='SCIT 2201', name='Computer Networks', year=2, semester=2, programme_id=scit.id),

        Course(code='SCIT 3101', name='Web Development', year=3, semester=1, programme_id=scit.id),
        Course(code='SCIT 3102', name='Software Engineering', year=3, semester=1, programme_id=scit.id),
        Course(code='SCIT 3201', name='Mobile App Development', year=3, semester=2, programme_id=scit.id),

        Course(code='SOBE 1101', name='Introduction to Business', year=1, semester=1, programme_id=sobe.id),
        Course(code='SOBE 1102', name='Business Mathematics', year=1, semester=1, programme_id=sobe.id),
        Course(code='SOBE 2101', name='Principles of Management', year=2, semester=1, programme_id=sobe.id),

        Course(code='SEEIE 1101', name='Circuit Theory', year=1, semester=1, programme_id=seeie.id),
        Course(code='SEEIE 1102', name='Engineering Mathematics', year=1, semester=1, programme_id=seeie.id),
        Course(code='SEEIE 2101', name='Digital Electronics', year=2, semester=1, programme_id=seeie.id),

        Course(code='SOMMME 1101', name='Engineering Drawing', year=1, semester=1, programme_id=sommme.id),
        Course(code='SOMMME 1102', name='Statics', year=1, semester=1, programme_id=sommme.id),
        Course(code='SOMMME 2101', name='Thermodynamics', year=2, semester=1, programme_id=sommme.id),
    ]

    inserted = 0

    for course in courses:
        existing = Course.query.filter_by(code=course.code).first()

        if existing:
            continue

        db.session.add(course)
        inserted += 1

    db.session.commit()

    print(f"✅ Seeded {inserted} new courses safely.")