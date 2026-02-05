def build_orgs_and_courses(rows):
    orgs = {}
    courses = {}

    for soc in rows:
        key = (soc.course_num, soc.semester)

        if key not in orgs:
            formatted = f"{soc.course_num[:2]}-{soc.course_num[2:]}"
            orgs[key] = {
                "name": f"{formatted} {soc.course_name}",
                "type": "COURSE",
                "description": soc.course_name,
                "tags": ["SOC"],
            }

        courses[key] = {
            "course_number": soc.course_num,
            "course_name": soc.course_name,
            "semesters": [soc.semester],
        }

    return orgs, courses
