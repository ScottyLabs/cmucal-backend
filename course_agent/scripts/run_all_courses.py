# course-agent/scripts/run_all_courses.py
from course_agent.app.env import load_env

load_env()

from course_agent.app.db.repositories import fetch_all_courses, get_or_build_category_for_course, insert_agent_run
from course_agent.app.agent.graph import build_course_agent
from course_agent.app.services.csv_export import write_courses_csv
from course_agent.app.services.fake_courses import fake_courses


def main():
    # courses = fetch_all_courses()
    courses = fake_courses

    # Create a new agent run record, should increment agent_version if needed
    agent_version = "v1.0"
    agent_run_id = insert_agent_run(agent_version=agent_version)

    agent = build_course_agent()

    # print the summary of courses being processed
    total_courses = len(courses)
    print(f"Total courses to process: {total_courses}")
    course_no_sites = 0
    course_no_calendars = 0
    successful_courses = 0

    course_no_calendars_data = []
    course_no_sites_data = []
    successful_courses_data = []

    for course in courses:
        category_id = get_or_build_category_for_course(course)
        final_state = agent.invoke(
            {
                "course_id": course["id"],
                "org_id": course["org_id"],
                "category_id": category_id,
                "course_number": course["course_number"],
                "course_name": course["course_name"],
                "agent_run_id": agent_run_id,
                "event_type": "ACADEMIC",
                "candidate_urls": [],
                "current_url_index": 0,
                "verified_site_id": None,
                "iframe_url": None,
                "ical_link": None,
                "terminal_status": None,
                "done": False,
            }
        )
        print(
            f"Course {course['course_number']} - {course['course_name']} completed with status: {final_state.get('terminal_status')}"
        )

        if final_state.get("terminal_status") == "no_site_found":
            course_no_sites += 1
            course_no_sites_data.append(course)
        elif final_state.get("terminal_status") == "no_calendar":
            course_no_calendars += 1
            course_no_calendars_data.append(course)
        else:
            successful_courses += 1
            successful_courses_data.append(course)

    print("\n=== Summary ===")
    print(f"Total courses processed: {total_courses}")
    print(f"Successful courses: {successful_courses}")
    print(f"Courses with no site found: {course_no_sites}")
    print(f"Courses with no calendar found: {course_no_calendars}")

    # Export results to CSV files
    write_courses_csv(successful_courses_data, "successful_courses.csv")

    write_courses_csv(course_no_sites_data, "courses_no_site_found.csv")

    write_courses_csv(course_no_calendars_data, "courses_no_calendar_found.csv")


if __name__ == "__main__":
    main()
