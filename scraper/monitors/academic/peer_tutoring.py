import datetime as dt
import re
from zoneinfo import ZoneInfo

import bs4
import requests

from scraper.monitors.base_scraper import BaseScraper
from scraper.models import PeerTutoring

# Timezone constant for CMU events
PT_TIMEZONE = ZoneInfo("America/New_York")

# Default tutoring time (8:00pm - 10:00pm)
DEFAULT_START_TIME = dt.time(20, 0)  # 8:00pm
DEFAULT_END_TIME = dt.time(22, 0)    # 10:00pm


class PeerTutoringScraper(BaseScraper):
    def __init__(self, db):
        super().__init__(db, "Peer Tutoring", "SASC")
        # https://www.cmu.edu/student-success/programs/tutoring.html

    def scrape(self):
        peer_tutoring_sessions = self.scrape_data_only()
        # TODO: Store in database

    def scrape_data_only(self) -> list[PeerTutoring]:
        pt_url = "https://www.cmu.edu/student-success/programs/tutoring.html"
        pt_html = requests.get(pt_url).content

        peer_tutoring_sessions = self._process_html(pt_html)

        return peer_tutoring_sessions

    def _process_html(self, pt_html) -> list[PeerTutoring]:
        soup = bs4.BeautifulSoup(pt_html, 'html.parser')

        table = soup.find(id="dropintable")
        if not table:
            print("Could not find dropintable in HTML")
            return []

        # Find all rows, skip header row (first row in thead)
        rows = table.find_all("tr")[1:]  # Skip the header row

        all_peer_tutoring_sessions = []

        for row in rows:
            # Get the course info from <th> element
            th = row.find("th")
            if not th:
                continue

            # Parse courses - they're separated by <br> tags
            courses = self._parse_courses(th)

            cols = row.find_all("td")
            if len(cols) < 3:
                continue

            # Day of the week
            day_of_week = cols[0].get_text(strip=True)

            # Location and optional custom time
            location_cell = cols[1]
            location, start_time, end_time = self._parse_location_and_time(location_cell)

            # Tutors - can be multiple separated by & or commas
            tutors_text = cols[2].get_text(strip=True)
            tutors = [name.strip() for name in re.split(r"[,&]+\s*", tutors_text) if name.strip()]

            # Generate time location data
            time_location = self._generate_time_location(day_of_week, start_time, end_time, location)

            # Create one PeerTutoring object per course
            for course in courses:
                session = PeerTutoring(
                    course_num=course["course_num"],
                    course_name=course["course_name"],
                    tutors=tutors,
                    time_location=time_location
                )
                all_peer_tutoring_sessions.append(session)
                print(f"Peer Tutoring {course['course_num']}: {day_of_week} at {location}")

        return all_peer_tutoring_sessions

    def _parse_courses(self, th_element) -> list[dict]:
        """Parse course info from the <th> element.
        
        Courses are formatted like "15-110 Principles of Computing"
        Multiple courses are separated by <br> tags.
        """
        courses = []

        # Get text nodes separated by <br> tags
        for content in th_element.stripped_strings:
            content = content.strip()
            if not content:
                continue

            # Parse course number and name: "15-110 Principles of Computing"
            match = re.match(r'^(\d{2}-\d{3})\s+(.+)$', content)
            if match:
                course_num = match.group(1).replace("-", "")
                course_name = match.group(2)
                courses.append({
                    "course_num": course_num,
                    "course_name": course_name
                })

        return courses

    def _parse_location_and_time(self, location_cell) -> tuple[str, dt.time, dt.time]:
        """Parse location and optional custom time from location cell.
        
        The location cell may contain a custom time in <strong> tags like:
        "Highmark Tartan Room<br><strong>(7:30pm - 9:30pm)</strong>"
        
        If no custom time, use default 8:00pm - 10:00pm.
        """
        # Check for custom time in <strong> tag
        strong_tag = location_cell.find("strong")
        
        if strong_tag:
            time_text = strong_tag.get_text(strip=True)
            start_time, end_time = self._parse_time_range(time_text)
            
            # Get location without the time part
            # Remove the strong tag temporarily to get clean location
            strong_tag.decompose()
            location = location_cell.get_text(strip=True)
        else:
            location = location_cell.get_text(strip=True)
            start_time = DEFAULT_START_TIME
            end_time = DEFAULT_END_TIME

        return location, start_time, end_time

    def _parse_time_range(self, time_text: str) -> tuple[dt.time, dt.time]:
        """Parse time range like '(7:30pm - 9:30pm)' or '7:30pm - 9:30pm'."""
        # Remove parentheses if present
        time_text = time_text.strip("()")
        
        # Split by "-" and parse each time
        parts = time_text.split("-")
        if len(parts) != 2:
            return DEFAULT_START_TIME, DEFAULT_END_TIME

        start_str = parts[0].strip().replace(" ", "")
        end_str = parts[1].strip().replace(" ", "")

        try:
            start_time = dt.datetime.strptime(start_str, "%I:%M%p").time()
            end_time = dt.datetime.strptime(end_str, "%I:%M%p").time()
            return start_time, end_time
        except ValueError:
            return DEFAULT_START_TIME, DEFAULT_END_TIME

    def _generate_time_location(self, day_of_week: str, start_time: dt.time, end_time: dt.time, location: str) -> dict:
        """Generate a time location dictionary.
        
        Returns: dict with the following keys:
            - recurrence_frequency: "WEEKLY"
            - recurrence_interval: 1
            - recurrence_by_day: "MO", "TU", etc.
            - start_datetime: datetime ISO string
            - end_datetime: datetime ISO string
            - location: location string
        """
        weekday_code = self._weekday_name_to_code(day_of_week)

        # Create timezone-aware datetime objects (date component is arbitrary)
        arbitrary_date = dt.date.today()
        start_datetime = dt.datetime.combine(arbitrary_date, start_time, tzinfo=PT_TIMEZONE)
        end_datetime = dt.datetime.combine(arbitrary_date, end_time, tzinfo=PT_TIMEZONE)

        time_location_data = {
            "recurrence_frequency": "WEEKLY",
            "recurrence_interval": 1,
            "recurrence_by_day": weekday_code,
            "start_datetime": start_datetime.isoformat(),
            "end_datetime": end_datetime.isoformat(),
            "location": location
        }

        return time_location_data

    def _weekday_name_to_code(self, weekday_name: str) -> str:
        """Convert weekday name directly to two-letter code ('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU')."""
        mapping = {
            'Monday': 'MO',
            'Tuesday': 'TU',
            'Wednesday': 'WE',
            'Thursday': 'TH',
            'Friday': 'FR',
            'Saturday': 'SA',
            'Sunday': 'SU'
        }
        # Handle both singular and plural forms
        day = weekday_name.rstrip('s').strip()
        return mapping.get(day, 'MO')

