import datetime
import bs4
import requests
import urllib3  # <-- 1. Import urllib3 to suppress warnings
from scraper.helpers.semester import get_current_semester
from scraper.monitors.base_scraper import BaseScraper
import re


class ScheduleOfClasses:
    def __init__(
        self,
        id,
        course_num,
        course_name,
        lecture_section,
        lecture_days,
        lecture_time_start,
        lecture_time_end,
        location,
        semester,
        sem_start,
        sem_end
    ):
        self.id = id
        self.course_num = course_num
        self.course_name = course_name
        self.lecture_section = lecture_section
        self.lecture_days = lecture_days
        self.lecture_time_start = lecture_time_start
        self.lecture_time_end = lecture_time_end
        self.location = location
        self.semester = semester
        self.sem_start = sem_start
        self.sem_end = sem_end

class ScheduleOfClassesScraper(BaseScraper):
    def __init__(self, db, semester_label: str):
        super().__init__(db, "Schedule of Classes", "SOC")

        (
            self.soc_layout,
            self.semester_label,
            self.sem_start,
            self.sem_end,
        ) = get_current_semester(semester_label)

        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
        }

        # --- 4. NEW FIX: Disable SSL verification for this session ---
        self.session.verify = False
        # Suppress only the InsecureRequestWarning
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        # --- END NEW FIX ---

    def scrape(self):
        print("Running Schedule of Classes scraper...")
        resources = self._fetch_courses()
        for resource in resources:
            print(f"Added course: {resource.course_num} - {resource.course_name}")
        # Add the logic to store data in database

    def scrape_data_only(self):
        resources = self._fetch_courses()
        return resources

    def _fetch_courses(self):
        html = self._fetch_html()
        if not html:
            return []
        return self._parse_html(html)

    def _fetch_html(self):
        if not self.soc_layout:
            print("Error: Could not determine current semester.")
            return None
        print(f"Current semester identified as: {self.semester_label}")

        url = f"https://enr-apps.as.cmu.edu/assets/SOC/{self.soc_layout}.htm"

        try:
            response = self.session.get(url, headers=self.headers)
            response.raise_for_status()
            # Fix malformed HTML: Insert <TR> before orphaned <TD> tags
            # Pattern: After a row ending with </TR>, if the next tag is <TD>, insert <TR>

            fixed_html = self._fix_malformed_html(response.text)

            print("Fixed malformed HTML by inserting missing <TR> tags")

            # Debug: save both versions
            # with open("debug_response_original.html", "w", encoding="utf-8") as f:
            #     f.write(response.text)
            # with open("debug_response_fixed.html", "w", encoding="utf-8") as f:
            #     f.write(fixed_html)
            # print("Saved original and fixed HTML files")
            return fixed_html
        except requests.exceptions.RequestException as e:
            print(f"Error fetching URL: {url}")
            print(f"Exception: {e}")
            return None

    def _fix_malformed_html(self, html):
        # Replace pattern: </TR>\n<TD with </TR>\n<TR><TD
        fixed_html = re.sub(
            r"(</TR>)\s*(<TD)", r"\1\n<TR>\2", html, flags=re.IGNORECASE
        )
        return fixed_html

    def _parse_html(self, html):
        soup = bs4.BeautifulSoup(html, "html.parser")
        print("Successfully fetched and parsed the schedule of classes page.")
        return self._parse_tables(soup)

    def _parse_tables(self, soup):

        all_tables = soup.find_all("table", {"border": "0"})

        # Now, filter these tables to find only the ones that are *course* tables
        tables = []
        for t in all_tables:
            header_rows = t.find_all("tr")
            # Check if table has at least 2 rows, and one of the first three rows contains <b>Course</b>
            if any(r.find("b", string="Course") for r in header_rows[:3]):
                tables.append(t)

        if not tables:
            print("Scraper found no course tables on the page.")

        resources = []
        for table_idx, table in enumerate(tables):
            rows = table.find_all("tr")

            last_course_num = None
            last_course_name = None

            # Process normal <tr> rows
            for row_idx, course in enumerate(rows):
                cols = course.find_all("td")
                last_course_num, last_course_name = self._process_row_columns(
                    cols, last_course_num, last_course_name, self.semester_label, resources
                )

        print(f"Scraper found {len(resources)} lecture and recitation sessions.")
        return resources

    def _process_row_columns(
        self, cols, last_course_num, last_course_name, semester_label, resources
    ):
        """Process a set of columns representing a course row"""
        if len(cols) < 7:
            return last_course_num, last_course_name
        raw_course_num = cols[0].text.strip()
        raw_course_name = cols[1].text.strip()
        lecture_section = cols[3].text.strip()

        # Track the base course name and last lecture section separately
        if not hasattr(self, "_last_base_course_name"):
            self._last_base_course_name = None
        if not hasattr(self, "_last_lecture_section"):
            self._last_lecture_section = None

        # Determine the course name to use for this row
        if raw_course_num:
            # New course row - this becomes the base
            last_course_num = raw_course_num
            last_course_name = raw_course_name
            self._last_base_course_name = raw_course_name
        elif raw_course_name:
            # Continuation row with subtitle - combine base + subtitle
            last_course_name = self._last_base_course_name + " " + raw_course_name
        # else: no course_num and no course_name means we keep last_course_name as-is

        # Update lecture section if present
        if lecture_section:
            self._last_lecture_section = lecture_section
        else:
            # Use the last known lecture section
            lecture_section = self._last_lecture_section

        if not last_course_num or not self.is_real_course_row(last_course_num):
            return last_course_num, last_course_name

        days = cols[4].text.strip()
        time_start = cols[5].text.strip()
        time_end = cols[6].text.strip()
        location = cols[7].text.strip() if len(cols) > 7 else "TBA"

        if days == "TBA" or not time_start:
            return last_course_num, last_course_name

        resource = ScheduleOfClasses(
            id=None,
            course_num=last_course_num,
            course_name=last_course_name,
            lecture_section=lecture_section,
            lecture_days=days,
            lecture_time_start=time_start,
            lecture_time_end=time_end,
            location=location,
            semester=self.semester_label,
            sem_start=self.sem_start,
            sem_end=self.sem_end,
        )
        resources.append(resource)

        return last_course_num, last_course_name
    def is_real_course_row(self, course_num: str) -> bool:
        # if is digit, then yes
        return course_num.isdigit()
