import datetime as dt
import re

import bs4
import requests

from scraper.monitors.base_scraper import BaseScraper
from scraper.models import SupplementalInstruction




class SupplementalInstructionScraper(BaseScraper):
    def __init__(self, db):
        super().__init__(db, "Supplemental Instruction", "SASC")
        # https://www.cmu.edu/student-success/programs/supp-inst.html

    def scrape(self):
        supplemental_instruction_events = self.scrape_data_only()
        #TODO: Store in database
    

    def scrape_data_only(self) -> list[SupplementalInstruction]:
        si_url = "https://www.cmu.edu/student-success/programs/supp-inst.html"
        si_html = requests.get(si_url).content

        supplemental_instruction_events = self._process_html(si_html)

        return supplemental_instruction_events

    

    def _process_html(self, si_html) -> list[SupplementalInstruction]:
        soup = bs4.BeautifulSoup(si_html, 'html.parser')

        table = soup.find(id="si-table").find_all("tr")[1:] # ignore first row as it only contains column labels

        all_supp_instr_events = []

        for row in table:
            header = row.th.string.split(" ", 1) # split only on first space

            course_num = header[0].replace("-", "")
            course_name = header[1]

            cols = row.find_all("td")

            # assume names are separated by commas or ampersands "Ramit, Nivedita, & Andrew"
            professors = [name.strip() for name in re.split(r"[,&]+\s*[,&]*", cols[0].get_text())]
            si_leaders = [name.strip() for name in re.split(r"[,&]+\s*[,&]*", cols[1].get_text())]

            # using stripped_strings to get text nodes split by "<br>" tags
            time_location_strings = [text.strip() for text in cols[2].stripped_strings]
            time_locations = []

            for string in time_location_strings:
                time_location_data = self._generate_time_location(string)
                time_locations.append(time_location_data)
                
            # Create SupplementalInstruction object
            event = SupplementalInstruction(
                course_num=course_num,
                course_name=course_name,
                professors=professors,
                si_leaders=si_leaders,
                time_locations=time_locations
            )
            
            all_supp_instr_events.append(event)
            print(f"{course_num} {course_name}: {len(time_locations)} session(s)")

        return all_supp_instr_events

    def _generate_time_location(self, time_location_string) -> dict:
        """Generate a time location dictionary from a string.
        
            Example: "Thursdays, 6:00pm - 7:00pm - POS 282"
            Returns: dict with the following keys:
                - recurrence_frequency: "WEEKLY"
                - recurrence_interval: 1
                - recurrence_by_day: "TH"
                - start_datetime: datetime object 6pm (date is arbitrary)
                - end_datetime: datetime object 7pm (date is arbitrary)
                - location: "POS 282"


            TODO: Deal with semester bounds
        """
        
        # Split by last "-" to get location
        parts = time_location_string.rsplit("-", 1)
        location = parts[1].strip()
        
        # Split remaining by "," to get weekday and time range
        day_time_parts = parts[0].split(",", 1)
        weekday_name = day_time_parts[0].strip()
        time_range = day_time_parts[1].strip()
        
        
        time_parts = time_range.split("-") #e.g. "6:00pm - 7:00pm"
        start_time_str = time_parts[0].strip().replace(" ", "") # remove spaces between time and am/pm
        end_time_str = time_parts[1].strip().replace(" ", "")

        start_time = dt.datetime.strptime(start_time_str, "%I:%M%p").time()
        end_time = dt.datetime.strptime(end_time_str, "%I:%M%p").time()
        
        weekday_code = self._weekday_name_to_code(weekday_name)

        # Create datetime objects with arbitrary date (date component is irrelevant)
        # TODO: DEAL WITH TIMEZONES
        arbitrary_date = dt.date.today()
        start_datetime = dt.datetime.combine(arbitrary_date, start_time)
        end_datetime = dt.datetime.combine(arbitrary_date, end_time)
        
        time_location_data = {
            "recurrence_frequency": "WEEKLY",
            "recurrence_interval": 1,
            "recurrence_by_day": weekday_code,
            "start_datetime": start_datetime.isoformat(),
            "end_datetime": end_datetime.isoformat(),
            "location": location
        }

        return time_location_data

    def _weekday_name_to_code(self, weekday_name) -> str:
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
        # handle both singular and plural forms
        day = weekday_name.rstrip('s').strip()
        return mapping.get(day)