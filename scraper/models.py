from datetime import datetime
from typing import Optional, List, Dict

class ResourceEvent:
    def __init__(
        self,
        start_datetime: datetime,
        end_datetime: datetime,
        location: str,
        recurrence: Optional[Dict] = None  # Defines recurrence pattern
    ):
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.location = location
        self.recurrence = recurrence

    def to_json(self):
        json = {
            "start_datetime": self.start_datetime,
            "end_datetime": self.end_datetime,
            "location": self.location,
            "recurrence": self.recurrence
        }
        return json

    def __str__(self):
        recurrence_str = f" | Recurs: {self.recurrence}" if self.recurrence else ""
        return f"{self.start_datetime.isoformat()} - {self.end_datetime.isoformat()} | {self.location}{recurrence_str}"

class CourseResource:
    def __init__(
        self,
        resource_type: str,  # "Office Hours", "Supplemental Instruction", "Peer Tutoring", "Drop In Tutoring"
        resource_source: str, # "122 OH Calendar", "CMU SI Page", etc.
        course_id: str,
        course_name: str,
        professor: str,
        instructor: str,
        events: List[ResourceEvent]
    ):
        self.resource_type = resource_type
        self.resource_source = resource_source
        self.course_id = course_id
        self.course_name = course_name
        self.professor = professor
        self.instructor = instructor
        self.events = events

    def to_json(self):
        json = {
            "resource_type": self.resource_type,
            "resource_source": self.resource_source,
            "course_id": self.course_id,
            "course_name": self.course_name,
            "professor": self.professor,
            "instructor": self.instructor,
            "events": [event.to_json() for event in self.events]
        }
        return json

    def __str__(self):
        events_str = '\n'.join([str(e) for e in self.events])
        return (
            f"{self.resource_type} - {self.resource_source}: {self.course_id} | {self.course_name}\n"
            f"P: {self.professor} | I: {self.instructor}\n{events_str}"
        )

class OtherResource:
    def __init__(
        self,
        resource_type: str,  # "Career", "Club"
        resource_source: str,  # "Handshake", "TartanConnect Website"
        event_name: str,
        event_host: str,
        events: List[ResourceEvent],
        categories: List[str],
        metadata: Optional[Dict] = None  # Additional metadata specific to the resource type
    ):
        self.resource_type = resource_type
        self.resource_source = resource_source
        self.event_name = event_name
        self.event_host = event_host
        self.events = events
        self.categories = categories
        self.metadata = metadata or {}

    def to_json(self):
        json = {
            "resource_type": self.resource_type,
            "resource_source": self.resource_source,
            "event_name": self.event_name,
            "event_host": self.event_host,
            "events": [event.to_json() for event in self.events],
            "categories": self.categories
        }
        # Add metadata if present
        if self.metadata:
            json.update(self.metadata)
        return json

    def __str__(self):
        events_str = '\n'.join([str(e) for e in self.events])
        categories_str = ', '.join(self.categories)
        metadata_str = f"\nMetadata: {self.metadata}" if self.metadata else ""
        return (
            f"{self.resource_type} - {self.resource_source}: {self.event_name} | {self.event_host}\n"
            f"Categories: {categories_str}\n{events_str}{metadata_str}"
        )

class ScheduleOfClasses:
    def __init__(
        self, 
        id: int, 
        course_num: str, 
        course_name: str, 
        lecture_section: str,
        lecture_days: str, 
        lecture_time_start: str, 
        lecture_time_end: str,
        location: str,
        semester: str,
        sem_start: datetime,
        sem_end: datetime
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
    
    def to_json(self):
        json = {
            "course_num": self.course_num,
            "course_name": self.course_name,
            "lecture_section": self.lecture_section,
            "lecture_days": self.lecture_days,
            "lecture_time_start": self.lecture_time_start,
            "lecture_time_end": self.lecture_time_end,
            "location": self.location,
            "semester": self.semester,
            "sem_start": self.sem_start,
            "sem_end": self.sem_end
        }
        return json
    
    def __str__(self):
        return f"{self.course_num} | {self.course_name} | Lecture Section: {self.lecture_section} | Lecture: {self.lecture_days} {self.lecture_time_start} {self.lecture_time_end} | Location: {self.location} | Semester: {self.semester} | Start: {self.sem_start} | End: {self.sem_end}"  
    



class SupplementalInstruction:
    """
    Example:
    {
        "course_num": "03121",
        "course_name": "Modern Biology",
        "professors": [
            "Wisniewski"
        ],
        "si_leaders": [
            "Hayden",
            "Louisa"
        ],
        "time_locations": [
            {
                "recurrence_frequency": "WEEKLY",
                "recurrence_interval": 1,
                "recurrence_by_day": "SU",
                "start_datetime": "2026-01-04T14:00:00",
                "end_datetime": "2026-01-04T15:00:00",
                "location": "POS 282"
            }
        ]
    }

    When processing, first time_locations should be converted 
    to RecurrenceRule and the rest will be overrides
    """
    def __init__(
        self,
        course_num: str,
        course_name: str,
        professors: list[str],
        si_leaders: list[str],
        time_locations: list[dict],  # Example: see above
    ):
        self.course_num = course_num
        self.course_name = course_name
        self.professors = professors
        self.si_leaders = si_leaders
        self.time_locations = time_locations