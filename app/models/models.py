from typing import List, Optional

from sqlalchemy import ARRAY, BigInteger, Boolean, Column, Date, DateTime, Double, Enum, ForeignKeyConstraint, SmallInteger, Identity, Numeric, PrimaryKeyConstraint, Table, Text, UniqueConstraint, text, Float
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
import datetime
from app.services.db import Base
from app.models.enums import FrequencyType, RecurrenceType

class CrosslistGroup(Base):
    __tablename__ = 'crosslist_groups'
    # __table_args__ = (
    #     PrimaryKeyConstraint('id', name='crosslist_groups_pkey'),
    # )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))

    course_crosslist: Mapped[List['CourseCrosslist']] = relationship('CourseCrosslist', back_populates='group')

class AgentRun(Base):
    __tablename__ = 'agent_runs'

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)

    agent_version: Mapped[str] = mapped_column(Text)

    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))

    course_websites: Mapped[list['CourseWebsite']] = relationship('CourseWebsite', back_populates='agent_run')

class Academic(Base):
    __tablename__ = 'academics'
    __table_args__ = (
        ForeignKeyConstraint(['event_id'], ['events.id'], ondelete='CASCADE', name='academics_event_id_fkey'),
        PrimaryKeyConstraint('event_id', name='academics_pkey')
    )

    event_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    course_num: Mapped[Optional[str]] = mapped_column(Text)
    course_name: Mapped[Optional[str]] = mapped_column(Text)
    instructors: Mapped[Optional[list]] = mapped_column(ARRAY(Text()))


class Career(Base):
    __tablename__ = 'careers'
    __table_args__ = (
        ForeignKeyConstraint(['event_id'], ['events.id'], ondelete='CASCADE', name='careers_event_id_fkey'),
        PrimaryKeyConstraint('event_id', name='careers_pkey')
    )

    event_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    host: Mapped[Optional[str]] = mapped_column(Text)
    link: Mapped[Optional[str]] = mapped_column(Text)
    registration_required: Mapped[Optional[bool]] = mapped_column(Boolean)


class Organization(Base):
    __tablename__ = 'organizations'
    __table_args__ = (
        # PrimaryKeyConstraint('id', name='organizations_pkey'),
        UniqueConstraint('name', name='organizations_name_key'),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    tags: Mapped[Optional[list]] = mapped_column(ARRAY(Text()))
    description: Mapped[Optional[str]] = mapped_column(Text)
    type: Mapped[Optional[str]] = mapped_column(Text)

    admins: Mapped[List['Admin']] = relationship('Admin', back_populates='org')
    courses: Mapped[List['Course']] = relationship('Course', back_populates='org')
    categories: Mapped[List['Category']] = relationship('Category', back_populates='org')
    events: Mapped[List['Event']] = relationship('Event', back_populates='org')
    event_occurrences: Mapped[List['EventOccurrence']] = relationship('EventOccurrence', back_populates='org')
    schedule_orgs: Mapped[List['ScheduleOrg']] = relationship('ScheduleOrg', back_populates='org')
    calendar_sources: Mapped[List['CalendarSource']] = relationship('CalendarSource', back_populates='org')

class Course(Base):
    __tablename__ = 'courses'
    __table_args__ = (
        UniqueConstraint("course_number", name="courses_course_number_key"),
        ForeignKeyConstraint(['org_id'], ['organizations.id'], ondelete='CASCADE', name='courses_org_id_fkey'),
        # PrimaryKeyConstraint('id', name='courses_pkey')
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    course_number: Mapped[str] = mapped_column(Text)
    course_name: Mapped[str] = mapped_column(Text)
    org_id: Mapped[int] = mapped_column(BigInteger)
    semesters: Mapped[Optional[list]] = mapped_column(ARRAY(Text()))
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))

    org: Mapped['Organization'] = relationship('Organization', back_populates='courses')
    course_crosslist: Mapped[List['CourseCrosslist']] = relationship('CourseCrosslist', back_populates='course')
    websites: Mapped[list['CourseWebsite']] = relationship('CourseWebsite', back_populates='course', cascade='all, delete-orphan')


class CourseWebsite(Base):
    __tablename__ = 'course_websites'
    __table_args__ = (
        UniqueConstraint(
            'course_id',
            'url',
            name='course_websites_course_url_key',
        ),
        ForeignKeyConstraint(
            ['course_id'],
            ['courses.id'],
            ondelete='CASCADE',
            name='course_websites_course_id_fkey',
        ),
        ForeignKeyConstraint(
            ['agent_run_id'],
            ['agent_runs.id'],
            ondelete='CASCADE',
            name='course_websites_agent_run_id_fkey',
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    course_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    agent_run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[Optional[str]] = mapped_column(Text)
    relevance_score: Mapped[Optional[float]] = mapped_column(Float)
    is_verified: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    verification_notes: Mapped[Optional[str]] = mapped_column(Text)
    agent_debug: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))

    course: Mapped['Course'] = relationship('Course',back_populates='websites')
    agent_run: Mapped['AgentRun'] = relationship('AgentRun',back_populates='course_websites')

class CourseCrosslist(Base):
    __tablename__ = 'course_crosslist'
    __table_args__ = (
        ForeignKeyConstraint(['course_id'], ['courses.id'], ondelete='CASCADE', name='course_crosslist_course_id_fkey'),
        ForeignKeyConstraint(['group_id'], ['crosslist_groups.id'], ondelete='CASCADE', name='course_crosslist_group_id_fkey'),
        # PrimaryKeyConstraint('id', name='course_crosslist_pkey')
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    course_id: Mapped[int] = mapped_column(BigInteger)
    group_id: Mapped[int] = mapped_column(BigInteger)

    course: Mapped['Course'] = relationship('Course', back_populates='course_crosslist')
    group: Mapped['CrosslistGroup'] = relationship('CrosslistGroup', back_populates='course_crosslist')


class Tag(Base):
    __tablename__ = 'tags'
    __table_args__ = (
        # PrimaryKeyConstraint('id', name='tags_pkey'),
        UniqueConstraint('name', name='tags_name_key'),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    name: Mapped[str] = mapped_column(Text)

    event_tags: Mapped[List['EventTag']] = relationship('EventTag', back_populates='tag')


class User(Base):
    __tablename__ = 'users'
    # __table_args__ = (
    #     PrimaryKeyConstraint('id', name='users_pkey'),
    # )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    email: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    clerk_id: Mapped[Optional[str]] = mapped_column(Text)
    fname: Mapped[Optional[str]] = mapped_column(Text)
    lname: Mapped[Optional[str]] = mapped_column(Text)
    calendar_id: Mapped[Optional[str]] = mapped_column(Text)

    admins: Mapped[List['Admin']] = relationship('Admin', back_populates='user')
    schedules: Mapped[List['Schedule']] = relationship('Schedule', back_populates='user')
    synced_events: Mapped[List['SyncedEvent']] = relationship('SyncedEvent', back_populates='user')
    user_saved_events: Mapped[List['UserSavedEvent']] = relationship('UserSavedEvent', back_populates='user')

class Admin(Base):
    __tablename__ = 'admins'
    __table_args__ = (
        ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE', name='admins_user_id_fkey'),
        ForeignKeyConstraint(['org_id'], ['organizations.id'], ondelete='CASCADE', name='admins_org_id_fkey'),
        PrimaryKeyConstraint('user_id', 'org_id', name='admins_pkey'),
    )

    # Scope:
    # - admin          → org_id NOT NULL
    # - manager        → org_id NOT NULL
    # - website_manager → org_id IS NULL

    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    # Nullable ONLY for website_manager
    org_id: Mapped[Optional[int]] = mapped_column(BigInteger, primary_key=True)
    role: Mapped[str] = mapped_column(Text, nullable=False) # 'admin', 'manager', 'website_manager'
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))

    user: Mapped['User'] = relationship('User', back_populates='admins')
    org: Mapped[Optional['Organization']] = relationship('Organization', back_populates='admins')

    admin_categories: Mapped[List['AdminCategory']] = relationship(
        'AdminCategory',
        back_populates='admin',
        cascade='all, delete-orphan'
    )

class AdminCategory(Base):
    __tablename__ = 'admin_categories'
    __table_args__ = (
        ForeignKeyConstraint(
            ['user_id', 'org_id'],
            ['admins.user_id', 'admins.org_id'],
            ondelete='CASCADE',
            name='admin_categories_admin_fkey'
        ),
        ForeignKeyConstraint(
            ['category_id'],
            ['categories.id'],
            ondelete='CASCADE',
            name='admin_categories_category_id_fkey'
        ),
        PrimaryKeyConstraint(
            'user_id',
            'org_id',
            'category_id',
            name='admin_categories_pkey'
        ),
    )

    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    org_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    category_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))

    admin: Mapped['Admin'] = relationship('Admin', back_populates='admin_categories')
    category: Mapped['Category'] = relationship('Category', back_populates='admin_categories')




class Category(Base):
    __tablename__ = 'categories'
    __table_args__ = (
        ForeignKeyConstraint(['org_id'], ['organizations.id'], ondelete='CASCADE', name='Category_org_id_fkey'),
        # PrimaryKeyConstraint('id', name='Category_pkey')
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(Text)
    org_id: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))

    org: Mapped['Organization'] = relationship('Organization', back_populates='categories')
    admin_categories: Mapped[List['AdminCategory']] = relationship('AdminCategory', back_populates='category')
    events: Mapped[List['Event']] = relationship('Event', back_populates='category')
    schedule_categories: Mapped[List['ScheduleCategory']] = relationship('ScheduleCategory', back_populates='category')
    event_occurrences: Mapped[List['EventOccurrence']] = relationship('EventOccurrence', back_populates='category')
    calendar_sources: Mapped[List['CalendarSource']] = relationship('CalendarSource', back_populates='category')

class CalendarSource(Base):
    __tablename__ = 'calendar_sources'
    __table_args__ = (
        UniqueConstraint('category_id', 'url', name='calendar_sources_category_url_key'),
        ForeignKeyConstraint(['category_id'], ['categories.id'], ondelete='CASCADE', name='calendar_sources_category_id_fkey'),
        ForeignKeyConstraint(['org_id'], ['organizations.id'], ondelete='CASCADE', name='calendar_sources_org_id_fkey'),
        # PrimaryKeyConstraint('id', name='calendar_sources_pkey')
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    # ownership
    category_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    org_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # feed identity
    url: Mapped[str] = mapped_column(Text, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean,server_default=text('true'),nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    # sync behavior
    fetch_interval_seconds: Mapped[int] = mapped_column(BigInteger, server_default=text("'21600'::bigint"))
    deletion_policy: Mapped[str] = mapped_column(Text, server_default=text("'mirror'::text"))
    all_day_handling: Mapped[str] = mapped_column(Text, server_default=text("'date_only'::text"))
    horizon_days: Mapped[int] = mapped_column(BigInteger, server_default=text("'180'::bigint"))
    sync_mode: Mapped[str] = mapped_column(Text, server_default=text("'delta'::text"))
    default_event_type: Mapped[Optional[str]] = mapped_column(Text)
    # sync metadata
    etag: Mapped[Optional[str]] = mapped_column(Text)
    last_modified_hdr: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True))
    content_hash: Mapped[Optional[str]] = mapped_column(Text)
    last_fetched_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True))
    last_sync_status: Mapped[Optional[str]] = mapped_column(Text)
    last_error: Mapped[Optional[str]] = mapped_column(Text)
    next_due_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True))
    # locking
    locked_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True))
    lock_owner: Mapped[Optional[str]] = mapped_column(Text)
    # audit
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    created_by_user_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    # relationships
    events: Mapped[List['Event']] = relationship("Event", back_populates="calendar_source", cascade="all, delete-orphan", passive_deletes=True)
    category: Mapped['Category'] = relationship('Category', back_populates='calendar_sources')
    org: Mapped['Organization'] = relationship('Organization', back_populates='calendar_sources')
    
    # default_tzid: Mapped[Optional[str]] = mapped_column(Text)

class Schedule(Base):
    __tablename__ = 'schedules'
    __table_args__ = (
        ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE', name='schedules_user_id_fkey'),
        # PrimaryKeyConstraint('id', name='schedules_pkey')
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    user_id: Mapped[int] = mapped_column(BigInteger)
    name: Mapped[Optional[str]] = mapped_column(Text)

    user: Mapped['User'] = relationship('User', back_populates='schedules')
    schedule_categories: Mapped[List['ScheduleCategory']] = relationship('ScheduleCategory', back_populates='schedule', cascade="all, delete-orphan")
    schedule_orgs: Mapped[List['ScheduleOrg']] = relationship('ScheduleOrg', back_populates='schedule', cascade="all, delete-orphan")
    user_saved_events: Mapped[List['UserSavedEvent']] = relationship('UserSavedEvent', back_populates='schedule', cascade="all, delete-orphan")


class SyncedEvent(Base):
    __tablename__ = 'synced_events'
    __table_args__ = (
        ForeignKeyConstraint(['user_id'], ['users.id'], onupdate='CASCADE', name='synced_events_user_id_fkey'),
        # PrimaryKeyConstraint('id', name='synced_events_pkey')
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    local_event_id: Mapped[str] = mapped_column(Text)
    google_event_id: Mapped[str] = mapped_column(Text)
    start: Mapped[str] = mapped_column(Text)
    synced_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    user_id: Mapped[int] = mapped_column(BigInteger)
    title: Mapped[Optional[str]] = mapped_column(Text)
    end: Mapped[Optional[str]] = mapped_column(Text)

    user: Mapped['User'] = relationship('User', back_populates='synced_events')


class Event(Base):
    __tablename__ = 'events'
    __table_args__ = (
        ForeignKeyConstraint(['calendar_source_id'], ['calendar_sources.id'], ondelete='SET NULL', name='events_calendar_source_id_fkey'),
        ForeignKeyConstraint(['category_id'], ['categories.id'], ondelete='CASCADE', name='events_category_id_fkey'),
        ForeignKeyConstraint(['org_id'], ['organizations.id'], ondelete='CASCADE', name='events_org_id_fkey'),
         # Dedupe SOC events only
        UniqueConstraint(
            "org_id",
            "title",
            "semester",
            "start_datetime",
            "end_datetime",
            "location",
            name="events_unique_soc",
        ),
        # Hard guarantee for ICS imports
        UniqueConstraint("calendar_source_id", "ical_uid", name="events_unique_calendar_uid"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    title: Mapped[str] = mapped_column(Text)
    start_datetime: Mapped[datetime.datetime] = mapped_column(DateTime(True))
    end_datetime: Mapped[datetime.datetime] = mapped_column(DateTime(True))
    is_all_day: Mapped[bool] = mapped_column(Boolean)
    event_timezone: Mapped[str] = mapped_column(Text, nullable=False)
    location: Mapped[str] = mapped_column(Text)
    # Can be UNKOWN but not null
    semester: Mapped[str] = mapped_column(Text)
    user_edited: Mapped[Optional[list]] = mapped_column(ARRAY(BigInteger()))
    org_id: Mapped[int] = mapped_column(BigInteger)
    category_id: Mapped[int] = mapped_column(BigInteger)
    calendar_source_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, index=True)

    description: Mapped[Optional[str]] = mapped_column(Text)
    source_url: Mapped[Optional[str]] = mapped_column(Text)
    event_type: Mapped[Optional[str]] = mapped_column(Text)
    last_updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    ical_uid: Mapped[Optional[str]] = mapped_column(Text)
    ical_sequence: Mapped[Optional[int]] = mapped_column(BigInteger)
    ical_last_modified: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True))
    occurrences_valid_through: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True))
    last_occurrence_build_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True))

    calendar_source: Mapped[Optional['CalendarSource']] = relationship('CalendarSource', back_populates='events')
    category: Mapped['Category'] = relationship('Category', back_populates='events')
    org: Mapped['Organization'] = relationship('Organization', back_populates='events')
    event_occurrences: Mapped[List['EventOccurrence']] = relationship('EventOccurrence', back_populates='event', passive_deletes=True)
    event_tags: Mapped[List['EventTag']] = relationship('EventTag', back_populates='event', passive_deletes=True)
    recurrence_rules: Mapped[List['RecurrenceRule']] = relationship('RecurrenceRule', back_populates='event', passive_deletes=True)
    user_saved_events: Mapped[List['UserSavedEvent']] = relationship('UserSavedEvent', back_populates='event', passive_deletes=True)

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class ScheduleOrg(Base):
    __tablename__ = 'schedule_orgs'
    __table_args__ = (
        ForeignKeyConstraint(['org_id'], ['organizations.id'], ondelete='CASCADE', name='schedule_orgs_org_id_fkey'),
        ForeignKeyConstraint(['schedule_id'], ['schedules.id'], ondelete='CASCADE', name='schedule_orgs_schedule_id_fkey'),
        PrimaryKeyConstraint('schedule_id', 'org_id', name='schedule_orgs_pkey')
    )

    schedule_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    org_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))

    org: Mapped['Organization'] = relationship('Organization', back_populates='schedule_orgs')
    schedule: Mapped['Schedule'] = relationship('Schedule', back_populates='schedule_orgs')


class ScheduleCategory(Base):
    __tablename__ = 'schedule_categories'
    __table_args__ = (
        ForeignKeyConstraint(['category_id'], ['categories.id'], ondelete='CASCADE', name='schedule_categories_category_id_fkey'),
        ForeignKeyConstraint(['schedule_id'], ['schedules.id'], ondelete='CASCADE', name='schedule_categories_schedule_id_fkey'),
        PrimaryKeyConstraint('schedule_id', 'category_id', name='schedule_categories_pkey')
    )

    schedule_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    category_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    category: Mapped['Category'] = relationship('Category', back_populates='schedule_categories')
    schedule: Mapped['Schedule'] = relationship('Schedule', back_populates='schedule_categories')


class Club(Base):
    __tablename__ = 'clubs'
    __table_args__ = (
        ForeignKeyConstraint(['event_id'], ['events.id'], ondelete='CASCADE', name='clubs_event_id_fkey'),
        PrimaryKeyConstraint('event_id', name='clubs_pkey')
    )

    event_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)


class EventOccurrence(Base):
    __tablename__ = 'event_occurrences'
    __table_args__ = (
        ForeignKeyConstraint(['category_id'], ['categories.id'], ondelete='CASCADE', name='event_occurrences_category_id_fkey'),
        ForeignKeyConstraint(['event_id'], ['events.id'], ondelete='CASCADE', name='event_occurrences_event_id_fkey'),
        ForeignKeyConstraint(['org_id'], ['organizations.id'], ondelete='CASCADE', name='event_occurrences_org_id_fkey'),
        # PrimaryKeyConstraint('id', name='event_occurrences_pkey')
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    start_datetime: Mapped[datetime.datetime] = mapped_column(DateTime(True))
    end_datetime: Mapped[datetime.datetime] = mapped_column(DateTime(True))
    location: Mapped[str] = mapped_column(Text)
    is_all_day: Mapped[bool] = mapped_column(Boolean)
    user_edited: Mapped[Optional[list]] = mapped_column(ARRAY(BigInteger()))
    event_id: Mapped[int] = mapped_column(BigInteger)
    event_saved_at: Mapped[datetime.datetime] = mapped_column(DateTime(True))
    title: Mapped[str] = mapped_column(Text)
    org_id: Mapped[int] = mapped_column(BigInteger)
    category_id: Mapped[int] = mapped_column(BigInteger)
    recurrence: Mapped[str] = mapped_column(Enum(RecurrenceType, name='recurrence_type', create_type=False))
    description: Mapped[Optional[str]] = mapped_column(Text)
    source_url: Mapped[Optional[str]] = mapped_column(Text)

    category: Mapped['Category'] = relationship('Category', back_populates='event_occurrences')
    event: Mapped['Event'] = relationship('Event', back_populates='event_occurrences')
    org: Mapped['Organization'] = relationship('Organization', back_populates='event_occurrences')


class EventTag(Base):
    __tablename__ = 'event_tags'
    __table_args__ = (
        ForeignKeyConstraint(['event_id'], ['events.id'], ondelete='CASCADE', name='event_tags_event_id_fkey'),
        ForeignKeyConstraint(['tag_id'], ['tags.id'], ondelete='CASCADE', name='event_tags_tag_id_fkey'),
        PrimaryKeyConstraint('event_id', 'tag_id', name='event_tags_pkey')
    )

    event_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    tag_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))

    event: Mapped['Event'] = relationship('Event', back_populates='event_tags')
    tag: Mapped['Tag'] = relationship('Tag', back_populates='event_tags')


class RecurrenceRule(Base):
    __tablename__ = 'recurrence_rules'
    __table_args__ = (
        ForeignKeyConstraint(['event_id'], ['events.id'], ondelete='CASCADE', name='recurrence_rules_event_id_fkey'),
        # PrimaryKeyConstraint('id', name='recurrence_rules_pkey')
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    event_id: Mapped[int] = mapped_column(BigInteger)
    frequency: Mapped[str] = mapped_column(Enum(FrequencyType, name='frequency_type', create_type=False))
    interval: Mapped[int] = mapped_column(BigInteger)
    start_datetime: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True))
    count: Mapped[Optional[int]] = mapped_column(BigInteger)
    until: Mapped[Optional[datetime.date]] = mapped_column(DateTime(timezone=True))
    by_month: Mapped[Optional[list]] = mapped_column(SmallInteger)
    by_month_day: Mapped[Optional[list]] = mapped_column(SmallInteger)
    by_day: Mapped[Optional[list]] = mapped_column(ARRAY(Text()))
    orig_until: Mapped[Optional[datetime.date]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_generated_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    event: Mapped['Event'] = relationship('Event', back_populates='recurrence_rules')
    event_overrides: Mapped[List['EventOverride']] = relationship('EventOverride', back_populates='rrule')
    recurrence_exdates: Mapped[List['RecurrenceExdate']] = relationship('RecurrenceExdate', back_populates='rrule')
    recurrence_rdates: Mapped[List['RecurrenceRdate']] = relationship('RecurrenceRdate', back_populates='rrule')
    recurrence_overrides: Mapped[List['RecurrenceOverride']] = relationship('RecurrenceOverride', back_populates='rrule')

class RecurrenceExdate(Base):
    __tablename__ = 'recurrence_exdates'
    __table_args__ = (
        ForeignKeyConstraint(['rrule_id'], ['recurrence_rules.id'], ondelete='CASCADE', name='recurrence_exdates_rrule_id_fkey'),
        # PrimaryKeyConstraint('id', name='recurrence_exdates_pkey')
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    rrule_id: Mapped[int] = mapped_column(BigInteger)
    exdate: Mapped[datetime.datetime] = mapped_column(DateTime(True))

    rrule: Mapped['RecurrenceRule'] = relationship('RecurrenceRule', back_populates='recurrence_exdates')

class RecurrenceRdate(Base):
    __tablename__ = 'recurrence_rdates'
    __table_args__ = (
        ForeignKeyConstraint(['rrule_id'], ['recurrence_rules.id'], ondelete='CASCADE', name='recurrence_rdates_rrule_id_fkey'),
        # PrimaryKeyConstraint('id', name='recurrence_rdates_pkey')
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    rrule_id: Mapped[int] = mapped_column(BigInteger)
    rdate: Mapped[datetime.datetime] = mapped_column(DateTime(True))

    rrule: Mapped['RecurrenceRule'] = relationship('RecurrenceRule', back_populates='recurrence_rdates')

class EventOverride(Base):
    __tablename__ = 'event_overrides'
    __table_args__ = (
        ForeignKeyConstraint(['rrule_id'], ['recurrence_rules.id'], ondelete='CASCADE', name='event_overrides_rrule_id_fkey'),
        # PrimaryKeyConstraint('id', name='event_overrides_pkey')
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    rrule_id: Mapped[int] = mapped_column(BigInteger)
    recurrence_date: Mapped[datetime.datetime] = mapped_column(DateTime(True))
    new_start: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True))
    new_end: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True))
    new_title: Mapped[Optional[str]] = mapped_column(Text)
    new_description: Mapped[Optional[str]] = mapped_column(Text)
    new_location: Mapped[Optional[str]] = mapped_column(Text)

    rrule: Mapped['RecurrenceRule'] = relationship('RecurrenceRule', back_populates='event_overrides')


class RecurrenceOverride(Base):
    """
    Pattern-based override for recurring events. Works alongside EventOverride.
    EventOverride targets specific dates, RecurrenceOverride targets patterns (e.g., "all Tuesdays").
    
    Priority: EventOverride (date-specific) > RecurrenceOverride (pattern-based) > Ex/RDate > Default event values
    
    Examples:
    - Every Tuesday, meeting is in Room B instead of Room A: by_day=['TU'], new_location='Room B'
    - All meetings in June start at 2pm: by_month=6, new_start with 2pm time
    """
    __tablename__ = 'recurrence_overrides'
    __table_args__ = (
        ForeignKeyConstraint(['rrule_id'], ['recurrence_rules.id'], ondelete='CASCADE', name='recurrence_overrides_rrule_id_fkey'),
        # PrimaryKeyConstraint('id', name='recurrence_overrides_pkey')
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    rrule_id: Mapped[int] = mapped_column(BigInteger)
    
    # Pattern matching fields - define which occurrences this override applies to
    frequency: Mapped[str] = mapped_column(Enum(FrequencyType, name='frequency_type', create_type=False))
    interval: Mapped[int] = mapped_column(BigInteger)

    by_day: Mapped[Optional[list]] = mapped_column(ARRAY(Text()))  # e.g., ['TU', 'TH'] for Tuesdays and Thursdays
    by_month: Mapped[Optional[int]] = mapped_column(SmallInteger)  # 1-12, e.g., 6 for June
    by_month_day: Mapped[Optional[int]] = mapped_column(SmallInteger)  # 1-31, specific day of month
    
    # Override values - same structure as EventOverride
    new_start: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True))
    new_end: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True))
    new_title: Mapped[Optional[str]] = mapped_column(Text)
    new_description: Mapped[Optional[str]] = mapped_column(Text)
    new_location: Mapped[Optional[str]] = mapped_column(Text)
    
    # TODO: Update priority system.
    # Priority for conflict resolution when multiple patterns match (higher number = higher priority)
    # priority: Mapped[int] = mapped_column(SmallInteger, server_default=text('0'))
    
    rrule: Mapped['RecurrenceRule'] = relationship('RecurrenceRule', back_populates='recurrence_overrides')


class UserSavedEvent(Base):
    __tablename__ = 'user_saved_events'
    __table_args__ = (
        ForeignKeyConstraint(['event_id'], ['events.id'], ondelete='CASCADE', name='user_saved_events_event_id_fkey'),
        ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE', name='user_saved_events_user_id_fkey'),
        ForeignKeyConstraint(['schedule_id'], ['schedules.id'], ondelete='CASCADE', name='user_saved_events_schedule_id_fkey'),
        PrimaryKeyConstraint('user_id', 'event_id', name='user_saved_events_pkey'),
        UniqueConstraint('google_event_id', name='user_saved_events_google_event_id_key')
    )

    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    event_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    google_event_id: Mapped[str] = mapped_column(Text)
    saved_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    schedule_id: Mapped[Optional[int]] = mapped_column(BigInteger)

    event: Mapped['Event'] = relationship('Event', back_populates='user_saved_events')
    schedule: Mapped[Optional['Schedule']] = relationship('Schedule', back_populates='user_saved_events')
    user: Mapped['User'] = relationship('User', back_populates='user_saved_events')
