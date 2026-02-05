# course-agent/app/db/repositories.py
from .supabase_client import get_supabase
from typing import Optional, Dict

def fetch_all_courses():
    supabase = get_supabase()
    return (
        supabase
        .table("courses")
        .select("id, course_number, course_name, org_id")
        .execute()
        .data
    )

def get_or_build_category_for_course(course) -> int:
    org_id = course["org_id"]
    category_name = 'Main'
    supabase = get_supabase()

    # Check if category exists
    existing = (
        supabase
        .table("categories")
        .select("id")
        .eq("org_id", org_id)
        .eq("name", category_name)
        .execute()
        .data
    )

    if existing:
        return existing[0]["id"]

    # Create new category
    new_category = (
        supabase
        .table("categories")
        .insert({
            "org_id": org_id,
            "name": category_name,
        })
        .execute()
        .data
    )

    return new_category[0]["id"]

def insert_agent_run(agent_version: str) -> str:
    supabase = get_supabase()
    return supabase.table("agent_runs").insert({
        "agent_version": agent_version
    }).execute().data[0]["id"]

def get_course_website_by_url(course_id: int, url: str):
    supabase = get_supabase()
    res = (
        supabase
        .table("course_websites")
        .select("*")
        .eq("course_id", course_id)
        .eq("url", url)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    return res.data[0] if res.data else None


def upsert_course_website(course_id, agent_run_id, url, score, debug=None):
    supabase = get_supabase()
    return (
        supabase
        .table('course_websites')
        .upsert(
            {
                'course_id': course_id,
                'agent_run_id': agent_run_id,
                'url': url,
                'relevance_score': score,
                'source': 'search',
                'agent_debug': debug,
            },
            on_conflict='course_id,url',
        )
        .execute()
        .data[0]['id']
    )


def verify_course_website(website_id, notes, final_score):
    supabase = get_supabase()
    supabase.table("course_websites").update({
        "is_verified": True,
        "verification_notes": notes,
        "relevance_score": final_score
    }).eq("id", website_id).execute()

def upsert_calendar_source(
    *,
    org_id: int,
    category_id: int,
    url: str,
    notes: Optional[str] = None,
    default_event_type: Optional[str] = None,
):
    supabase = get_supabase()
    supabase.table('calendar_sources').upsert(
        {
            'org_id': org_id,
            'category_id': category_id,
            'url': url,
            'active': True,
            'notes': notes,
            'default_event_type': default_event_type,
        },
        on_conflict='category_id,url',
    ).execute()


