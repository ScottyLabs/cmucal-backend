# app/helpers/tags.py

from app.models.tag import get_tag_by_name, save_tag
from app.models.event_tag import save_event_tag, get_tags_by_event, delete_event_tag


def get_or_create_tag(db, tag_name: str):
    """Get an existing tag or create it if it doesn't exist."""
    name = tag_name.strip().lower()
    tag = get_tag_by_name(db, name)
    return tag or save_tag(db, name=name)


def save_event_tags(db, event_id: int, tag_names: list[str]):
    """Attach a list of tags to an event, creating any that don't exist."""
    for tag_name in tag_names:
        tag = get_or_create_tag(db, tag_name)
        if not tag:
            raise ValueError(f"Failed to save tag '{tag_name}'")
        save_event_tag(db, event_id=event_id, tag_id=tag.id)


def sync_event_tags(db, event_id: int, desired_tag_names: list[str]):
    """
    Used on update — adds new tags and removes ones no longer wanted.
    """
    desired = [t.strip().lower() for t in desired_tag_names]
    current = [t.name.strip().lower() for t in get_tags_by_event(db, event_id)]

    for tag_name in desired:
        if tag_name not in current:
            tag = get_or_create_tag(db, tag_name)
            save_event_tag(db, event_id=event_id, tag_id=tag.id)

    for tag_name in current:
        if tag_name not in desired:
            tag = get_tag_by_name(db, tag_name)
            delete_event_tag(db, event_id=event_id, tag_id=tag.id)