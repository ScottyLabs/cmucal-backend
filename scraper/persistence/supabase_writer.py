# scraper/persistence/supabase_writer.py
from supabase import create_client, Client
import os

_supabase: Client | None = None


def get_supabase() -> Client:
    global _supabase
    if _supabase is None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        print(f"[Supabase] Connecting to {url}")
        print(f"[Supabase] Using 'SERVICE ROLE' key: {'SET' if key else 'MISSING'}")

        if not url or not key:
            raise RuntimeError(
                "Supabase env vars not set. "
                "Did you forget to call load_env() at process startup?"
            )

        _supabase = create_client(url, key)

    return _supabase

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]
