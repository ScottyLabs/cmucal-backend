# course_agent/app/env.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file BEFORE other imports
def detect_env() -> str:
    # Respect explicit setting (e.g., in Railway dashboard)
    app_env = os.getenv("APP_ENV")
    if app_env:
        return app_env.lower()
    # If we’re on Railway and APP_ENV isn’t set, assume production
    if os.getenv("RAILWAY_PROJECT_ID") or os.getenv("RAILWAY_ENVIRONMENT"):
        return "production"
    # Local default
    return "development"

def load_env():
    ENV = detect_env()
    print(f"[init] Detected APP_ENV={ENV}")
    dotfile = f".env.{ENV}"
    load_dotenv(dotfile)
    return ENV

def get_api_base_url() -> str:
    url = os.getenv("API_BASE_URL")
    if not url:
        raise RuntimeError("API_BASE_URL is not set")
    return url.rstrip("/")
