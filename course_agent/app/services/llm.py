# course_agent/app/services/llm.py
import os

from langchain_openai import ChatOpenAI
from course_agent.app.env import load_env

load_env()

_llm = None

def get_llm():
    global _llm
    if _llm is None:
        openrouter_key = os.getenv("OPENROUTER_API_KEY")
        openrouter_base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
        openrouter_model = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")

        if not openrouter_key:
            raise RuntimeError(
                "OPENROUTER_API_KEY is not set. OpenAI fallback is disabled; configure OpenRouter credentials to run the course agent."
            )

        try:
            _llm = ChatOpenAI(
                model=openrouter_model,
                api_key=openrouter_key,
                base_url=openrouter_base_url,
                temperature=0,
                default_headers={
                    "HTTP-Referer": os.getenv("OPENROUTER_HTTP_REFERER", "http://localhost"),
                    "X-Title": os.getenv("OPENROUTER_X_TITLE", "cmucal-course-agent"),
                },
            )
        except Exception as exc:
            raise RuntimeError(
                f"Failed to initialize OpenRouter client: {exc}"
            ) from exc
    return _llm
