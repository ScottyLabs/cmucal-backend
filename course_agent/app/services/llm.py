# course_agent/app/services/llm.py
from langchain_openai import ChatOpenAI
from course_agent.app.env import load_env

load_env()

_llm = None

def get_llm():
    global _llm
    if _llm is None:
        _llm = ChatOpenAI(
            model='gpt-4o-mini',
            temperature=0,
        )
    return _llm
