import requests

def fetch_html(url: str) -> str | None:
    try:
        return requests.get(url, timeout=20).text
    except requests.RequestException as e:
        print(f"[fetch_html] {url} failed: {e}")
        return None
