from collections import deque
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup


SKIP_EXTENSIONS = {
    ".pdf",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".webp",
    ".zip",
    ".rar",
    ".tar",
    ".gz",
    ".mp4",
    ".mov",
    ".avi",
    ".css",
    ".js",
}


def _normalize_url(url: str) -> str:
    parsed = urlparse(url)
    # Drop query/fragment to reduce duplicate crawling noise.
    normalized = parsed._replace(params="", query="", fragment="")
    return urlunparse(normalized)


def _is_internal_url(candidate_url: str, base_host: str) -> bool:
    parsed = urlparse(candidate_url)
    if parsed.scheme not in ("http", "https"):
        return False
    host = (parsed.hostname or "").lower()
    return host == base_host or host.endswith(f".{base_host}")


def _is_skippable_url(candidate_url: str) -> bool:
    path = urlparse(candidate_url).path.lower()
    return any(path.endswith(ext) for ext in SKIP_EXTENSIONS)


def extract_internal_links(html: str, base_url: str, base_host: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        absolute = urljoin(base_url, anchor["href"].strip())
        normalized = _normalize_url(absolute)

        if normalized in seen:
            continue
        if not _is_internal_url(normalized, base_host):
            continue
        if _is_skippable_url(normalized):
            continue

        seen.add(normalized)
        links.append(normalized)

    return links


def fetch_html(url: str) -> str | None:
    try:
        return requests.get(url, timeout=20).text
    except requests.RequestException as e:
        print(f"[fetch_html] {url} failed: {e}")
        return None


def crawl_site_pages(
    start_url: str,
    max_pages: int = 25,
    max_depth: int = 2,
) -> list[dict[str, str | int]]:
    """Crawl internal pages with BFS and strict bounds.

    Returns a list of dicts: {"url": str, "html": str, "depth": int}
    in crawl order.
    """
    normalized_start = _normalize_url(start_url)
    start_host = (urlparse(normalized_start).hostname or "").lower()
    if not start_host:
        return []

    visited: set[str] = set()
    queue: deque[tuple[str, int]] = deque([(normalized_start, 0)])
    pages: list[dict[str, str | int]] = []

    while queue and len(pages) < max_pages:
        current_url, depth = queue.popleft()
        if current_url in visited:
            continue
        if depth > max_depth:
            continue

        visited.add(current_url)
        html = fetch_html(current_url)
        if not html:
            continue

        pages.append({"url": current_url, "html": html, "depth": depth})

        if depth == max_depth:
            continue

        for link in extract_internal_links(html, current_url, start_host):
            if link not in visited:
                queue.append((link, depth + 1))

    return pages
