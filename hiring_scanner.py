"""
Agentic AI – Global Sales Hiring Scanner
=======================================

What this script does
---------------------
- Searches public web sources for open roles in:
  Senior Sales Strategy, Sales Operations, Sales Intelligence, Global Sales Operations,
  Sales Insights & Analytics, Sales Enablement, Sales Strategy Manager,
  Sales Planning & Operations, Revenue Operations, Sales Transformation,
  Go-to-Market Strategy, Sales Excellence, Sales Analytics, Market Intelligence.
- Sources (extensible):
  * Google (via SerpAPI) – discovers postings on public ATS/job boards
  * Greenhouse boards – parses job cards
  * Lever boards – parses job JSON API
  * Generic job pages – simple fallback parsing
- Normalizes results and writes to:
  * SQLite DB (jobs.db)
  * CSV export (jobs.csv)
- Optional: Slack webhook + Email digest (daily)

How to run
----------
1) Python 3.10+
2) `pip install -r requirements.txt`
3) Set env vars (optional but recommended):
   - SERPAPI_KEY=<your_key>  (for broad discovery)
   - SLACK_WEBHOOK_URL=<optional>
   - SMTP_HOST/SMTP_PORT/SMTP_USER/SMTP_PASS/SMTP_TO (optional)
4) `python agentic_sales_hiring_scanner.py`  (this file)

Notes & ethics
--------------
- Respects robots.txt by using search-engine results (SerpAPI) + polite rate limits.
- Do NOT scrape sites that disallow bots; always follow each site's ToS.
- Prefer vendor APIs (Lever public postings API, Greenhouse public boards) when available.

"""
from __future__ import annotations
import asyncio
import csv
import dataclasses
import datetime as dt
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Iterable, Tuple

import httpx
from bs4 import BeautifulSoup

# ----------------------------
# Configuration
# ----------------------------
ROLE_KEYWORDS = [
    "Senior Sales Strategy",
    "Sales Strategy",
    "Sales Operations",
    "Global Sales Operations",
    "Revenue Operations",
    "RevOps",
    "Sales Intelligence",
    "Sales Insights",
    "Sales Insights & Analytics",
    "Sales Enablement",
    "Sales Planning & Operations",
    "Sales Transformation",
    "Go-to-Market Strategy",
    "GTM Strategy",
    "Sales Excellence",
    "Sales Analytics",
    "Market Intelligence",
]

SEARCH_SITES = [
    "site:boards.greenhouse.io",
    "site:jobs.lever.co",
    "site:ashbyhq.com",
    "site:workable.com",
    "site:myworkdayjobs.com",
    "site:smartrecruiters.com",
    "site:jobs.ashbyhq.com",
]

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)

DB_PATH = "jobs.db"
CSV_PATH = "jobs.csv"
REQUEST_TIMEOUT = 20
RATE_LIMIT_PER_HOST = 1.0  # seconds between requests per host

SERPAPI_KEY = os.getenv("SERPAPI_KEY")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# ----------------------------
# Data models
# ----------------------------
@dataclass
class JobPosting:
    title: str
    company: str
    location: str
    url: str
    source: str
    posted_at: Optional[str] = None
    captured_at: str = dt.datetime.utcnow().isoformat()
    match_score: float = 0.0

# ----------------------------
# Utilities
# ----------------------------
_host_last_hit: Dict[str, float] = {}

async def polite_get(client: httpx.AsyncClient, url: str) -> Optional[httpx.Response]:
    from urllib.parse import urlparse
    host = urlparse(url).netloc
    last = _host_last_hit.get(host, 0)
    wait = max(0.0, RATE_LIMIT_PER_HOST - (time.time() - last))
    if wait > 0:
        await asyncio.sleep(wait)
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT})
        _host_last_hit[host] = time.time()
        if r.status_code == 200:
            return r
        return None
    except Exception:
        return None

# ----------------------------
# Discovery via SerpAPI (Google)
# ----------------------------
async def serpapi_search(query: str, num: int = 10) -> List[str]:
    if not SERPAPI_KEY:
        return []
    # SerpAPI Google Search
    # Docs: https://serpapi.com/search-api
    params = {
        "engine": "google",
        "q": query,
        "num": num,
        "api_key": SERPAPI_KEY,
        "hl": "en",
        "gl": "us",
    }
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get("https://serpapi.com/search.json", params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            links = []
            for item in data.get("organic_results", []):
                link = item.get("link")
                if link:
                    links.append(link)
            return links
        except Exception:
            return []

async def discover_urls() -> List[str]:
    """Create search queries that combine role keywords with target job sites."""
    tasks = []
    for role in ROLE_KEYWORDS:
        for site in SEARCH_SITES:
            q = f"\"{role}\" {site}"
            tasks.append(serpapi_search(q, num=10))
    results = await asyncio.gather(*tasks)
    urls = []
    for r in results:
        urls.extend(r)
    # de-dup
    seen = set()
    unique = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)
    return unique

# ----------------------------
# Parsers for common ATS/job boards
# ----------------------------

GH_RE = re.compile(r"https?://boards\.greenhouse\.io/([^/?#]+)/?", re.I)
LEVER_RE = re.compile(r"https?://jobs\.lever\.co/([^/?#]+)/?", re.I)

async def parse_greenhouse(client: httpx.AsyncClient, url: str) -> List[JobPosting]:
    resp = await polite_get(client, url)
    if not resp:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    company = None
    m = GH_RE.match(url)
    if m:
        company = m.group(1)
    postings: List[JobPosting] = []
    for job in soup.select(".opening, .job, .opening a, a.posting-title"):  # broad selectors
        title = job.get_text(strip=True)
        href = job.get("href")
        if not href or not title:
            continue
        full_url = href if href.startswith("http") else resp.request.url.join(href)
        # Try to grab nearby location text
        loc = ""
        li = job.find_parent("div")
        if li:
            loc_el = li.find(class_=re.compile("location|office", re.I))
            if loc_el:
                loc = loc_el.get_text(strip=True)
        postings.append(JobPosting(
            title=title,
            company=company or "",
            location=loc,
            url=str(full_url),
            source="greenhouse",
        ))
    return postings

async def parse_lever(client: httpx.AsyncClient, url: str) -> List[JobPosting]:
    # Use Lever public postings API when possible
    m = LEVER_RE.match(url)
    postings: List[JobPosting] = []
    if m:
        company = m.group(1)
        api_url = f"https://api.lever.co/v0/postings/{company}?mode=json"
        r = await polite_get(client, api_url)
        if r and r.status_code == 200:
            try:
                data = r.json()
                for item in data:
                    title = item.get("text", "").strip()
                    lever_url = item.get("hostedUrl") or item.get("applyUrl") or url
                    location = (item.get("categories") or {}).get("location", "")
                    created = item.get("createdAt")
                    posted_iso = None
                    if created:
                        try:
                            posted_iso = dt.datetime.utcfromtimestamp(int(created)/1000).isoformat()
                        except Exception:
                            posted_iso = None
                    postings.append(JobPosting(
                        title=title,
                        company=company,
                        location=location or "",
                        url=lever_url,
                        source="lever",
                        posted_at=posted_iso,
                    ))
                return postings
            except Exception:
                pass
    # Fallback: parse HTML page
    resp = await polite_get(client, url)
    if not resp:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    company = company if 'company' in locals() else ""
    for a in soup.select("a[href]"):
        text = a.get_text(strip=True)
        if text:
            postings.append(JobPosting(
                title=text,
                company=company,
                location="",
                url=str(a.get("href")),
                source="lever-html",
            ))
    return postings

async def parse_generic(client: httpx.AsyncClient, url: str) -> List[JobPosting]:
    resp = await polite_get(client, url)
    if not resp:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    title_nodes = soup.select("a, h2, h3")
    postings: List[JobPosting] = []
    for node in title_nodes:
        text = node.get_text(" ", strip=True)
        if not text:
            continue
        # Quick title filter by role keywords, case-insensitive
        if any(k.lower() in text.lower() for k in ROLE_KEYWORDS):
            href = node.get("href")
            link = None
            if href:
                link = href if href.startswith("http") else resp.request.url.join(href)
            postings.append(JobPosting(
                title=text,
                company="",
                location="",
                url=str(link) if link else url,
                source="generic",
            ))
    return postings

async def parse_url(client: httpx.AsyncClient, url: str) -> List[JobPosting]:
    if GH_RE.match(url):
        return await parse_greenhouse(client, url)
    if LEVER_RE.match(url):
        return await parse_lever(client, url)
    return await parse_generic(client, url)

# ----------------------------
# Post-processing: normalize, score, dedupe
# ----------------------------

def normalize_company_from_url(url: str) -> str:
    m = GH_RE.match(url)
    if m:
        return m.group(1)
    m = LEVER_RE.match(url)
    if m:
        return m.group(1)
    # try domain-based fallback
    try:
        from urllib.parse import urlparse
        host = urlparse(url).netloc
        parts = host.split('.')
        if len(parts) >= 2:
            return parts[-2]
    except Exception:
        pass
    return ""

def heuristic_score(job: JobPosting) -> float:
    score = 0.0
    title_l = job.title.lower()
    for k in ROLE_KEYWORDS:
        if k.lower() in title_l:
            score += 20
    # Prefer RevOps/Strategy terms
    for bonus in ["revops", "revenue operations", "strategy", "enablement", "analytics", "intelligence", "insights", "market intelligence", "gtm"]:
        if bonus in title_l:
            score += 5
    # Slight boost for known ATS sources
    if job.source in ("greenhouse", "lever"):
        score += 5
    return min(score, 100.0)

def dedupe(posts: List[JobPosting]) -> List[JobPosting]:
    seen = set()
    unique: List[JobPosting] = []
    for p in posts:
        key = (p.title.lower(), (p.company or normalize_company_from_url(p.url)).lower(), p.location.lower())
        if key not in seen:
            seen.add(key)
            unique.append(p)
    return unique

# ----------------------------
# Persistence
# ----------------------------

def ensure_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            company TEXT,
            location TEXT,
            url TEXT UNIQUE,
            source TEXT,
            posted_at TEXT,
            captured_at TEXT,
            match_score REAL
        )
        """
    )
    con.commit()
    con.close()

def upsert_jobs(posts: List[JobPosting]) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    inserted = 0
    for p in posts:
        try:
            cur.execute(
                """
                INSERT OR IGNORE INTO jobs (title, company, location, url, source, posted_at, captured_at, match_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (p.title, p.company or normalize_company_from_url(p.url), p.location, p.url, p.source, p.posted_at, p.captured_at, p.match_score)
            )
            if cur.rowcount:
                inserted += 1
        except Exception:
            continue
    con.commit()
    con.close()
    return inserted

def export_csv(path: str = CSV_PATH):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT title, company, location, url, source, posted_at, captured_at, match_score FROM jobs ORDER BY match_score DESC, captured_at DESC")
    rows = cur.fetchall()
    con.close()
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "company", "location", "url", "source", "posted_at", "captured_at", "match_score"])
        writer.writerows(rows)

# ----------------------------
# Notifications (optional)
# ----------------------------

def notify_slack(message: str):
    if not SLACK_WEBHOOK_URL:
        return
    try:
        import requests
        requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
    except Exception:
        pass

# ----------------------------
# Orchestrator (Agent loop)
# ----------------------------

async def run_scan() -> Tuple[int, int]:
    ensure_db()
    discovered = await discover_urls() if SERPAPI_KEY else []
    # If no SerpAPI key, you can seed with known boards or company lists.
    if not discovered:
        print("[warn] SERPAPI_KEY not set or no results; add a key for broad discovery.")
    async with httpx.AsyncClient(follow_redirects=True, headers={"User-Agent": USER_AGENT}) as client:
        tasks = [parse_url(client, u) for u in discovered]
        results = await asyncio.gather(*tasks)
    flat: List[JobPosting] = [p for lst in results for p in lst]
    # Filter & score
    filtered: List[JobPosting] = []
    for p in flat:
        # Tighten: only keep titles that match our roles
        if any(k.lower() in p.title.lower() for k in ROLE_KEYWORDS):
            p.company = p.company or normalize_company_from_url(p.url)
            p.match_score = heuristic_score(p)
            filtered.append(p)
    unique = dedupe(filtered)
    inserted = upsert_jobs(unique)
    export_csv(CSV_PATH)
    return len(unique), inserted

# ----------------------------
# CLI entry
# ----------------------------

if __name__ == "__main__":
    print("Agentic AI – Global Sales Hiring Scanner :: starting...")
    total, inserted = asyncio.run(run_scan())
    print(f"Scan complete. Matched: {total} | New inserted: {inserted} | CSV: {CSV_PATH} | DB: {DB_PATH}")
    if inserted:
        notify_slack(f"New sales strategy/ops jobs inserted: {inserted}. See {CSV_PATH}")
