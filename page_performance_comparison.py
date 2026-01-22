"""
Compare URL performance vs previous period (Google Search Console Search Analytics API)

Features
- Enter GSC property (siteUrl) in terminal (works with multiple properties)
- Enter current period start/end (YYYY-MM-DD)
- Automatically compares against the previous period of equal length
- Reads URLs from a list file (one URL per line)
- Exports CSV with:
  - clicks + impressions (current + previous)
  - absolute + % change for clicks and impressions
  - ctr + position (current + previous) (NO change calc for these)
- Includes a guard for GSC's ~16-month Search Analytics data window:
  exits with a warning if current/previous period starts too far back.

Auth files supported in either location:
- project root: client_secret.json / token.json

Install deps:
  pip install google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2
"""

from __future__ import annotations

import calendar
import csv
import os
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]


# -----------------------------
# Filesystem + auth helpers
# -----------------------------
def first_existing_path(paths: List[str]) -> Optional[str]:
    for p in paths:
        if p and os.path.exists(p):
            return p
    return None


def ensure_parent_dir(file_path: str) -> None:
    """
    Creates the parent directory of file_path if it exists and is non-empty.
    Avoids os.makedirs("") when saving token.json in project root.
    """
    parent = os.path.dirname(file_path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def get_service():
    client_secret = first_existing_path(
        ["client_secret.json", os.path.join(".secrets", "client_secret.json")]
    )
    if not client_secret:
        raise FileNotFoundError(
            "Could not find client_secret.json in project root or .secrets/. "
            "Download OAuth Desktop credentials and save as client_secret.json."
        )

    # Prefer an existing token if present; otherwise default to root token.json
    token_file = first_existing_path(
        ["token.json", os.path.join(".secrets", "token.json")]
    ) or "token.json"

    creds: Optional[Credentials] = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret, SCOPES)
            creds = flow.run_local_server(port=0)

        ensure_parent_dir(token_file)
        with open(token_file, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return build("searchconsole", "v1", credentials=creds)


def read_url_list(path: str) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"URL list file not found: {path}")

    urls: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            u = line.strip()
            if not u or u.startswith("#"):
                continue
            urls.append(u)

    # de-dup preserving order
    seen = set()
    out: List[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# -----------------------------
# Date window guard (~16 months)
# -----------------------------
def subtract_months(d: date, months: int) -> date:
    """
    Subtract N calendar months from a date, clamping day if needed.
    Example: 2026-03-31 minus 1 month -> 2026-02-28
    """
    y = d.year
    m = d.month - months
    while m <= 0:
        m += 12
        y -= 1
    last_day = calendar.monthrange(y, m)[1]
    return date(y, m, min(d.day, last_day))


def earliest_gsc_date(today: date) -> date:
    # Search Analytics data is available for ~16 months.
    return subtract_months(today, 16)


def enforce_16_month_window(cur_start: date, prev_start: date) -> None:
    earliest = earliest_gsc_date(date.today())
    if cur_start < earliest or prev_start < earliest:
        print("\n⚠️  Date range is outside Google Search Console’s ~16-month data window.")
        print(f"Earliest available date (approx): {earliest.isoformat()}")
        print(f"Your current period start:        {cur_start.isoformat()}")
        print(f"Your previous period start:       {prev_start.isoformat()}")
        print("\nFix: Choose a more recent start date so BOTH current and previous periods fit within ~16 months.")
        raise SystemExit(1)


# -----------------------------
# Date parsing + previous period
# -----------------------------
def parse_yyyy_mm_dd(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Invalid date: {s} (expected YYYY-MM-DD)")


def compute_previous_period(cur_start: date, cur_end: date) -> Tuple[date, date, int]:
    """
    Previous period = same length (inclusive), immediately before current period.
    """
    if cur_end < cur_start:
        raise ValueError("End date must be >= start date.")

    days_inclusive = (cur_end - cur_start).days + 1
    prev_end = cur_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days_inclusive - 1)
    return prev_start, prev_end, days_inclusive


# -----------------------------
# Change calculations
# -----------------------------
def pct_change(current: float, previous: float) -> Optional[float]:
    """
    Returns percent change (e.g., +12.34 means +12.34%).
    If previous == 0:
      - current == 0 -> 0.0
      - current > 0  -> None (undefined / infinite)
    """
    if previous == 0:
        return 0.0 if current == 0 else None
    return ((current - previous) / previous) * 100.0


# -----------------------------
# GSC query
# -----------------------------
def fetch_page_metrics(
    service,
    site_url: str,
    page_url: str,
    start_date: str,
    end_date: str,
) -> Dict[str, float]:
    """
    Returns aggregated metrics for ONE page over a date range:
    clicks, impressions, ctr, position.
    """
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": ["page"],
        "dimensionFilterGroups": [
            {
                "filters": [
                    {
                        "dimension": "page",
                        "operator": "equals",
                        "expression": page_url,
                    }
                ]
            }
        ],
        "rowLimit": 1,
    }

    resp = service.searchanalytics().query(siteUrl=site_url, body=body).execute()
    rows = resp.get("rows", []) or []
    if not rows:
        return {"clicks": 0.0, "impressions": 0.0, "ctr": 0.0, "position": 0.0}

    r = rows[0]
    return {
        "clicks": float(r.get("clicks", 0.0)),
        "impressions": float(r.get("impressions", 0.0)),
        "ctr": float(r.get("ctr", 0.0)),
        "position": float(r.get("position", 0.0)),
    }


# -----------------------------
# Main
# -----------------------------
def main():
    service = get_service()

    # List properties to help selection
    try:
        sites = service.sites().list().execute().get("siteEntry", []) or []
    except HttpError as e:
        print(f"Could not list properties (HttpError): {e}")
        sites = []

    if sites:
        print("\nAvailable properties (copy/paste one):")
        for s in sites:
            print(f" - {s.get('siteUrl')} ({s.get('permissionLevel')})")

    print("\nEnter siteUrl EXACTLY as in GSC (URL-prefix or sc-domain).")
    site_url = input("siteUrl: ").strip()
    if not site_url:
        raise SystemExit("siteUrl is required.")

    print("\nCurrent period:")
    cur_start_s = input("Start date (YYYY-MM-DD): ").strip()
    cur_end_s = input("End date   (YYYY-MM-DD): ").strip()

    cur_start = parse_yyyy_mm_dd(cur_start_s)
    cur_end = parse_yyyy_mm_dd(cur_end_s)

    prev_start, prev_end, days_inclusive = compute_previous_period(cur_start, cur_end)
    prev_start_s = prev_start.isoformat()
    prev_end_s = prev_end.isoformat()

    # Guard for ~16 months window (must include previous period too)
    enforce_16_month_window(cur_start, prev_start)

    print(f"\nPrevious period (auto): {prev_start_s} → {prev_end_s} ({days_inclusive} days)")

    urls_file = input("URL list file (default: input/urls.txt): ").strip() or "input/urls.txt"
    urls = read_url_list(urls_file)
    if not urls:
        raise SystemExit(f"No URLs found in {urls_file}")

    out_dir = os.path.join("results", date.today().isoformat())
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, "page_performance_comparison.csv")

    fieldnames = [
        "page_url",
        "site_url",
        "current_start",
        "current_end",
        "previous_start",
        "previous_end",
        "clicks_current",
        "clicks_previous",
        "clicks_change_abs",
        "clicks_change_pct",
        "impressions_current",
        "impressions_previous",
        "impressions_change_abs",
        "impressions_change_pct",
        "ctr_current",
        "ctr_previous",
        "position_current",
        "position_previous",
    ]

    print(f"\nComparing {len(urls)} URLs…")
    print(f"Saving results to: {out_csv}\n")

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, page_url in enumerate(urls, start=1):
            try:
                cur = fetch_page_metrics(service, site_url, page_url, cur_start_s, cur_end_s)
                prev = fetch_page_metrics(service, site_url, page_url, prev_start_s, prev_end_s)

                clicks_cur = cur["clicks"]
                clicks_prev = prev["clicks"]
                impr_cur = cur["impressions"]
                impr_prev = prev["impressions"]

                clicks_abs = clicks_cur - clicks_prev
                impr_abs = impr_cur - impr_prev

                clicks_pct = pct_change(clicks_cur, clicks_prev)
                impr_pct = pct_change(impr_cur, impr_prev)

                writer.writerow(
                    {
                        "page_url": page_url,
                        "site_url": site_url,
                        "current_start": cur_start_s,
                        "current_end": cur_end_s,
                        "previous_start": prev_start_s,
                        "previous_end": prev_end_s,
                        "clicks_current": int(round(clicks_cur)),
                        "clicks_previous": int(round(clicks_prev)),
                        "clicks_change_abs": int(round(clicks_abs)),
                        "clicks_change_pct": "" if clicks_pct is None else round(clicks_pct, 2),
                        "impressions_current": int(round(impr_cur)),
                        "impressions_previous": int(round(impr_prev)),
                        "impressions_change_abs": int(round(impr_abs)),
                        "impressions_change_pct": "" if impr_pct is None else round(impr_pct, 2),
                        "ctr_current": round(cur["ctr"], 6),
                        "ctr_previous": round(prev["ctr"], 6),
                        "position_current": round(cur["position"], 2),
                        "position_previous": round(prev["position"], 2),
                    }
                )

            except HttpError as e:
                print(f"HttpError for {page_url}: {e}")
                writer.writerow(
                    {
                        "page_url": page_url,
                        "site_url": site_url,
                        "current_start": cur_start_s,
                        "current_end": cur_end_s,
                        "previous_start": prev_start_s,
                        "previous_end": prev_end_s,
                        "clicks_current": "",
                        "clicks_previous": "",
                        "clicks_change_abs": "",
                        "clicks_change_pct": "",
                        "impressions_current": "",
                        "impressions_previous": "",
                        "impressions_change_abs": "",
                        "impressions_change_pct": "",
                        "ctr_current": "",
                        "ctr_previous": "",
                        "position_current": "",
                        "position_previous": "",
                    }
                )

            if i % 25 == 0:
                print(f"Processed {i}/{len(urls)}…")

    print("\nDone.")


if __name__ == "__main__":
    main()