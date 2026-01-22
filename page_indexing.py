from __future__ import annotations

import csv
import os
import sys
import time
from datetime import date
from typing import Dict, List, Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
CLIENT_SECRET_FILE = "client_secret.json"
TOKEN_FILE = "token.json"


def mkdirp(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def safe_get(d: Dict, path: List[str], default=None):
    cur = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def read_urls(path: str) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"URL list file not found: {path}")

    urls: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            u = line.strip()
            if not u or u.startswith("#"):
                continue
            urls.append(u)

    # de-dup while preserving order
    seen = set()
    deduped = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            deduped.append(u)

    return deduped


def get_service() -> object:
    if not os.path.exists(CLIENT_SECRET_FILE):
        raise FileNotFoundError(
            f"Missing {CLIENT_SECRET_FILE}. Download OAuth Desktop credentials JSON and rename it to {CLIENT_SECRET_FILE}."
        )

    creds: Optional[Credentials] = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return build("searchconsole", "v1", credentials=creds)


def list_properties(service) -> List[Dict]:
    resp = service.sites().list().execute()
    return resp.get("siteEntry", []) or []


def inspect_url(service, site_url: str, inspection_url: str, language_code: str = "en-US") -> Dict:
    body = {
        "inspectionUrl": inspection_url,
        "siteUrl": site_url,
        "languageCode": language_code,
    }
    # URL Inspection API endpoint
    return service.urlInspection().index().inspect(body=body).execute()


def main():
    try:
        service = get_service()
    except Exception as e:
        print(f"Setup error: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nFetching your Search Console properties...")
    try:
        sites = list_properties(service)
    except HttpError as e:
        print(f"Could not list properties (HttpError): {e}", file=sys.stderr)
        sites = []

    if sites:
        print("\nAvailable properties (copy/paste one into the prompt):")
        for s in sites:
            print(f" - {s.get('siteUrl')} ({s.get('permissionLevel')})")
    else:
        print("No properties returned (or insufficient access). You can still paste siteUrl manually.\n")

    print("\nEnter the property siteUrl EXACTLY as it appears in GSC.")
    print("Examples:")
    print("  - https://www.example.com/      (URL-prefix property)")
    print("  - sc-domain:example.com        (Domain property)\n")
    site_url = input("siteUrl: ").strip()
    if not site_url:
        print("siteUrl is required.", file=sys.stderr)
        sys.exit(1)

    urls_file = input("Path to URL list file (default: input/urls.txt): ").strip() or "input/urls.txt"
    language_code = input("languageCode (default: en-US): ").strip() or "en-US"

    try:
        urls = read_urls(urls_file)
    except Exception as e:
        print(f"Could not read URLs: {e}", file=sys.stderr)
        sys.exit(1)

    if not urls:
        print(f"No URLs found in {urls_file}", file=sys.stderr)
        sys.exit(1)

    out_dir = os.path.join("results", date.today().isoformat())
    mkdirp(out_dir)
    out_csv = os.path.join(out_dir, "index_status.csv")

    print(f"\nInspecting {len(urls)} URLs under property: {site_url}")
    print(f"Input file: {urls_file}")
    print(f"Saving results to: {out_csv}\n")

    fieldnames = [
        "inspection_url",
        "site_url",
        "verdict",
        "coverage_state",
        "indexing_state",
        "robots_txt_state",
        "page_fetch_state",
        "crawled_as",
        "last_crawl_time",
        "canonical_google",
        "canonical_user",
        "referring_urls_count",
        "error",
    ]

    # Gentle pacing to reduce 429s. Increase if you hit rate limits.
    delay_seconds = 0.15

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, u in enumerate(urls, start=1):
            row = {k: "" for k in fieldnames}
            row["inspection_url"] = u
            row["site_url"] = site_url

            try:
                resp = inspect_url(service, site_url=site_url, inspection_url=u, language_code=language_code)
                index_result = safe_get(resp, ["inspectionResult", "indexStatusResult"], {}) or {}

                row["verdict"] = index_result.get("verdict", "")
                row["coverage_state"] = index_result.get("coverageState", "")
                row["indexing_state"] = index_result.get("indexingState", "")
                row["robots_txt_state"] = index_result.get("robotsTxtState", "")
                row["page_fetch_state"] = index_result.get("pageFetchState", "")
                row["crawled_as"] = index_result.get("crawledAs", "")
                row["last_crawl_time"] = index_result.get("lastCrawlTime", "")
                row["canonical_google"] = index_result.get("googleCanonical", "")
                row["canonical_user"] = index_result.get("userCanonical", "")
                refs = index_result.get("referringUrls") or []
                row["referring_urls_count"] = str(len(refs))

            except HttpError as e:
                # Keep going but log the error in the CSV
                status = getattr(e, "status_code", None)
                row["error"] = f"HttpError: {e}"

                # Basic backoff for transient errors / rate limiting
                if status in (429, 500, 503):
                    backoff = min(10, 1 + (i % 5) * 1.5)
                    time.sleep(backoff)

            except Exception as e:
                row["error"] = f"Error: {e}"

            writer.writerow(row)

            if i % 25 == 0:
                print(f"Processed {i}/{len(urls)}...")

            time.sleep(delay_seconds)

    print("\nDone.")
    print(f"CSV saved: {out_csv}\n")
    print("Tip: If you see many permission errors, your URLs may not belong to the chosen property (domain vs URL-prefix mismatch).")


if __name__ == "__main__":
    main()