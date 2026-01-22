#!/usr/bin/env python3
"""
Keyword Performance from a Keyword List (Google Search Console API)

What it does:
- Prompts you to choose/paste a GSC property (siteUrl)
- Prompts you for a date range (YYYY-MM-DD)
- Reads keywords from a text file (one keyword per line)
- Queries GSC Search Analytics API for each keyword (exact match)
- Saves results to: results/YYYY-MM-DD/keyword_performance/keywords.csv

Works whether your auth files are in:
- project root: client_secret.json + token.json
OR
- .secrets/: .secrets/client_secret.json + .secrets/token.json

Prereqs:
- Enable "Google Search Console API" in Google Cloud Console
- Create OAuth Client ID (Desktop app)
- Download credentials as client_secret.json

Install deps:
  pip install google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2
"""

from __future__ import annotations

import csv
import os
from datetime import date
from typing import List, Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]


def first_existing_path(paths: List[str]) -> Optional[str]:
    for p in paths:
        if p and os.path.exists(p):
            return p
    return None


def ensure_parent_dir(file_path: str) -> None:
    parent = os.path.dirname(file_path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def get_service():
    client_secret = first_existing_path(
        ["client_secret.json", os.path.join(".secrets", "client_secret.json")]
    )
    if not client_secret:
        raise FileNotFoundError(
            "Missing client_secret.json (root or .secrets/)."
        )

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


def read_list(path: str) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")

    items: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            v = line.strip()
            if not v or v.startswith("#"):
                continue
            items.append(v)

    # de-dup, preserve order
    seen = set()
    out = []
    for v in items:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def prompt_date(label: str) -> str:
    s = input(f"{label} (YYYY-MM-DD): ").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise ValueError(f"Invalid date format: {s}")
    return s


def main():
    service = get_service()

    # Properties
    sites = service.sites().list().execute().get("siteEntry", []) or []
    if sites:
        print("\nAvailable properties:")
        for s in sites:
            print(f" - {s.get('siteUrl')}")

    site_url = input("\nEnter siteUrl: ").strip()
    if not site_url:
        raise SystemExit("siteUrl is required.")

    # Date range
    start_date = prompt_date("Start date")
    end_date = prompt_date("End date")

    # Inputs
    keywords_file = input(
        "Keyword file (default: input/keywords.txt): "
    ).strip() or "input/keywords.txt"

    match_type = input(
        "Match type: equals or contains (default: equals): "
    ).strip().lower() or "equals"

    if match_type not in ("equals", "contains"):
        raise ValueError("Match type must be 'equals' or 'contains'.")

    keywords = read_list(keywords_file)
    if not keywords:
        raise SystemExit("No keywords found.")

    # Output
    out_dir = os.path.join("results", date.today().isoformat())
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, "keywords_performance.csv")

    fieldnames = [
        "keyword",
        "clicks",
        "impressions",
        "ctr",
        "position",
        "start_date",
        "end_date",
        "match_type",
    ]

    print(f"\nSaving results to: {out_csv}\n")

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for kw in keywords:
            body = {
                "startDate": start_date,
                "endDate": end_date,
                "dimensions": ["query"],
                "dimensionFilterGroups": [
                    {
                        "filters": [
                            {
                                "dimension": "query",
                                "operator": match_type,
                                "expression": kw,
                            }
                        ]
                    }
                ],
                "rowLimit": 25000,
            }

            try:
                resp = service.searchanalytics().query(
                    siteUrl=site_url, body=body
                ).execute()

                rows = resp.get("rows", []) or []

                if not rows:
                    writer.writerow(
                        {
                            "keyword": kw,
                            "start_date": start_date,
                            "end_date": end_date,
                            "match_type": match_type,
                            "clicks": 0,
                            "impressions": 0,
                            "ctr": 0,
                            "position": "",
                        }
                    )
                    continue

                total_clicks = 0.0
                total_impr = 0.0
                weighted_pos = 0.0

                for r in rows:
                    c = float(r.get("clicks", 0))
                    im = float(r.get("impressions", 0))
                    pos = float(r.get("position", 0))
                    total_clicks += c
                    total_impr += im
                    weighted_pos += pos * im

                ctr = total_clicks / total_impr if total_impr > 0 else 0
                avg_pos = weighted_pos / total_impr if total_impr > 0 else ""

                writer.writerow(
                    {
                        "keyword": kw,
                        "start_date": start_date,
                        "end_date": end_date,
                        "match_type": match_type,
                        "clicks": int(round(total_clicks)),
                        "impressions": int(round(total_impr)),
                        "ctr": round(ctr, 4),
                        "position": round(avg_pos, 2) if avg_pos != "" else "",
                    }
                )

            except HttpError as e:
                print(f"Error for '{kw}': {e}")
                writer.writerow(
                    {
                        "keyword": kw,
                        "start_date": start_date,
                        "end_date": end_date,
                        "match_type": match_type,
                        "clicks": "",
                        "impressions": "",
                        "ctr": "",
                        "position": "",
                    }
                )

    print("Done.")


if __name__ == "__main__":
    main()