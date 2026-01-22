# Google Search Console API – Python Scripts for SEOs

A collection of **Python scripts** for working with the **Google Search Console API**, built for **technical SEOs** who want to automate data extraction, audits, and analysis locally.

This repository is designed to:
- Support **multiple GSC properties**
- Run scripts **from the terminal**
- Keep credentials **out of version control**
- Output data as **CSV files** for further analysis

---

## What’s in this repository

This repo contains **independent scripts**, each focused on a specific Google Search Console use case.

Current scripts:

| Script | Description |
|------|-------------|
| `page_indexing.py` | Check page indexing status for a list of URLs using the URL Inspection API |
| `keyword_performance.py` | Check performance data for a list of keywords during a specified date range |


More scripts can be added over time (performance exports, coverage audits, etc.).

---

## Repository structure

```text
gsc-api/
├── scripts/
│   └── page_indexing.py
├── data/
│   ├── input/
│   └── output/
├── .gitignore
├── README.md
└── LICENSE
```

Each script:
- Reads input from the `input/` folder
- Writes output to the `results/` folder
- Prompts for the relevant GSC property (`siteUrl`) in the terminal

---

## Requirements

- Python 3.9+
- A Google account with access to the relevant Search Console properties
- Google Cloud project with **Google Search Console API enabled**


## Security

This repository ignores:
	•	OAuth credentials
	•	Access tokens
	•	Input files
	•	Generated results

If credentials are ever committed by mistake, rotate them immediately in Google Cloud Console.

## Contributing

Feedback are welcome:
	•	New scripts
	•	Improvements to existing ones
	•	Better error handling
	•	Documentation enhancements

Contact me: [Aubrey Yung](https://aubreyyung.com/)
Last Update: 2026-01-23

---
