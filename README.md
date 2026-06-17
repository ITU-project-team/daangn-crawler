# Karrot Community Research Crawler

Public-safe scaffold for collecting locally approved community-post samples for
research. The repository keeps code only. Collected rows, progress files, local
exports, sessions, and derived datasets are ignored by Git.

## Install

```bash
pip install aiohttp rich openpyxl
```

## Use

Run only for an approved, limited ID range and comply with the service terms,
robots guidance, applicable law, and institutional review requirements.

```bash
python app.py --start START_ID --end END_ID --step 100 --rps 1 --concurrency 1
```

The crawler defaults to conservative request settings and writes local CSV
outputs. Do not commit collected text, post identifiers, author metadata,
progress files, or exports.

## Files

| File | Role |
| --- | --- |
| `app.py` | Terminal UI and CLI entry point |
| `crawler.py` | Async request loop with conservative backoff |
| `export.py` | Local CSV/XLSX export helper |
| `make_chunks.py` | Optional local chunk helper |
| `seoul_regions.json` | Seoul administrative region lookup |

## Data Boundary

Tracked files must remain code, public configuration, and documentation only.
The `.gitignore` excludes local crawl outputs, exports, progress snapshots,
raw/processed data folders, browser/session files, and local assistant files.
