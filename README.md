# 🏆 Hackathon Finder → Google Calendar

A Python script that automatically finds upcoming in-person hackathons across Europe, filters them by distance from your home, and adds them directly to Google Calendar — complete with deadline reminders.

> Built because manually searching Devpost, Eventbrite, and newsletters every week was a waste of time. It found [Berlin Hack]([https://luma.com/bigberlinhack?tk=ynNd3O]) for me.

---

## How it works

The script runs in 4 phases:

```
Phase 1 — Scraping      →  Devpost, MLH, Eventbrite, Hackathons.com (parallel)
Phase 2 — Agentic Search →  80+ DuckDuckGo queries + snowball crawling
Phase 3 — LLM Extraction →  Ollama parses raw HTML into structured events
Phase 4 — Filter + Push  →  Geo filter, link check, Google Calendar push
```

Each phase saves a checkpoint. If anything crashes, re-run and it picks up where it left off.

---

## Features

- **Parallel scraping** of Devpost API, MLH, Eventbrite (IT/DE/CH/FR), Hackathons.com
- **Agentic search** with 80+ seed queries covering cities, themes, orgs, languages (IT/EN/DE/FR/ES)
- **Snowball crawling** — follows links to related event pages automatically
- **Local LLM extraction** via [Ollama](https://ollama.com) — no API keys, no costs
- **Geo filter** from Padova (configurable) with estimated travel cost
- **Checkpoint system** — resume from any phase after a crash
- **Google Calendar push** with color-coded priority, deadline reminders, and registration open alerts
- **Smart dedup** — fuzzy name matching to avoid duplicate events

---

## Requirements

- Python 3.10+
- [Ollama](https://ollama.com) running locally with a model pulled

```bash
pip install requests beautifulsoup4 ollama google-auth google-auth-oauthlib \
            google-api-python-client rich ddgs
```

```bash
ollama pull qwen2.5:7b   # or any model you prefer
```

---

## Google Calendar Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a project → enable **Google Calendar API**
3. Create OAuth 2.0 credentials → download as `credentials.json`
4. Place `credentials.json` in the same folder as the script
5. First run will open a browser for OAuth authorization → saves `token.json`

---

## Usage

```bash
# Standard run — resumes from checkpoint if available
python hackathon_to_gcal.py

# Dry run — shows events without adding to calendar
python hackathon_to_gcal.py --dry-run

# Use a different Ollama model
python hackathon_to_gcal.py --model llama3.2:3b

# More searches (default: 80)
python hackathon_to_gcal.py --max-searches 150

# Use Google search instead of DuckDuckGo
python hackathon_to_gcal.py --search-engine google

# Use a self-hosted SearXNG instance
python hackathon_to_gcal.py --search-engine searxng --searxng http://localhost:8888
```

### Checkpoint control

```bash
# Full reset — start from scratch
python hackathon_to_gcal.py --reset

# Resume from a specific phase
python hackathon_to_gcal.py --reset-from 2   # redo search + extraction + filters
python hackathon_to_gcal.py --reset-from 3   # redo only Ollama extraction + filters
python hackathon_to_gcal.py --reset-from 4   # redo only filters (tweak without re-scraping)
```

| Phase | What it does | Checkpoint file |
|-------|-------------|-----------------|
| 1 | Direct scraping | `checkpoints/fase1_scraper.json` |
| 2 | Agentic search + crawling | `checkpoints/fase2_pages.json` |
| 3 | Ollama LLM extraction | `checkpoints/fase3_extracted.json` |
| 4 | Geo filter + link verification | `checkpoints/fase4_filtered.json` |

---

## Configuration

Edit the constants at the top of the script:

```python
OLLAMA_MODEL      = "qwen2.5:7b"    # any Ollama model
HOME_LAT          = 45.4064         # your latitude
HOME_LON          = 11.8768         # your longitude (default: Padova, Italy)
MAX_DISTANCE_KM   = 1000            # max distance to consider
MAX_TRANSPORT_COST = 200            # max estimated travel cost in €
CONF_MAX_DIST_KM  = 100             # max distance for conferences (stricter)
PARALLEL_WORKERS  = 6               # I/O threads
OLLAMA_WORKERS    = 2               # Ollama threads (GPU-bound, keep ≤ 3)
```

---

## Output

Events are saved to `hackathons_trovati.json` and added to Google Calendar with:

- 🏆 Main event (color-coded by distance: 🟢 <200km / 🟡 <500km / 🔴 >500km)
- 📅 Deadline reminder (email 7 days before, 3 days before + popup 1 day before)
- 🟢 Registration open alert (if available)

---

## Project structure

```
hackathon_to_gcal.py     # main script
credentials.json         # Google OAuth credentials (you provide this)
token.json               # auto-generated after first auth
hackathon_cache.json     # page cache (avoids re-fetching, expires after 3 days)
hackathons_trovati.json  # last run results
checkpoints/
  fase1_scraper.json
  fase2_pages.json
  fase3_extracted.json
  fase4_filtered.json
```

---

## Troubleshooting

**`model 'qwen2.5:7b' not found`**
```bash
ollama pull qwen2.5:7b
# or check what you have:
ollama list
# then pass it explicitly:
python hackathon_to_gcal.py --model llama3.2:3b
```

**0 events found after Phase 3**
Ollama is running but returning empty results. Try a larger model or check with `--paste` mode to test extraction manually:
```bash
python hackathon_to_gcal.py --paste
# paste some event text, then type END
```

**All events filtered out in Phase 4**
Most common cause: all events extracted as `"online": true`. Use `--reset-from 4` after tweaking the filter logic rather than re-running everything.

**Rate limited by DuckDuckGo**
Switch to SearXNG (self-hosted, no limits) or slow down with `--max-searches 40`.

---

## License

MIT
