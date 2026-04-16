#!/usr/bin/env python3
"""
hackathon_to_gcal.py  v5.1 — Smart Hackathon Finder (Parallel)

Features:
  - Scraping diretto parallelizzato (Devpost/MLH/Eventbrite/Hackathons.com)
  - Ricerca agentica + snowball search
  - Fetch pagine in parallelo (ThreadPoolExecutor)
  - Estrazione Ollama parallelizzata (una pagina per thread)
  - Verifica link in parallelo
  - Filtro geo da Padova + durata + no conferenze lontane
  - Dedup intelligente + cache
  - Colori calendario per priorità distanza

Dipendenze:
  pip install requests beautifulsoup4 ollama google-auth google-auth-oauthlib \
              google-api-python-client rich ddgs
"""

import argparse, json, re, sys, time, hashlib, math
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import ollama, requests
from bs4 import BeautifulSoup
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import print as rprint

from google.auth.transport.requests import Request as GRequest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build as gbuild

# ─── CONFIG ──────────────────────────────────────────────────────────────────

OLLAMA_MODEL         = "qwen2.5:7b"
CALENDAR_ID          = "primary"
SCOPES               = ["https://www.googleapis.com/auth/calendar"]
TOKEN_FILE           = Path("token.json")
CREDS_FILE           = Path("credentials.json")
CACHE_FILE           = Path("hackathon_cache.json")
OUTPUT_FILE          = Path("hackathons_trovati.json")

# Checkpoint — riprendere da dove si è interrotti
CKPT_DIR        = Path("checkpoints")
CKPT_SCRAPER    = CKPT_DIR / "fase1_scraper.json"
CKPT_PAGES      = CKPT_DIR / "fase2_pages.json"
CKPT_EXTRACTED  = CKPT_DIR / "fase3_extracted.json"
CKPT_FILTERED   = CKPT_DIR / "fase4_filtered.json"

MAX_SEARCHES_DEFAULT = 80
MAX_FETCH_PER_SEARCH = 5
SNOWBALL_MAX         = 3
PARALLEL_WORKERS     = 6      # thread per fetch/verifica (I/O bound)
OLLAMA_WORKERS       = 2      # thread per Ollama (GPU bound — 2 è il max sicuro)

HOME_LAT, HOME_LON  = 45.4064, 11.8768
MAX_DISTANCE_KM      = 1000
MAX_TRANSPORT_COST    = 200
MIN_DURATION_FAR_H   = 24
FAR_THRESHOLD_KM     = 200
CONF_MAX_DIST_KM     = 100

console = Console()
TODAY = datetime.today()
YEAR  = TODAY.year

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,it;q=0.8",
}

# ═══════════════════════════════════════════════════════════════════════════════
# GEO
# ═══════════════════════════════════════════════════════════════════════════════

CITIES = {
    "padova":(45.4064,11.8768),"padua":(45.4064,11.8768),
    "milano":(45.4643,9.19),"milan":(45.4643,9.19),"roma":(41.9028,12.4964),"rome":(41.9028,12.4964),
    "torino":(45.0703,7.6869),"turin":(45.0703,7.6869),"bologna":(44.4949,11.3426),
    "firenze":(43.7696,11.2558),"florence":(43.7696,11.2558),
    "napoli":(40.8518,14.2681),"naples":(40.8518,14.2681),
    "venezia":(45.4408,12.3155),"venice":(45.4408,12.3155),
    "genova":(44.4056,8.9463),"verona":(45.4384,10.9916),"trieste":(45.6495,13.7768),
    "trento":(46.0748,11.1217),"bolzano":(46.4983,11.3548),
    "modena":(44.6471,10.9252),"parma":(44.8015,10.3279),
    "bari":(41.1171,16.8719),"catania":(37.5079,15.087),"palermo":(38.1157,13.3615),
    "cagliari":(39.2238,9.1217),"perugia":(43.1107,12.3908),"ancona":(43.6158,13.5188),
    "treviso":(45.6669,12.245),"vicenza":(45.5455,11.5354),"udine":(46.0711,13.2346),
    "brescia":(45.5416,10.2118),"bergamo":(45.6983,9.6699),"pisa":(43.7228,10.4017),
    "siena":(43.3188,11.3308),"lecce":(40.3516,18.171),"rimini":(44.0594,12.5681),
    "como":(45.8081,9.0852),"pavia":(45.1847,9.16),"ferrara":(44.8381,11.6199),
    "ravenna":(44.4184,12.2035),"reggio emilia":(44.6989,10.631),
    "zurich":(47.3769,8.5417),"zürich":(47.3769,8.5417),"geneva":(46.2044,6.1432),
    "bern":(46.948,7.4474),"lausanne":(46.5197,6.6323),"basel":(47.5596,7.5886),
    "lugano":(46.0037,8.9511),"st. gallen":(47.4245,9.3767),
    "vienna":(48.2082,16.3738),"wien":(48.2082,16.3738),"innsbruck":(47.2692,11.3928),
    "graz":(47.0707,15.4395),"salzburg":(47.8095,13.055),"linz":(48.3069,14.2858),
    "munich":(48.1351,11.582),"münchen":(48.1351,11.582),"stuttgart":(48.7758,9.1829),
    "frankfurt":(50.1109,8.6821),"berlin":(52.52,13.405),"nuremberg":(49.4521,11.0767),
    "freiburg":(47.999,7.8421),
    "paris":(48.8566,2.3522),"lyon":(45.764,4.8357),"marseille":(43.2965,5.3698),
    "nice":(43.7102,7.262),"grenoble":(45.1885,5.7245),
    "ljubljana":(46.0569,14.5058),"zagreb":(45.815,15.9819),
    "london":(51.5074,-0.1278),"amsterdam":(52.3676,4.9041),
    "brussels":(50.8503,4.3517),"bruxelles":(50.8503,4.3517),
    "barcelona":(41.3874,2.1686),"madrid":(40.4168,-3.7038),
    "lisbon":(38.7223,-9.1393),"dublin":(53.3498,-6.2603),
    "prague":(50.0755,14.4378),"budapest":(47.4979,19.0402),
    "warsaw":(52.2297,21.0122),"copenhagen":(55.6761,12.5683),
    "stockholm":(59.3293,18.0686),"helsinki":(60.1699,24.9384),
    "athens":(37.9838,23.7275),"bucharest":(44.4268,26.1025),
    "vilnius":(54.6872,25.2798),"tallinn":(59.437,24.7536),
}

def haversine(lat1,lon1,lat2,lon2):
    R=6371; dlat=math.radians(lat2-lat1); dlon=math.radians(lon2-lon1)
    a=math.sin(dlat/2)**2+math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R*2*math.asin(math.sqrt(a))

def get_city_distance(city_str):
    if not city_str: return None
    c = city_str.lower().strip()
    for key in [c, c.split(",")[0].strip(), c.split(" ")[0].strip()]:
        if key in CITIES: return haversine(HOME_LAT,HOME_LON,*CITIES[key])
    for key in CITIES:
        if key in c: return haversine(HOME_LAT,HOME_LON,*CITIES[key])
    return None

def estimate_transport(d):
    if d is None: return None
    if d<50: return 10
    if d<200: return 30
    if d<500: return 70
    if d<800: return 120
    if d<1200: return 180
    return 250

# ═══════════════════════════════════════════════════════════════════════════════
# SEARCH BACKENDS
# ═══════════════════════════════════════════════════════════════════════════════

class SearchBackend:
    def search(self, query, max_results=10): raise NotImplementedError

class DuckDuckGoBackend(SearchBackend):
    def __init__(self):
        self._ddgs = None
        try:
            from ddgs import DDGS; self._ddgs = DDGS
        except ImportError:
            try:
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    from duckduckgo_search import DDGS; self._ddgs = DDGS
            except ImportError:
                console.print("[red]pip install ddgs[/red]"); sys.exit(1)
    def search(self, query, max_results=10):
        try:
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                with self._ddgs() as d: results = list(d.text(query, max_results=max_results))
            return [{"title":r.get("title",""),"url":r.get("href",r.get("link","")),"snippet":r.get("body","")} for r in results]
        except: return []

class GoogleBackend(SearchBackend):
    def __init__(self): from googlesearch import search as gs; self._gs = gs
    def search(self, query, max_results=10):
        try: return [{"title":"","url":u,"snippet":""} for u in self._gs(query,num_results=max_results,sleep_interval=3)]
        except: return []

class SearXNGBackend(SearchBackend):
    def __init__(self, url): self.url = url.rstrip("/")
    def search(self, query, max_results=10):
        try:
            r = requests.get(f"{self.url}/search",params={"q":query,"format":"json"},timeout=15)
            return [{"title":x.get("title",""),"url":x.get("url",""),"snippet":x.get("content","")} for x in r.json().get("results",[])[:max_results]]
        except: return []

def get_backend(engine, url=None):
    if engine=="searxng": return SearXNGBackend(url or "http://localhost:8888")
    if engine=="google": return GoogleBackend()
    return DuckDuckGoBackend()

# ═══════════════════════════════════════════════════════════════════════════════
# WEB FETCHER + SNOWBALL (parallelizzabile)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_page(url, max_chars=10000):
    if not url: return "", []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        related = []
        for a in soup.find_all("a", href=True):
            href, txt = a["href"], a.get_text(strip=True).lower()
            if any(kw in txt for kw in ["hackathon","event","challenge","workshop","altri eventi","more events","prossimi"]):
                full = urljoin(url, href)
                if full != url and full.startswith("http"): related.append(full)
            if any(d in href for d in ["devpost.com/software","eventbrite.com/e/","eventbrite.it/e/","meetup.com/events","lu.ma/"]):
                full = urljoin(url, href)
                if full != url: related.append(full)
        for tag in soup(["script","style","nav","footer","header","aside","iframe","noscript"]): tag.decompose()
        text = re.sub(r'\n{3,}','\n\n', soup.get_text(separator="\n",strip=True))
        return text[:max_chars], related[:SNOWBALL_MAX*2]
    except: return "", []

def fetch_pages_parallel(urls, seen_hashes):
    """Fetcha più pagine in parallelo."""
    results = []
    to_fetch = []
    for u in urls:
        h = url_hash(u)
        if h not in seen_hashes and is_promising_url(u):
            seen_hashes.add(h)
            to_fetch.append(u)

    if not to_fetch: return results, []

    all_related = []
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as ex:
        futures = {ex.submit(fetch_page, u): u for u in to_fetch}
        for future in as_completed(futures):
            url = futures[future]
            try:
                text, related = future.result()
                if text and len(text) > 150:
                    results.append({"url": url, "text": text})
                    all_related.extend(related)
            except: pass
    return results, all_related

def verify_link(url):
    if not url: return False
    try:
        r = requests.head(url, headers=HEADERS, timeout=8, allow_redirects=True)
        return r.status_code < 400
    except: return False

def verify_links_parallel(events):
    """Verifica tutti i link in parallelo."""
    urls = [(i, e.get("url","")) for i,e in enumerate(events) if e.get("url")]
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as ex:
        futures = {ex.submit(verify_link, url): idx for idx, url in urls}
        for future in as_completed(futures):
            idx = futures[future]
            try: events[idx]["link_verificato"] = future.result()
            except: events[idx]["link_verificato"] = False
    return events

def url_hash(url): return hashlib.md5(url.encode()).hexdigest()[:12]

SKIP_DOMAINS = {"youtube.com","twitter.com","x.com","facebook.com","instagram.com",
                "linkedin.com/posts","reddit.com","wikipedia.org","amazon.com","tiktok.com","pinterest.com"}
GOOD_DOMAINS = {"devpost.com","eventbrite","mlh.io","hackathon","f6s.com","lablab.ai",
                "meetup.com","lu.ma","gdg.community","developers.google","codemotion","pycon",
                "polihub","i3p","h-farm","hfarm","ogr","talentgarden","cariplo","lventure",
                "noi.bz","almacube","cdpventure","socialfare","startupweekend"}

def is_promising_url(url):
    u = url.lower()
    if any(d in u for d in SKIP_DOMAINS): return False
    if u.endswith((".pdf",".jpg",".png",".gif",".mp4")): return False
    if any(d in u for d in GOOD_DOMAINS): return True
    return any(k in u for k in ["hackathon","hack","workshop","meetup","event","startup",
                                 "accelerat","incubat","devfest","challenge","competition","bootcamp"])

# ═══════════════════════════════════════════════════════════════════════════════
# CACHE
# ═══════════════════════════════════════════════════════════════════════════════

def load_cache():
    if CACHE_FILE.exists():
        try:
            data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            ts = data.get("timestamp","")
            if ts and (TODAY - datetime.fromisoformat(ts)).days > 3:
                console.print("[dim]Cache scaduta (>3 giorni)[/dim]")
                return {"pages":{},"events":[],"timestamp":""}
            console.print(f"[dim]Cache: {len(data.get('pages',{}))} pagine, {len(data.get('events',[]))} eventi[/dim]")
            return data
        except: pass
    return {"pages":{},"events":[],"timestamp":""}

def save_cache(cache):
    cache["timestamp"] = TODAY.isoformat()
    CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=1), encoding="utf-8")

# ═══════════════════════════════════════════════════════════════════════════════
# CHECKPOINT
# ═══════════════════════════════════════════════════════════════════════════════

def ckpt_save(path: Path, data, label: str = ""):
    CKPT_DIR.mkdir(exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    console.print(f"  [dim]💾 Checkpoint: {path.name} ({label})[/dim]")

def ckpt_load(path: Path):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            console.print(f"  [yellow]⚠ Checkpoint corrotto ({path.name}): {e} — ricalcolo[/yellow]")
    return None

def ckpt_clear(paths=None):
    for p in (paths or [CKPT_SCRAPER, CKPT_PAGES, CKPT_EXTRACTED, CKPT_FILTERED]):
        if p.exists():
            p.unlink()
            console.print(f"  [dim]🗑 Rimosso: {p.name}[/dim]")

# ═══════════════════════════════════════════════════════════════════════════════
# FASE 1: SCRAPING DIRETTO (parallelizzato)
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_devpost():
    events, seen = [], set()
    for status in ["upcoming","open"]:
        for ctype in ["in-person","online"]:
            for page in range(1, 4):
                try:
                    r = requests.get("https://devpost.com/api/hackathons",
                        params={"challenge_type[]":ctype,"status[]":status,"order_by":"deadline",
                                "per_page":50,"page":page}, headers=HEADERS, timeout=15)
                    hacks = r.json().get("hackathons",[])
                    if not hacks: break
                    for h in hacks:
                        uid = h.get("url","")
                        if uid in seen: continue
                        seen.add(uid)
                        if h.get("open_state") in ("ended","closed"): continue
                        ds = h.get("submission_period_dates","")
                        yrs = re.findall(r'20\d{2}', ds)
                        if yrs and max(int(y) for y in yrs) < YEAR-1: continue
                        themes = ', '.join(t if isinstance(t,str) else t.get('name','') for t in h.get('themes',[]))
                        events.append({"source":"devpost","url":uid,"type":ctype,
                            "raw":f"Title: {h.get('title','')}\nLocation: {h.get('displayed_location',{}).get('location','Online')}\nDates: {ds}\nThemes: {themes}\nTime left: {h.get('time_left_to_submission','')}\nURL: {uid}"})
                except: break
    return events

def scrape_mlh():
    events = []
    for url in [f"https://mlh.io/seasons/{YEAR}/events",f"https://mlh.io/seasons/{YEAR+1}/events"]:
        try:
            soup = BeautifulSoup(requests.get(url,headers=HEADERS,timeout=15).text,"html.parser")
            for card in soup.select(".event, article.event-wrapper, .event-content")[:50]:
                text = card.get_text(separator="\n",strip=True)[:600]
                link = card.find("a",href=True)
                href = link["href"] if link else ""
                if href and not href.startswith("http"): href = "https://mlh.io"+href
                if text: events.append({"source":"mlh","raw":text,"url":href})
        except: pass
    return events

def scrape_eventbrite():
    events, seen = [], set()
    urls = [
        "https://www.eventbrite.com/d/europe/hackathon/",
        "https://www.eventbrite.it/d/italy/hackathon/",
        "https://www.eventbrite.it/d/italy--milano/hackathon/",
        "https://www.eventbrite.it/d/italy--roma/hackathon/",
        "https://www.eventbrite.it/d/italy--torino/hackathon/",
        "https://www.eventbrite.it/d/italy--padova/hackathon/",
        "https://www.eventbrite.it/d/italy--bologna/hackathon/",
        "https://www.eventbrite.com/d/europe/ai-hackathon/",
        "https://www.eventbrite.com/d/europe/startup-weekend/",
        "https://www.eventbrite.it/d/italy/workshop-intelligenza-artificiale/",
        "https://www.eventbrite.com/d/switzerland--zurich/hackathon/",
        "https://www.eventbrite.com/d/germany--munich/hackathon/",
    ]
    for page_url in urls:
        for pg in ["","?page=2"]:
            try:
                soup = BeautifulSoup(requests.get(page_url+pg,headers=HEADERS,timeout=15).text,"html.parser")
                for card in soup.select("[data-testid='event-card'], .search-event-card, article")[:25]:
                    text = card.get_text(separator="\n",strip=True)[:500]
                    link = card.find("a",href=True)
                    href = link["href"] if link else ""
                    if href in seen or len(text)<30: continue
                    seen.add(href); events.append({"source":"eventbrite","raw":text,"url":href})
            except: pass
    return events

def scrape_hackathonscom():
    events = []
    for u in ["https://www.hackathons.com/hackathons?country=EU&type=inperson",
              "https://www.hackathons.com/hackathons?country=IT",
              "https://www.hackathons.com/hackathons?country=CH",
              "https://www.hackathons.com/hackathons?country=DE",
              "https://www.hackathons.com/hackathons?country=AT",
              "https://www.hackathons.com/hackathons?country=FR",
              "https://www.hackathons.com/hackathons?country=SI"]:
        try:
            soup = BeautifulSoup(requests.get(u,headers=HEADERS,timeout=15).text,"html.parser")
            for card in soup.select(".hackathon-card, .event-card, article")[:30]:
                text = card.get_text(separator="\n",strip=True)[:500]
                link = card.find("a",href=True); href = link["href"] if link else ""
                if href and not href.startswith("http"): href = "https://www.hackathons.com"+href
                if text: events.append({"source":"hackathons.com","raw":text,"url":href})
        except: pass
    return events

def run_scrapers_parallel(force=False):
    """Tutti gli scraper in parallelo. Riprende dal checkpoint se disponibile."""
    cached = ckpt_load(CKPT_SCRAPER)
    if cached is not None and not force:
        console.print(Panel.fit(
            f"[bold]FASE 1: Scraping[/bold] [green]↩ da checkpoint[/green] ({len(cached)} grezzi)",
            border_style="cyan"))
        return cached

    console.print(Panel.fit("[bold]FASE 1: Scraping diretto (parallelo)[/bold]", border_style="cyan"))
    all_raw = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {
            ex.submit(scrape_devpost): "Devpost",
            ex.submit(scrape_mlh): "MLH",
            ex.submit(scrape_eventbrite): "Eventbrite",
            ex.submit(scrape_hackathonscom): "Hackathons.com",
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                results = future.result()
                console.print(f"  [green]✓ {name}: {len(results)}[/green]")
                all_raw.extend(results)
            except Exception as e:
                console.print(f"  [yellow]✗ {name}: {e}[/yellow]")
    console.print(f"[bold green]Fase 1: {len(all_raw)} eventi grezzi[/bold green]")
    ckpt_save(CKPT_SCRAPER, all_raw, f"{len(all_raw)} eventi")
    return all_raw

# ═══════════════════════════════════════════════════════════════════════════════
# FASE 2: RICERCA AGENTICA + SNOWBALL (fetch parallelo)
# ═══════════════════════════════════════════════════════════════════════════════

def build_seed_queries():
    Y = YEAR
    Y1 = Y + 1
    # Mesi futuri per query specifiche per trimestre
    months_it = {1:"gennaio",2:"febbraio",3:"marzo",4:"aprile",5:"maggio",6:"giugno",
                 7:"luglio",8:"agosto",9:"settembre",10:"ottobre",11:"novembre",12:"dicembre"}
    current_month = TODAY.month

    queries = [
        # Piattaforme — anno corrente + prossimo
        f"site:devpost.com hackathon Italy Europe {Y}",
        f"site:devpost.com hackathon Italy Europe {Y1}",
        f"site:eventbrite.it hackathon {Y}",
        f"site:eventbrite.it hackathon {Y1}",
        f"site:eventbrite.com hackathon Italy {Y}",
        f"site:mlh.io hackathon Europe {Y}",
        f"site:mlh.io hackathon Europe {Y1}",
        f"site:f6s.com hackathon Italy {Y}",
        f"site:lablab.ai hackathon Europe {Y}",
        f"site:meetup.com hackathon AI Italy {Y}",
        f"site:lu.ma hackathon Italy {Y}",
        # Tematici — anno corrente
        f"hackathon intelligenza artificiale Italia {Y} in presenza",
        f"AI hackathon Europe in-person {Y}",
        f"generative AI LLM hackathon Europe {Y}",
        f"hackathon deep learning data science Italia {Y}",
        f"startup weekend Italia {Y}",
        f"hackathon coding programmazione Italia {Y}",
        f"hackathon blockchain fintech Italia {Y}",
        f"hackathon robotics IoT Italia {Y}",
        f"hackathon cybersecurity Italia {Y}",
        f"hackathon healthtech medtech Italia {Y}",
        f"hackathon ESA space Italia Europe {Y}",
        f"hackathon open innovation corporate Italia {Y}",
        f"hackathon university students Italy Europe {Y}",
        # Tematici — anno prossimo
        f"hackathon AI Italy Europe {Y1}",
        f"hackathon coding startup Italy {Y1}",
        f"hackathon Europe in-person {Y1}",
        # Città
        f"hackathon Milano {Y} in presenza",
        f"hackathon Torino {Y} in presenza",
        f"hackathon Roma Bologna {Y}",
        f"hackathon Padova Venezia Verona {Y}",
        f"hackathon Zurich Munich {Y} in-person",
        f"hackathon Napoli Bari Palermo {Y}",
        f"hackathon Firenze Genova Trieste {Y}",
        # GDG
        f"GDG DevFest hackathon Italia {Y}",
        f"Google I/O Extended Italia {Y}",
        # Workshop
        f"workshop AI machine learning Italia {Y}",
        f"PyCon Codemotion RustLab Italia {Y}",
        f"AWS Community Day Italia {Y}",
    ]

    # Aggiungi query per ogni trimestre futuro (fino a fine anno prossimo)
    for y in [Y, Y1]:
        for q_start in [1, 4, 7, 10]:
            q_end = q_start + 2
            if y == Y and q_end < current_month:
                continue  # trimestre passato, salta
            q_name = f"Q{(q_start-1)//3+1}"
            m1 = months_it.get(q_start, "")
            m3 = months_it.get(q_end, "")
            queries.append(f"hackathon Italia {m1} {m3} {y}")
            queries.append(f"hackathon Europe in-person {m1} {m3} {y}")

    return queries

QUERY_GEN_PROMPT = """Generate {n} search queries for upcoming IN-PERSON hackathons and coding competitions within 1000km of Padova, Italy.
Search from {today} through the end of {next_year}.
IMPORTANT: Include queries for {year} AND {next_year}. Search for events in ALL upcoming months, not just the next 1-2.
Only events with OPEN or UPCOMING registrations.
Mix Italian/English. Vary: cities, platforms, themes, specific months/seasons.
Already searched: {done}
Return ONLY JSON array: ["q1","q2",...]"""

def gen_queries(done, n=15):
    try:
        r = ollama.generate(model=OLLAMA_MODEL, prompt=QUERY_GEN_PROMPT.format(
            n=n,today=TODAY.strftime("%Y-%m-%d"),year=YEAR,next_year=YEAR+1,
            done=json.dumps(done[-40:]) if done else "[]"),
            options={"temperature":0.8,"num_predict":2048})
        out = re.sub(r"```json\s*","",re.sub(r"```\s*","",r["response"].strip()))
        m = re.search(r'\[.*\]',out,re.DOTALL)
        if m: return [q for q in json.loads(m.group()) if isinstance(q,str) and len(q)>5]
    except: pass
    return []

def run_agentic_search(backend, max_searches, cache, force=False):
    cached = ckpt_load(CKPT_PAGES)
    if cached is not None and not force:
        console.print(Panel.fit(
            f"[bold]FASE 2: Ricerca[/bold] [green]↩ da checkpoint[/green] ({len(cached)} pagine)",
            border_style="cyan"))
        return cached

    pages = []
    seen = set(cache.get("pages",{}).keys())
    queries_done = []; searches = 0; snowball_queue = []

    def do_search(query):
        nonlocal searches
        console.print(f"[cyan][{searches+1}/{max_searches}][/cyan] 🔍 {query[:80]}")
        results = backend.search(query, max_results=10)
        searches += 1; queries_done.append(query)
        if not results:
            console.print("  [dim]0 risultati[/dim]"); return
        urls_to_fetch = [r["url"] for r in results if r.get("url")]
        new_pages, related = fetch_pages_parallel(urls_to_fetch[:MAX_FETCH_PER_SEARCH], seen)
        pages.extend(new_pages)
        for pg in new_pages:
            cache["pages"][url_hash(pg["url"])] = {"url":pg["url"],"text":pg["text"][:3000]}
            console.print(f"  [green]📄[/green] {pg['url'][:75]}")
        snowball_queue.extend(related)
        console.print(f"  → {len(new_pages)} pagine")
        if searches % 10 == 0:
            ckpt_save(CKPT_PAGES, pages, f"{searches} ricerche / {len(pages)} pagine")

    seeds = build_seed_queries()
    console.print(Panel.fit(f"[bold]FASE 2a: Query seed ({len(seeds)})[/bold]", border_style="cyan"))
    for q in seeds:
        if searches >= max_searches: break
        do_search(q); time.sleep(1.0)

    unique_snowball = list(set(snowball_queue))[:40]
    if unique_snowball:
        console.print(Panel.fit(f"[bold]FASE 2b: Snowball ({len(unique_snowball)} link)[/bold]", border_style="blue"))
        new_pages, _ = fetch_pages_parallel(unique_snowball, seen)
        pages.extend(new_pages)
        for pg in new_pages:
            cache["pages"][url_hash(pg["url"])] = {"url":pg["url"],"text":pg["text"][:3000]}
        console.print(f"  [blue]{len(new_pages)} pagine da snowball[/blue]")

    remaining = max_searches - searches
    if remaining > 0:
        rounds = max(1, remaining // 15)
        console.print(Panel.fit(f"[bold]FASE 2c: Query agentiche ({remaining} rim.)[/bold]", border_style="magenta"))
        for rnd in range(rounds):
            if searches >= max_searches: break
            qs = gen_queries(queries_done, min(15, max_searches-searches))
            for q in qs:
                if searches >= max_searches: break
                do_search(q); time.sleep(1.0)

    console.print(f"\n[bold green]Fase 2: {searches} ricerche → {len(pages)} pagine[/bold green]")
    ckpt_save(CKPT_PAGES, pages, f"{len(pages)} pagine totali")
    return pages

# ═══════════════════════════════════════════════════════════════════════════════
# FASE 3: ESTRAZIONE OLLAMA (parallelizzata)
# ═══════════════════════════════════════════════════════════════════════════════

EXTRACT_P = """Find events in this text. Return a JSON array.
Each event needs: "nome" (name), "data_inizio" (YYYY-MM-DD or null), "data_fine" (YYYY-MM-DD or null), "luogo" (city, country), "online" (true/false), "url", "tipo" (Hackathon/Workshop/Conference/Startup/Other), "organizzatore", "deadline_candidature" (YYYY-MM-DD or null), "rilevante" (true if tech event in Europe).
Today is {today}. Year is {year}. Skip past events. Return [] if none found.

{text}"""

def extract_single(url, text):
    prompt = EXTRACT_P.format(today=TODAY.strftime("%Y-%m-%d"), year=YEAR, text=text[:4000])
    try:
        r = ollama.generate(model=OLLAMA_MODEL, prompt=prompt,
                            options={"temperature":0.1, "num_predict":2048})
        out = r["response"].strip()
        # Prova a parsare direttamente se inizia con [
        if out.startswith("["):
            try: return [e for e in json.loads(out) if isinstance(e,dict)]
            except: pass
        # Rimuovi markdown fences
        out = re.sub(r"```json\s*","",re.sub(r"```\s*","",out))
        # Cerca array JSON
        m = re.search(r'\[.*\]', out, re.DOTALL)
        if m:
            return [e for e in json.loads(m.group()) if isinstance(e,dict)]
    except json.JSONDecodeError:
        pass  # JSON malformato — normale con LLM piccoli
    except Exception as e:
        console.print(f"    [yellow]Extract err ({url[:50]}): {type(e).__name__}: {e}[/yellow]")
    return []

def extract_batch(raw_events):
    """Estrae da testi grezzi degli scraper — in batch da 3 (più piccolo = più preciso)."""
    BATCH_P = """Find events in these texts. Return a JSON array.
Each event: "nome", "data_inizio" (YYYY-MM-DD/null), "data_fine", "luogo", "online" (bool), "url", "tipo", "organizzatore", "deadline_candidature" (YYYY-MM-DD/null), "rilevante" (true if tech event in Europe).
Today: {today}. Year: {year}. Skip past events. Return [] if none.

{texts}"""
    results = []
    bs = 3  # batch più piccoli
    batches = [raw_events[i:i+bs] for i in range(0,len(raw_events),bs)]
    for bi,batch in enumerate(batches):
        if bi % 10 == 0:
            console.print(f"  Batch [{bi+1}/{len(batches)}]...")
        texts = "".join(f"\n---\nURL:{e.get('url','')}\n{e['raw'][:1500]}\n" for e in batch)
        try:
            r = ollama.generate(model=OLLAMA_MODEL,
                prompt=BATCH_P.format(today=TODAY.strftime("%Y-%m-%d"), year=YEAR, texts=texts),
                options={"temperature":0.1, "num_predict":2048})
            out = r["response"].strip()
            out = re.sub(r"```json\s*","",re.sub(r"```\s*","",out))
            m = re.search(r'\[.*\]', out, re.DOTALL)
            if m:
                parsed = json.loads(m.group())
                results.extend(e for e in parsed if isinstance(e,dict))
        except json.JSONDecodeError:
            pass
        except Exception as e:
            if bi < 3:  # logga solo i primi errori
                console.print(f"    [yellow]Batch err: {type(e).__name__}: {e}[/yellow]")
    console.print(f"  → {len(results)} eventi da {len(raw_events)} testi")
    return results

def extract_pages_parallel(pages):
    """Estrae eventi da pagine agentiche — Ollama in parallelo."""
    all_events = []
    console.print(f"  Estraggo da {len(pages)} pagine ({OLLAMA_WORKERS} thread Ollama)...")
    with ThreadPoolExecutor(max_workers=OLLAMA_WORKERS) as ex:
        futures = {ex.submit(extract_single, pg["url"], pg["text"]): pg["url"] for pg in pages}
        done = 0
        found = 0
        for future in as_completed(futures):
            done += 1
            if done % 20 == 0:
                console.print(f"  ...{done}/{len(pages)} pagine, {found} eventi trovati finora")
                ckpt_save(CKPT_EXTRACTED, all_events, f"intermedio {done}/{len(pages)}")
            try:
                result = future.result()
                if result:
                    found += len(result)
                    all_events.extend(result)
            except Exception as e:
                if done < 5:
                    console.print(f"    [yellow]Page extract err: {e}[/yellow]")
    console.print(f"  → {found} eventi da {len(pages)} pagine")
    return all_events

# ═══════════════════════════════════════════════════════════════════════════════
# DATE + DEDUP
# ═══════════════════════════════════════════════════════════════════════════════

def is_valid_date(s):
    if not s: return False
    try: return YEAR-1 <= datetime.strptime(s,"%Y-%m-%d").year <= YEAR+2
    except: return False

def normalize_date(s):
    if not s: return None
    if is_valid_date(s): return s
    for fmt in ["%d/%m/%Y","%m/%d/%Y","%d-%m-%Y","%B %d, %Y","%d %B %Y","%b %d, %Y","%d %b %Y","%Y/%m/%d","%d.%m.%Y"]:
        try:
            dt = datetime.strptime(s.strip(),fmt)
            if YEAR-1 <= dt.year <= YEAR+2: return dt.strftime("%Y-%m-%d")
        except: continue
    return None

def smart_dedup(events):
    def norm(n):
        n = re.sub(r'20\d{2}','',n.lower().strip())
        return re.sub(r'\s+',' ',re.sub(r'[^\w\s]','',n)).strip()
    def sim(a,b):
        wa,wb = set(norm(a).split()),set(norm(b).split())
        if not wa or not wb: return 0
        return len(wa&wb)/len(wa|wb)
    kept = []
    for ev in events:
        name = ev.get("nome","")
        dup = False
        for ex in kept:
            if sim(name, ex.get("nome","")) > 0.6:
                # Tieni quello con più campi compilati
                if sum(1 for v in ev.values() if v) > sum(1 for v in ex.values() if v):
                    kept.remove(ex); kept.append(ev)
                dup = True; break
        if not dup: kept.append(ev)
    return kept

# ═══════════════════════════════════════════════════════════════════════════════
# FASE 3: ORCHESTRAZIONE ESTRAZIONE
# ═══════════════════════════════════════════════════════════════════════════════

def run_extraction(raw_scraper, agentic_pages, cache, force=False):
    cached = ckpt_load(CKPT_EXTRACTED)
    if cached is not None and not force:
        console.print(Panel.fit(
            f"[bold]FASE 3: Estrazione Ollama[/bold] [green]↩ da checkpoint[/green] ({len(cached)} eventi)",
            border_style="yellow"))
        for ev in cached:
            if "distanza_km" not in ev:
                dist = get_city_distance(ev.get("luogo") or "")
                ev["distanza_km"] = round(dist) if dist else None
                ev["costo_trasporto_stimato"] = estimate_transport(dist)
        cache["events"] = cached
        return cached

    console.print(Panel.fit("[bold]FASE 3: Estrazione Ollama[/bold]", border_style="yellow"))
    all_raw = []

    if raw_scraper:
        console.print(f"\n[bold]3a. Scraper ({len(raw_scraper)} testi)...[/bold]")
        all_raw.extend(extract_batch(raw_scraper))
        console.print(f"  → {len(all_raw)} eventi da scraper")
        ckpt_save(CKPT_EXTRACTED, all_raw, f"dopo scraper: {len(all_raw)}")

    if agentic_pages:
        console.print(f"\n[bold]3b. Pagine agentiche ({len(agentic_pages)}, parallelo)...[/bold]")
        before = len(all_raw)
        all_raw.extend(extract_pages_parallel(agentic_pages))
        console.print(f"  → {len(all_raw)-before} eventi da pagine agentiche")

    console.print(f"\n  Estratti grezzi totali: {len(all_raw)}")

    cleaned = []
    for ev in all_raw:
        if ev.get("rilevante") is False:
            continue
        for f in ["data_inizio","data_fine","deadline_candidature"]:
            ev[f] = normalize_date(ev.get(f))
        if ev.get("data_inizio") and not ev.get("data_fine"):
            ev["data_fine"] = ev["data_inizio"]
        d = ev.get("data_fine") or ev.get("data_inizio")
        if is_valid_date(d) and datetime.strptime(d,"%Y-%m-%d").date() < TODAY.date():
            continue
        dl = ev.get("deadline_candidature")
        if is_valid_date(dl) and datetime.strptime(dl,"%Y-%m-%d").date() < TODAY.date():
            continue
        if not ev.get("nome"):
            continue
        cleaned.append(ev)

    console.print(f"  Dopo filtro date: {len(cleaned)}")
    cleaned = smart_dedup(cleaned)
    console.print(f"  Dopo dedup: {len(cleaned)}")

    for ev in cleaned:
        dist = get_city_distance(ev.get("luogo") or "")
        ev["distanza_km"] = round(dist) if dist else None
        ev["costo_trasporto_stimato"] = estimate_transport(dist)

    cache["events"] = cleaned
    console.print(f"[bold green]Fase 3: {len(cleaned)} eventi[/bold green]")
    ckpt_save(CKPT_EXTRACTED, cleaned, f"{len(cleaned)} eventi normalizzati")
    return cleaned

# ═══════════════════════════════════════════════════════════════════════════════
# FASE 4: FILTRI + VERIFICA LINK PARALLELA
# ═══════════════════════════════════════════════════════════════════════════════

ITALY_KEYWORDS = {"italia","italy","milan","roma","rome","torin","bologn","firenz","florence",
    "napol","naples","venez","venice","padov","padua","genov","veron","triest","trent","bolzan",
    "bari","catan","palerm","cagliar","perug","ancon","trevis","vicen","udin","bresc","bergam",
    "pisa","lecce","moden","parma","ferrar","rimini","como","pavia","raven","siena"}

def apply_filters(events, force=False):
    cached = ckpt_load(CKPT_FILTERED)
    if cached is not None and not force:
        console.print(Panel.fit(
            f"[bold]FASE 4: Filtri[/bold] [green]↩ da checkpoint[/green] ({len(cached)} eventi)",
            border_style="red"))
        return cached

    console.print(Panel.fit("[bold]FASE 4: Filtri + verifica link[/bold]", border_style="red"))
    before = len(events)

    console.print(f"  Verifico {sum(1 for e in events if e.get('url'))} link...")
    events = verify_links_parallel(events)

    filtered = []
    for ev in events:
        nome = (ev.get("nome") or "").lower()
        dist = ev.get("distanza_km")
        cost = ev.get("costo_trasporto_stimato")
        luogo = (ev.get("luogo") or "").lower()
        online = ev.get("online") or False
        tipo = (ev.get("tipo") or "").lower()        # fix: None → ""
        org = (ev.get("organizzatore") or "").lower()

        if online: continue

        dl = ev.get("deadline_candidature")
        if is_valid_date(dl):
            if datetime.strptime(dl, "%Y-%m-%d").date() < TODAY.date():
                continue

        in_italy = any(x in luogo for x in ITALY_KEYWORDS)
        if dist is not None:
            if dist > MAX_DISTANCE_KM and (cost is None or cost > MAX_TRANSPORT_COST) and not in_italy:
                console.print(f"  [dim]✗ Lontano ({dist}km): {ev.get('nome','')}[/dim]"); continue

        is_conference = "conference" in tipo or "conferenza" in tipo or "summit" in tipo
        is_gdg = "gdg" in nome or "gdg" in org or "google developer" in org or "devfest" in nome
        if is_conference and not is_gdg:
            if dist is None or dist > CONF_MAX_DIST_KM:
                console.print(f"  [dim]✗ Conferenza lontana: {ev.get('nome','')}[/dim]"); continue

        filtered.append(ev)

    for ev in filtered:
        dist = ev.get("distanza_km")
        score = 50
        if dist is not None:
            if dist < 50: score = 95
            elif dist < 100: score = 85
            elif dist < 200: score = 75
            elif dist < 500: score = 60
            elif dist < 800: score = 45
            else: score = 30
        ev["priorita"] = score
        ev["priorita_label"] = "🟢 Alta" if score>=70 else ("🟡 Media" if score>=50 else "🔴 Bassa")

    filtered.sort(key=lambda e: -e.get("priorita",0))
    console.print(f"\n  {before} → {len(filtered)} eventi ({before-len(filtered)} scartati)")
    ckpt_save(CKPT_FILTERED, filtered, f"{len(filtered)} eventi finali")
    return filtered

# ═══════════════════════════════════════════════════════════════════════════════
# GOOGLE CALENDAR
# ═══════════════════════════════════════════════════════════════════════════════

def get_gcal_service():
    creds = None
    if TOKEN_FILE.exists(): creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token: creds.refresh(GRequest())
        else:
            if not CREDS_FILE.exists():
                console.print(Panel("[red]credentials.json non trovato![/red]",title="Setup")); sys.exit(1)
            creds = InstalledAppFlow.from_client_secrets_file(str(CREDS_FILE),SCOPES).run_local_server(port=0)
        TOKEN_FILE.write_text(creds.to_json())
    return gbuild("calendar","v3",credentials=creds)

def check_dup(svc, summary, date):
    try:
        tmin=f"{date}T00:00:00Z"
        tmax=f"{(datetime.strptime(date,'%Y-%m-%d')+timedelta(days=1)).strftime('%Y-%m-%d')}T00:00:00Z"
        for e in svc.events().list(calendarId=CALENDAR_ID,timeMin=tmin,timeMax=tmax,q=summary[:50],maxResults=5).execute().get("items",[]):
            if e.get("summary","").strip()==summary.strip(): return True
    except: pass
    return False

def pri_color(p):
    if p>=70: return "2"    # verde
    if p>=50: return "5"    # giallo
    return "11"             # rosso

def build_gcal_event(h):
    nome=h.get("nome","Hackathon"); luogo=h.get("luogo") or ""; url=h.get("url") or ""
    d_ini=h.get("data_inizio"); d_fin=h.get("data_fine") or d_ini
    deadline=h.get("deadline_candidature"); apertura=h.get("apertura_iscrizioni")
    pri=h.get("priorita",50)

    desc = []
    if h.get("tipo"): desc.append(f"📋 {h['tipo']}")
    if h.get("tema"): desc.append(f"🎯 {h['tema']}")
    if luogo: desc.append(f"📍 {luogo}")
    if h.get("distanza_km"): desc.append(f"📏 {h['distanza_km']}km da Padova (~{h.get('costo_trasporto_stimato','?')}€)")
    if d_ini: desc.append(f"📆 {d_ini} → {d_fin or d_ini}")
    if h.get("duration_hours"): desc.append(f"⏱ ~{h['duration_hours']}h")
    if apertura: desc.append(f"🟢 Apertura: {apertura}")
    if deadline: desc.append(f"⏰ Deadline: {deadline}")
    if h.get("costo"): desc.append(f"💰 {h['costo']}")
    if h.get("organizzatore"): desc.append(f"🏢 {h['organizzatore']}")
    if url: desc.append(f"🔗 {url}")
    if not h.get("link_verificato") and url: desc.append("⚠️ Link da verificare")
    desc.append(f"\n[hackathon_to_gcal v5.1]")

    ap_ev = dl_ev = None
    if is_valid_date(apertura):
        ap_end=(datetime.strptime(apertura,"%Y-%m-%d")+timedelta(days=1)).strftime("%Y-%m-%d")
        ap_ev={"summary":f"🟢 ISCRIZIONI: {nome}","start":{"date":apertura},"end":{"date":ap_end},
               "colorId":"10","description":f"Registrazioni: {nome}\n📍{luogo}\n🔗{url}",
               "reminders":{"useDefault":False,"overrides":[{"method":"popup","minutes":60}]}}
    if is_valid_date(deadline):
        dl_end=(datetime.strptime(deadline,"%Y-%m-%d")+timedelta(days=1)).strftime("%Y-%m-%d")
        dl_ev={"summary":f"📅 DEADLINE: {nome}","start":{"date":deadline},"end":{"date":dl_end},
               "colorId":"11","description":f"Scadenza: {nome}\n📍{luogo}\n📆{d_ini or 'TBD'}→{d_fin or 'TBD'}\n🔗{url}",
               "reminders":{"useDefault":False,"overrides":[
                   {"method":"email","minutes":60*24*7},{"method":"email","minutes":60*24*3},{"method":"popup","minutes":60*24}]}}

    if is_valid_date(d_ini):
        end_dt=datetime.strptime(d_fin or d_ini,"%Y-%m-%d")+timedelta(days=1)
        event={"summary":f"🏆 {nome}","location":luogo,"description":"\n".join(desc),
               "start":{"date":d_ini},"end":{"date":end_dt.strftime("%Y-%m-%d")},
               "colorId":pri_color(pri),"reminders":{"useDefault":False,"overrides":[
                   {"method":"email","minutes":60*24*14},{"method":"popup","minutes":60*24*3}]}}
    else:
        nm=(TODAY.replace(day=1)+timedelta(days=32)).replace(day=1)
        event={"summary":f"❓ {nome} (TBD)","location":luogo,"description":"\n".join(desc)+"\n\n⚠️ Data TBD",
               "start":{"date":nm.strftime("%Y-%m-%d")},"end":{"date":(nm+timedelta(days=1)).strftime("%Y-%m-%d")},"colorId":"8"}
    if dl_ev: event["_dl"]=dl_ev
    if ap_ev: event["_ap"]=ap_ev
    return event

def add_to_cal(svc, events, dry_run=False):
    added=skipped=0
    for h in events:
        ev=build_gcal_event(h); dl=ev.pop("_dl",None); ap=ev.pop("_ap",None)
        pri=h.get("priorita_label","")
        if dry_run:
            rprint(f"\n  [DRY-RUN] {pri} {ev['summary']}")
            rprint(f"            📆 {ev['start']['date']} → {ev['end']['date']}")
            if h.get("luogo"): rprint(f"            📍 {h['luogo']} ({h.get('distanza_km','?')}km ~{h.get('costo_trasporto_stimato','?')}€)")
            if h.get("url"): rprint(f"            🔗 {h['url']} {'✓' if h.get('link_verificato') else '⛔?'}")
            if ap: rprint(f"            🟢 Apertura: {ap['start']['date']}")
            if dl: rprint(f"            📅 Deadline: {dl['start']['date']}")
            added+=1; continue
        if check_dup(svc,ev["summary"],ev["start"]["date"]):
            console.print(f"  [dim]⏭ {ev['summary']}[/dim]"); skipped+=1; continue
        try:
            r=svc.events().insert(calendarId=CALENDAR_ID,body=ev).execute()
            console.print(f"  [green]✓[/green] {ev['summary']} → {r.get('htmlLink')}"); added+=1
            for sub,lbl,clr in [(ap,"Apertura","green"),(dl,"Deadline","cyan")]:
                if sub:
                    try: svc.events().insert(calendarId=CALENDAR_ID,body=sub).execute(); console.print(f"    [{clr}]+[/{clr}] {lbl}: {sub['start']['date']}")
                    except Exception as e: console.print(f"    [yellow]{lbl}: {e}[/yellow]")
        except Exception as e: console.print(f"  [red]Err: {e}[/red]")
    if skipped: console.print(f"\n[dim]⏭ {skipped} dup[/dim]")
    return added

# ═══════════════════════════════════════════════════════════════════════════════
# OUTPUT
# ═══════════════════════════════════════════════════════════════════════════════

def print_summary(events):
    t = Table(title="Eventi (per priorità distanza)", show_header=True, header_style="bold cyan")
    t.add_column("#",style="dim",width=3); t.add_column("Pri",width=3)
    t.add_column("Nome",style="white",max_width=28); t.add_column("Tipo",max_width=16)
    t.add_column("Luogo",max_width=16); t.add_column("Dist",justify="right",width=6)
    t.add_column("€",justify="right",width=5); t.add_column("Inizio",justify="center")
    t.add_column("Deadline",justify="center",style="red"); t.add_column("🔗",width=3)
    for i,h in enumerate(events,1):
        nome=h.get("nome","?")[:28]
        dl=h.get("deadline_candidature")
        if is_valid_date(dl) and datetime.strptime(dl,"%Y-%m-%d")-TODAY<timedelta(days=14): nome=f"⚠️{nome}"
        pri=h.get("priorita",0)
        icon="🟢" if pri>=70 else "🟡" if pri>=50 else "🔴"
        dist=f"{h['distanza_km']}km" if h.get("distanza_km") else "?"
        cost=f"{h['costo_trasporto_stimato']}€" if h.get("costo_trasporto_stimato") else "?"
        link="✓" if h.get("link_verificato") else ("⛔" if h.get("link_verificato") is False else "?")
        t.add_row(str(i),icon,nome,(h.get("tipo") or "—")[:16],(h.get("luogo") or "?")[:16],
                  dist,cost,h.get("data_inizio") or "[dim]TBD[/dim]",
                  h.get("deadline_candidature") or "[dim]—[/dim]",link)
    console.print(t)

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    global OLLAMA_MODEL, PARALLEL_WORKERS, OLLAMA_WORKERS
    p = argparse.ArgumentParser(description="Hackathon Finder v5.1 (Parallel)")
    p.add_argument("--dry-run",action="store_true")
    p.add_argument("--model",default=OLLAMA_MODEL)
    p.add_argument("--max-searches",type=int,default=MAX_SEARCHES_DEFAULT)
    p.add_argument("--search-engine",choices=["duckduckgo","google","searxng"],default="duckduckgo")
    p.add_argument("--searxng",type=str,default=None)
    p.add_argument("--no-cache",action="store_true")
    p.add_argument("--paste",action="store_true")
    p.add_argument("--workers",type=int,default=PARALLEL_WORKERS,help="Thread per I/O")
    p.add_argument("--ollama-workers",type=int,default=OLLAMA_WORKERS,help="Thread per Ollama (max 2-3)")
    p.add_argument("--reset",action="store_true",
                   help="Cancella tutti i checkpoint e riparte da zero")
    p.add_argument("--reset-from",choices=["1","2","3","4"],default=None,
                   help="Riparte dalla fase indicata (1=scraping 2=ricerca 3=estrazione 4=filtri)")
    a = p.parse_args()
    OLLAMA_MODEL = a.model
    PARALLEL_WORKERS = a.workers; OLLAMA_WORKERS = a.ollama_workers

    # Gestione reset
    if a.reset:
        console.print("[yellow]⚠ Reset completo checkpoint...[/yellow]")
        ckpt_clear()
    elif a.reset_from:
        phase = int(a.reset_from)
        all_ckpts = [CKPT_SCRAPER, CKPT_PAGES, CKPT_EXTRACTED, CKPT_FILTERED]
        ckpt_clear([all_ckpts[i] for i in range(phase-1, 4)])
        console.print(f"[yellow]⚠ Reset dalla fase {phase} in poi[/yellow]")

    # Stato checkpoint
    stati = []
    for lbl, path in [("F1",CKPT_SCRAPER),("F2",CKPT_PAGES),("F3",CKPT_EXTRACTED),("F4",CKPT_FILTERED)]:
        stati.append(f"[green]✓{lbl}[/green]" if path.exists() else f"[dim]✗{lbl}[/dim]")

    console.print(Panel.fit(
        f"[bold]🔍 Hackathon Finder v5.1 (Parallel)[/bold]\n"
        f"📍 Padova | {MAX_DISTANCE_KM}km max | {MAX_TRANSPORT_COST}€ max trasporto\n"
        f"🤖 {OLLAMA_MODEL} | 🔍 {a.search_engine} | {a.max_searches} ricerche\n"
        f"⚡ {PARALLEL_WORKERS} I/O threads | {OLLAMA_WORKERS} Ollama threads | Dry-run: {a.dry_run}\n"
        f"Checkpoint: {'  '.join(stati)}",
        border_style="green"))

    cache = load_cache() if not a.no_cache else {"pages":{},"events":[],"timestamp":""}

    if a.paste:
        console.print(Panel("[yellow]Incolla testo, poi END.[/yellow]"))
        lines=[]
        while True:
            try:
                l=input()
                if l.strip().upper()=="END": break
                lines.append(l)
            except EOFError: break
        events = extract_single("","\n".join(lines))
    else:
        raw_scraper = run_scrapers_parallel()
        backend = get_backend(a.search_engine, a.searxng)
        agentic_pages = run_agentic_search(backend, a.max_searches, cache)
        events = run_extraction(raw_scraper, agentic_pages, cache)

    events = apply_filters(events)
    save_cache(cache)

    if not events:
        console.print("[yellow]Nessun evento supera i filtri.[/yellow]"); sys.exit(0)

    console.print(f"\n[bold green]═══ {len(events)} EVENTI FINALI ═══[/bold green]")
    print_summary(events)

    console.print(f"\n[dim]🟢{sum(1 for e in events if e.get('priorita',0)>=70)} "
                  f"🟡{sum(1 for e in events if 50<=e.get('priorita',0)<70)} "
                  f"🔴{sum(1 for e in events if e.get('priorita',0)<50)}[/dim]")

    OUTPUT_FILE.write_text(json.dumps(events, indent=2, ensure_ascii=False), encoding="utf-8")
    console.print(f"[dim]📁 {OUTPUT_FILE}[/dim]")

    if not a.dry_run:
        confirm=input(f"\nAggiungi {len(events)} eventi a GCal? [y/N] ")
        if confirm.strip().lower()!="y": console.print("[yellow]Annullato.[/yellow]"); sys.exit(0)

    svc=get_gcal_service()
    added=add_to_cal(svc,events,dry_run=a.dry_run)
    console.print(Panel.fit(f"[bold green]✓ {added} eventi {'simulati' if a.dry_run else 'aggiunti'}[/bold green]",border_style="green"))

if __name__=="__main__":
    main()