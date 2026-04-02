"""
Fuel Sentinel — Scraper v3
===========================
Sources: Google News RSS + ABC News + Reddit (r/melbourne, r/victoria, r/australia)
Logic:   Signals buffered — 2+ sources needed to auto-update map.
         Single signals queued for admin review.
         All statuses expire to gray after 4 hours.
Deploy:  Render.com worker — runs every 30 minutes.
"""

import urllib.request
import json
import ssl
import time
import re
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

# ── CONFIG ───────────────────────────────────────────────────
SUPABASE_URL      = os.environ.get("SUPABASE_URL",      "https://sqnhggdhgvjdgjvhyjej.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNxbmhnZ2RoZ3ZqZGdqdmh5amVqIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ2MzMwNDAsImV4cCI6MjA5MDIwOTA0MH0.pa3W1tk8SF6lKvAOQ9hxlnX2yiIgZDKUx8nxU9xc4mE")
MIN_SIGNALS       = 2
SIGNAL_EXPIRY_HRS = 4
STATUS_EXPIRY_HRS = 25
RUN_INTERVAL_MINS = 30
# ─────────────────────────────────────────────────────────────

ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

HEADERS = {
    "apikey":        SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=minimal",
}

SIGNAL_PATTERNS = [
    (r"\bno fuel\b",                              "red",    10),
    (r"\bout of (fuel|petrol|gas)\b",             "red",    10),
    (r"\bfuel (has )?run out\b",                  "red",    10),
    (r"\bpetrol (has )?run out\b",                "red",    10),
    (r"\bempty (bowsers?|pumps?)\b",              "red",    10),
    (r"\bno (unleaded|diesel|e10|91|95|98)\b",    "red",     9),
    (r"\bsold out.{0,20}fuel\b",                  "red",     9),
    (r"\bfuel.{0,20}sold out\b",                  "red",     9),
    (r"\bfuel drought\b",                         "red",     8),
    (r"\bdried up\b",                             "red",     8),
    (r"\bcan.t get (fuel|petrol)\b",              "red",     9),
    (r"\bnowhere to get (fuel|petrol)\b",          "red",     9),
    (r"\bfuel (shortage|crisis)\b",               "yellow",  9),
    (r"\bpetrol (shortage|crisis)\b",             "yellow",  9),
    (r"\brationing\b",                            "yellow",  9),
    (r"\bfuel limit\b",                           "yellow",  9),
    (r"\b\$\d+ (limit|cap)\b",                   "yellow",  8),
    (r"\blong (queue|line)[s]?\b",                "yellow",  7),
    (r"\bpanic buy(ing)?\b",                      "yellow",  8),
    (r"\bfuel running (low|out)\b",               "yellow",  8),
    (r"\bpetrol running (low|out)\b",             "yellow",  8),
    (r"\blow (on )?fuel\b",                       "yellow",  6),
    (r"\bfuel supply (issue|problem|disruption)\b","yellow", 8),
    (r"\bhour[s]? (wait|queue)\b",               "yellow",  7),
    (r"\bwaiting.{0,30}fuel\b",                  "yellow",  7),
    (r"\bqueue.{0,30}petrol\b",                  "yellow",  7),
    (r"\bfuel (back|available|arrived)\b",        "green",   8),
    (r"\bresuppl(y|ied|ying)\b",                  "green",   8),
    (r"\bfuel truck[s]? arriv\b",                 "green",   9),
    (r"\bback in stock\b",                        "green",   8),
    (r"\bplenty of (fuel|petrol)\b",              "green",   8),
    (r"\bsupply restored\b",                      "green",   9),
    (r"\bfuel delivery\b",                        "green",   7),
    (r"\bgot fuel\b",                             "green",   8),
    (r"\bfound fuel\b",                           "green",   8),
    (r"\bfuel available\b",                       "green",   9),
]

VIC_SUBURBS = [
    "melbourne","cbd","fitzroy","richmond","collingwood","brunswick","footscray",
    "sunshine","werribee","geelong","ballarat","bendigo","shepparton","wodonga",
    "wangaratta","traralgon","morwell","moe","sale","bairnsdale","warrnambool",
    "hamilton","horsham","mildura","swan hill","echuca","benalla","ararat",
    "stawell","portland","colac","frankston","dandenong","cranbourne","pakenham",
    "berwick","ringwood","lilydale","healesville","yarra glen","belgrave",
    "ferntree gully","box hill","camberwell","hawthorn","south yarra","st kilda",
    "prahran","northcote","heidelberg","epping","craigieburn","sunbury","melton",
    "bacchus marsh","point cook","hoppers crossing","tarneit","mornington",
    "rosebud","rye","dromana","chelsea","mentone","moorabbin","cheltenham",
    "bentleigh","glen waverley","clayton","oakleigh","springvale","noble park",
    "narre warren","hallam","officer","drouin","warragul","lakes entrance",
    "bright","myrtleford","yarrawonga","cobram","kerang","nhill","dimboola",
    "st arnaud","macedon","woodend","kyneton","gisborne","romsey","whittlesea",
    "doreen","mernda","wallan","kilmore","seymour","yea","mansfield","wyndham",
    "clyde","lang lang","leongatha","korumburra","wonthaggi","orbost","omeo",
    "corryong","rutherglen","chiltern","ouyen","donald","charlton","camperdown",
    "cobden","terang","port fairy","heywood","casterton","coleraine","dunkeld",
    "halls gap","natimuk","apollo bay","lorne","anglesea","torquay","ocean grove",
    "barwon heads","leopold","lara","corio","latrobe valley","gippsland",


    # Note: "victoria" and "victorian" removed — handled as state-wide signals separately
]

NEWS_FEEDS = [
    "https://news.google.com/rss/search?q=fuel+shortage+victoria&hl=en-AU&gl=AU&ceid=AU:en",
    "https://news.google.com/rss/search?q=petrol+shortage+victoria&hl=en-AU&gl=AU&ceid=AU:en",
    "https://news.google.com/rss/search?q=fuel+victoria+empty+OR+crisis+OR+rationing&hl=en-AU&gl=AU&ceid=AU:en",
    "https://news.google.com/rss/search?q=no+fuel+victoria+petrol+station&hl=en-AU&gl=AU&ceid=AU:en",
    "https://news.google.com/rss/search?q=fuel+supply+disruption+victoria&hl=en-AU&gl=AU&ceid=AU:en",
    "https://www.abc.net.au/news/feed/2942460/rss.xml",
]

REDDIT_FEEDS = [
    "https://www.reddit.com/r/melbourne/search.json?q=fuel+OR+petrol+shortage&sort=new&limit=25&restrict_sr=1",
    "https://www.reddit.com/r/victoria/search.json?q=fuel+OR+petrol+shortage&sort=new&limit=25&restrict_sr=1",
    "https://www.reddit.com/r/australia/search.json?q=fuel+shortage+victoria&sort=new&limit=25",
    "https://www.reddit.com/r/melbourne/new.json?limit=75",
    "https://www.reddit.com/r/victoria/new.json?limit=75",
]

seen_urls = set()


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch(url, timeout=25, reddit=False):
    try:
        req = urllib.request.Request(url)
        ua = "FuelSentinel:v3 (fuel monitoring)" if reddit else "FuelSentinel/3.0"
        req.add_header("User-Agent", ua)
        with urllib.request.urlopen(req, timeout=timeout, context=ssl_ctx) as r:
            return r.read().decode("utf-8", errors="ignore")
    except Exception as e:
        log(f"  Fetch error: {e}")
        return None


def sb_get(path):
    try:
        req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/{path}")
        for k, v in HEADERS.items():
            req.add_header(k, v)
        with urllib.request.urlopen(req, timeout=15, context=ssl_ctx) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        log(f"  DB GET error: {e}")
        return None


def sb_post(path, body):
    try:
        req = urllib.request.Request(
            f"{SUPABASE_URL}/rest/v1/{path}",
            data=json.dumps(body).encode(), method="POST")
        for k, v in HEADERS.items():
            req.add_header(k, v)
        with urllib.request.urlopen(req, timeout=15, context=ssl_ctx) as r:
            return r.status
    except Exception as e:
        log(f"  DB POST error: {e}")
        return None


def sb_patch(path, body):
    try:
        req = urllib.request.Request(
            f"{SUPABASE_URL}/rest/v1/{path}",
            data=json.dumps(body).encode(), method="PATCH")
        for k, v in HEADERS.items():
            req.add_header(k, v)
        with urllib.request.urlopen(req, timeout=15, context=ssl_ctx) as r:
            return r.status
    except Exception as e:
        log(f"  DB PATCH error: {e}")
        return None


def classify(text):
    tl = text.lower()
    best, score = None, 0
    for pat, status, conf in SIGNAL_PATTERNS:
        if re.search(pat, tl) and conf > score:
            best, score = status, conf
    return best, score


# Terms that are too generic to map to specific stations
GENERIC_TERMS = {
    "victoria", "victorian", "latrobe valley", "gippsland",
    "mornington peninsula", "yarra valley", "dandenong ranges",
    "macedon ranges", "sunraysia", "mallee", "wimmera",
    "grampians", "surf coast", "bellarine",
}

def find_suburbs(text):
    """
    Extract specific suburb mentions. Filters out generic regional terms
    that would match hundreds of stations unhelpfully.
    Returns (specific_suburbs, is_statewide).
    """
    tl = text.lower()
    found = []
    for s in VIC_SUBURBS:
        if s in GENERIC_TERMS:
            continue
        if re.search(r'\b' + re.escape(s) + r'\b', tl):
            found.append(s)
    # Check if it's a statewide/regional signal with no specific suburb
    is_statewide = len(found) == 0 and any(
        w in tl for w in ["victoria", "victorian", "across victoria",
                          "statewide", "state-wide", "across the state"]
    )
    return found, is_statewide


def is_vic_relevant(text):
    tl = text.lower()
    return any(w in tl for w in
               ["victoria", "victorian", "melbourne", "geelong", "vic "])


def load_stations():
    return sb_get("stations?select=id,name,suburb,status,updated_at&order=suburb") or []


def stations_for_suburb(suburb, stations):
    sl = suburb.lower().strip()
    results = []
    for s in stations:
        s_suburb = (s.get("suburb") or "").lower().strip()
        s_name   = (s.get("name")   or "").lower().strip()
        # Exact suburb match
        if sl == s_suburb:
            results.append(s)
        # Suburb is a word within the station suburb (e.g. "south yarra" in "yarra")
        elif sl in s_suburb and len(sl) > 4:
            results.append(s)
        # Station suburb is contained in search term (multi-word suburbs)
        elif s_suburb in sl and len(s_suburb) > 4:
            results.append(s)
    return results


def save_signal(suburb, status, conf, stype, desc, ids):
    sb_post("signals", {
        "suburb":      suburb,
        "status":      status,
        "confidence":  conf,
        "source_type": stype,
        "source_desc": desc[:300],
        "station_ids": json.dumps(ids),
        "processed":   False,
        "created_at":  datetime.now(timezone.utc).isoformat(),
    })


def parse_rss(xml):
    items = []
    try:
        root = ET.fromstring(xml)
        for item in root.iter("item"):
            t = item.findtext("title") or ""
            d = item.findtext("description") or ""
            u = item.findtext("link") or ""
            items.append((t, d, u))
    except Exception as e:
        log(f"  RSS parse error: {e}")
    return items


def parse_reddit(raw):
    items = []
    try:
        data = json.loads(raw)
        for post in data.get("data", {}).get("children", []):
            p = post.get("data", {})
            title = p.get("title", "")
            body  = p.get("selftext", "")
            url   = "https://reddit.com" + p.get("permalink", "")
            items.append((title, body, url))
    except Exception as e:
        log(f"  Reddit parse error: {e}")
    return items


def process_items(items, stations, stype):
    saved = 0
    for title, body, url in items:
        if url in seen_urls:
            continue
        seen_urls.add(url)
        full = f"{title} {body}"
        if not is_vic_relevant(full):
            continue
        status, conf = classify(full)
        if not status or conf < 6:
            continue
        suburbs, is_statewide = find_suburbs(full)

        if is_statewide and conf >= 9:
            # High-confidence statewide signal — save as "victoria" for admin review
            # Only auto-applies if another statewide signal confirms
            log(f"  [{stype.upper()}] STATEWIDE {status.upper()} conf={conf} (queued for review)")
            log(f"    {title[:95]}")
            save_signal("victoria (statewide)", status, conf, stype, title, [])
            saved += 1
            continue

        if not suburbs:
            continue

        log(f"  [{stype.upper()}] {status.upper()} conf={conf} suburbs={suburbs[:4]}")
        log(f"    {title[:95]}")
        for suburb in suburbs:
            ids = [s["id"] for s in stations_for_suburb(suburb, stations)]
            if ids:
                save_signal(suburb, status, conf, stype, title, ids)
                saved += 1
    return saved


def process_buffer(stations):
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=SIGNAL_EXPIRY_HRS)).isoformat()
    signals = sb_get(
        f"signals?processed=is.false&created_at=gte.{cutoff}&order=created_at.desc"
    ) or []
    if not signals:
        return

    log(f"Buffer: {len(signals)} pending signals")
    groups = {}
    for sig in signals:
        key = (sig.get("suburb", "").lower(), sig.get("status", ""))
        groups.setdefault(key, []).append(sig)

    for (suburb, status), sigs in groups.items():
        count = len(sigs)
        if count >= MIN_SIGNALS:
            log(f"  AUTO-UPDATE: {suburb} → {status.upper()} ({count} signals agree)")
            now = datetime.now(timezone.utc).isoformat()
            for s in stations_for_suburb(suburb, stations):
                sb_patch(
                    f"stations?id=eq.{s['id']}",
                    {"status": status,
                     "source": f"Auto ({count} sources confirmed)",
                     "updated_at": now}
                )
                log(f"    ✓ {s['name']} ({s['suburb']}) → {status}")
                s["status"] = status
            for sig in sigs:
                sb_patch(f"signals?id=eq.{sig['id']}",
                         {"processed": True, "auto_applied": True})
        else:
            log(f"  PENDING: {suburb} → {status.upper()} ({count}/{MIN_SIGNALS} — needs admin review)")


def expire_statuses(stations):
    now = datetime.now(timezone.utc)
    expired = 0
    for s in stations:
        if s.get("status") == "gray":
            continue
        # Never expire manually verified or Fair Fuel API stations
        source = (s.get("source") or "").lower()
        if source.startswith("manual") or source.startswith("fair fuel"):
            continue
        raw = s.get("updated_at")
        if not raw:
            continue
        try:
            updated = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if (now - updated).total_seconds() / 3600 > STATUS_EXPIRY_HRS:
                sb_patch(f"stations?id=eq.{s['id']}",
                         {"status": "gray", "source": "Auto-expired",
                          "updated_at": now.isoformat()})
                s["status"] = "gray"
                expired += 1
        except:
            pass
    if expired:
        log(f"Expired {expired} stale statuses → gray")


def run_cycle():
    log("=" * 50)
    log(f"Cycle — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log("=" * 50)

    stations = load_stations()
    if not stations:
        log("No stations — skipping")
        return
    log(f"Loaded {len(stations)} stations")

    expire_statuses(stations)

    total = 0

    log("--- News feeds ---")
    for url in NEWS_FEEDS:
        raw = fetch(url)
        if raw:
            total += process_items(parse_rss(raw), stations, "news")
        time.sleep(1)

    log("--- Reddit ---")
    for url in REDDIT_FEEDS:
        raw = fetch(url, reddit=True)
        if raw:
            total += process_items(parse_reddit(raw), stations, "reddit")
        time.sleep(2)

    log(f"Saved {total} new signals")
    process_buffer(stations)

    # Run FuelCheck monitor every other cycle (every 60 mins)
    global _cycle_count
    _cycle_count = getattr(globals(), '_cycle_count', 0) + 1
    if _cycle_count % 2 == 0:
        log("--- FuelCheck monitor ---")
        try:
            run_fuelcheck_cycle(stations)
        except Exception as e:
            log(f"  FuelCheck error: {e}")

    log("Cycle complete\n")



def run_fuelcheck_cycle(stations):
    """Check FuelCheck API for stations that have gone dark."""
    FUELCHECK_BASE = "https://fppdirectapi-prod.fuelcheck.vic.gov.au"
    FC_HEADERS = {
        "User-Agent": "Mozilla/5.0 FuelSentinel/1.0",
        "Accept": "application/json",
        "Origin": "https://www.fuelcheck.vic.gov.au",
        "Referer": "https://www.fuelcheck.vic.gov.au/",
        "Content-Type": "application/json",
    }

    # Get auth token
    token = None
    try:
        req = urllib.request.Request(f"{FUELCHECK_BASE}/Security/CreateAnonymousUser", data=b\'{}\', method="POST")
        for k,v in FC_HEADERS.items(): req.add_header(k,v)
        with urllib.request.urlopen(req, timeout=20, context=ssl_ctx) as r:
            token = json.loads(r.read().decode()).get("access_token")
    except Exception as e:
        log(f"  FuelCheck auth failed: {e}")
        return

    if not token:
        log("  No FuelCheck token")
        return

    # Get prices
    try:
        req = urllib.request.Request(f"{FUELCHECK_BASE}/Price/GetSitesPrices?countryId=21&geoRegionLevel=3&geoRegionId=1")
        for k,v in FC_HEADERS.items(): req.add_header(k,v)
        req.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(req, timeout=30, context=ssl_ctx) as r:
            data = json.loads(r.read().decode())
    except Exception as e:
        log(f"  FuelCheck price fetch failed: {e}")
        return

    sites = data.get("SitesPrices", [])
    log(f"  FuelCheck: {len(sites)} active sites")

    now = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(hours=6)
    signals_saved = 0

    for site in sites[:200]:  # Cap to avoid long loops
        prices = site.get("Prices", [])
        if not prices: continue

        latest = None
        for p in prices:
            t_str = p.get("TransactionDateUtc") or p.get("LastUpdateDate","")
            if t_str:
                try:
                    t = datetime.fromisoformat(t_str.replace("Z","+00:00").replace(" ","T"))
                    if not latest or t > latest: latest = t
                except: pass

        if not latest: continue

        suburb = (site.get("Suburb") or "").lower()
        age_hrs = (now - latest).total_seconds() / 3600

        matching = stations_for_suburb(suburb, stations)
        if not matching: continue

        ids = [s["id"] for s in matching]

        if latest < stale_cutoff:
            save_signal(suburb, "red", 7, "fuelcheck",
                f"Gone dark on FuelCheck for {age_hrs:.0f}hrs — possible fuel out", ids)
            signals_saved += 1
        elif age_hrs < 2:
            active_stations = [s for s in matching if s.get("status") in ("red","gray")]
            if active_stations:
                active_ids = [s["id"] for s in active_stations]
                save_signal(suburb, "green", 6, "fuelcheck",
                    f"Actively reporting prices on FuelCheck ({age_hrs:.1f}hrs ago)", active_ids)
                signals_saved += 1

    log(f"  FuelCheck signals saved: {signals_saved}")


def main():
    log("Fuel Sentinel Scraper v3")
    log(f"News feeds: {len(NEWS_FEEDS)} | Reddit feeds: {len(REDDIT_FEEDS)}")
    log(f"Auto-update threshold: {MIN_SIGNALS} signals | Interval: {RUN_INTERVAL_MINS} mins")
    log("")
    cycle = 0
    while True:
        cycle += 1
        log(f"=== CYCLE #{cycle} ===")
        try:
            run_cycle()
        except Exception as e:
            log(f"ERROR: {e}")
        log(f"Sleeping {RUN_INTERVAL_MINS} mins...\n")
        time.sleep(RUN_INTERVAL_MINS * 60)


if __name__ == "__main__":
    main()
