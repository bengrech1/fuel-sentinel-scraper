#!/usr/bin/env python3
"""
Fuel Sentinel — Fair Fuel Open Data API Monitor
=================================================
Uses the Victorian Government Fair Fuel Open Data API to detect:
1. Stations where isAvailable = false (direct unavailability signal)
2. Stations that have gone dark on price reporting (48hrs+ = likely out)

Base URL: https://api.fuel.service.vic.gov.au/open-data/v1
API Key header: x-consumer-id
Also requires: x-transactionid (unique UUID per request)

Runs every 6 hours on Render alongside scraper.py.
Add SERVO_SAVER_KEY to Render environment variables.
"""

import urllib.request
import json
import ssl
import os
import uuid
import time
from datetime import datetime, timezone, timedelta

# CONFIG
SERVO_SAVER_KEY   = os.environ.get("SERVO_SAVER_KEY",   "9f0cc4c5f348abc5a9ce00d9cf72af91")
SUPABASE_URL      = os.environ.get("SUPABASE_URL",      "https://sqnhggdhgvjdgjvhyjej.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNxbmhnZ2RoZ3ZqZGdqdmh5amVqIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ2MzMwNDAsImV4cCI6MjA5MDIwOTA0MH0.pa3W1tk8SF6lKvAOQ9hxlnX2yiIgZDKUx8nxU9xc4mE")

BASE_URL         = "https://api.fuel.service.vic.gov.au/open-data/v1"
STALE_HOURS      = 48
RUN_INTERVAL_HRS = 6

ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

SB_HEADERS = {
    "apikey":        SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=minimal",
}

FUEL_TYPE_NAMES = {
    "U91":"Unleaded 91","P95":"Premium 95","P98":"Premium 98",
    "DSL":"Diesel","PDSL":"Premium Diesel","E10":"E10",
    "E85":"E85","B20":"Biodiesel","LPG":"LPG","LNG":"LNG","CNG":"CNG",
}

def log(msg):
    print(f"[FairFuel {datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def api_request(endpoint):
    url = f"{BASE_URL}{endpoint}"
    req = urllib.request.Request(url)
    req.add_header("x-consumer-id",   SERVO_SAVER_KEY)
    req.add_header("x-transactionid", str(uuid.uuid4()))
    req.add_header("User-Agent",      "FuelSentinel/1.0 (fuelsentinel.com.au)")
    req.add_header("Accept",          "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30, context=ssl_ctx) as r:
            return json.loads(r.read().decode()), r.status
    except urllib.error.HTTPError as e:
        log(f"  HTTP {e.code} on {endpoint}: {e.reason}")
        if e.code == 429:
            log("  Rate limited — waiting 65s...")
            time.sleep(65)
        return None, e.code
    except Exception as e:
        log(f"  Error: {e}")
        return None, 0

def load_our_stations():
    all_stations = []
    offset = 0
    while True:
        try:
            url = f"{SUPABASE_URL}/rest/v1/stations?select=id,name,suburb,brand,lat,lng,status&order=id&limit=1000&offset={offset}"
            req = urllib.request.Request(url)
            for k, v in SB_HEADERS.items():
                req.add_header(k, v)
            with urllib.request.urlopen(req, timeout=30, context=ssl_ctx) as r:
                batch = json.loads(r.read().decode())
            if not batch: break
            all_stations.extend(batch)
            if len(batch) < 1000: break
            offset += 1000
        except Exception as e:
            log(f"  Station load error: {e}")
            break
    return all_stations

def apply_to_station(station_ids, status, source_desc, fuel_note=None, fuel_prices=None):
    """Directly update station status in Supabase."""
    now = datetime.now(timezone.utc).isoformat()
    for sid in station_ids:
        try:
            patch = {
                "status": status,
                "source": f"Fair Fuel API (auto)",
                "updated_at": now
            }
            if fuel_note:
                patch["fuel_notes"] = fuel_note
            if fuel_prices:
                patch["fuel_prices"] = json.dumps(fuel_prices)
            data = json.dumps(patch).encode()
            req = urllib.request.Request(
                f"{SUPABASE_URL}/rest/v1/stations?id=eq.{sid}",
                data=data, method="PATCH"
            )
            for k, v in SB_HEADERS.items():
                req.add_header(k, v)
            with urllib.request.urlopen(req, timeout=15, context=ssl_ctx) as r:
                pass
        except Exception as e:
            log(f"  Station update error: {e}")

def extract_fuel_note(unavail_names, avail_names):
    """Build a fuel note string from unavailable types."""
    if not unavail_names:
        return None
    if not avail_names:
        return "All fuels unavailable"
    return f"No {', '.join(unavail_names)}"

def save_signal(suburb, status, confidence, source_desc, station_ids):
    try:
        body = {
            "suburb": suburb, "status": status, "confidence": confidence,
            "source_type": "fair_fuel_api", "source_desc": source_desc[:300],
            "station_ids": json.dumps(station_ids), "processed": True, "auto_applied": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        req = urllib.request.Request(
            f"{SUPABASE_URL}/rest/v1/signals",
            data=json.dumps(body).encode(), method="POST"
        )
        for k, v in SB_HEADERS.items():
            req.add_header(k, v)
        with urllib.request.urlopen(req, timeout=15, context=ssl_ctx) as r:
            return r.status < 300
    except Exception as e:
        log(f"  Signal save error: {e}")
        return False

def match_station(api_station, our_stations):
    lat  = api_station.get("location", {}).get("latitude")
    lng  = api_station.get("location", {}).get("longitude")
    name = (api_station.get("name") or "").lower()
    addr = (api_station.get("address") or "").lower()
    best = None
    best_score = 0
    for s in our_stations:
        score = 0
        if lat and lng and s.get("lat") and s.get("lng"):
            dlat = abs(float(lat) - float(s["lat"]))
            dlng = abs(float(lng) - float(s["lng"]))
            if dlat < 0.001 and dlng < 0.001:   score += 10
            elif dlat < 0.003 and dlng < 0.003: score += 6
            elif dlat < 0.008 and dlng < 0.008: score += 2
        our_sub = (s.get("suburb") or "").lower()
        if our_sub and our_sub in addr: score += 3
        our_brand = (s.get("brand") or "").lower()
        for brand in ["bp","shell","7-eleven","coles","united","puma","ampol","liberty","metro","mobil"]:
            if brand in name and brand in our_brand:
                score += 2
                break
        if score > best_score:
            best_score = score
            best = s
    return best if best_score >= 5 else None

def run_cycle(our_stations):
    log("Fetching Fair Fuel price data...")
    data, status = api_request("/fuel/prices")
    if not data or status != 200:
        log(f"  Failed (status {status})")
        return 0, 0, 0

    records = data.get("fuelPriceDetails", [])
    log(f"  {len(records)} station records received")

    now          = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(hours=STALE_HOURS)
    active_cutoff = now - timedelta(hours=26)

    unavail_count = stale_count = signals_saved = 0

    for record in records:
        station     = record.get("fuelStation", {})
        fuel_prices = record.get("fuelPrices", [])
        updated_at  = record.get("updatedAt")
        if not station or not fuel_prices: continue

        matched = match_station(station, our_stations)
        if not matched: continue

        station_name = station.get("name", matched["name"])
        addr_parts   = (station.get("address") or "").split(",")
        suburb = matched.get("suburb") or (addr_parts[-2].strip() if len(addr_parts) >= 2 else "")

        # SIGNAL 1: isAvailable field — direct government reported unavailability
        unavail_types = [fp["fuelType"] for fp in fuel_prices if fp.get("isAvailable") is False]
        avail_types   = [fp["fuelType"] for fp in fuel_prices if fp.get("isAvailable") is True]
        
        # Build structured price data for storage
        price_data = [
            {
                "type": fp["fuelType"],
                "price": fp.get("price"),
                "available": fp.get("isAvailable", True)
            }
            for fp in fuel_prices
            if fp.get("fuelType") in ["U91","P95","P98","DSL","PDSL","E10","E85","LPG","LNG","CNG"]
        ]

        if unavail_types:
            unavail_count += 1
            unavail_names = [FUEL_TYPE_NAMES.get(t, t) for t in unavail_types]
            avail_names   = [FUEL_TYPE_NAMES.get(t, t) for t in avail_types]

            if not avail_types:
                log(f"  OUT: {station_name} ({suburb}) — ALL fuels unavailable")
                apply_to_station([matched["id"]], "red",
                    f"Fair Fuel API: {station_name} — ALL fuels unavailable",
                    "All fuels unavailable", price_data)
                if save_signal(suburb.lower(), "red", 9,
                    f"Fair Fuel API: {station_name} — ALL fuel types marked unavailable. Match: {matched['name']}",
                    [matched["id"]]): signals_saved += 1
            else:
                log(f"  PARTIAL: {station_name} — no {', '.join(unavail_names)}")
                fuel_note = extract_fuel_note(unavail_names, avail_names)
                apply_to_station([matched["id"]], "yellow",
                    f"Fair Fuel API: {station_name} — partial unavailability",
                    fuel_note, price_data)
                if save_signal(suburb.lower(), "yellow", 8,
                    f"Fair Fuel API: {station_name} — {', '.join(unavail_names)} unavailable. {', '.join(avail_names)} available. Match: {matched['name']}",
                    [matched["id"]]): signals_saved += 1

        # SIGNAL 2: Gone dark on price reporting
        if updated_at:
            try:
                last_update = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
                age_hrs = (now - last_update).total_seconds() / 3600

                if last_update < stale_cutoff and not unavail_types:
                    stale_count += 1
                    log(f"  STALE: {station_name} — {age_hrs:.0f}hrs since last report")
                    apply_to_station([matched["id"]], "red",
                        f"Fair Fuel API: gone dark on price reporting")
                    if save_signal(suburb.lower(), "red", 7,
                        f"Fair Fuel API: {station_name} — no price update in {age_hrs:.0f}hrs (legally required every 24hrs). Match: {matched['name']}",
                        [matched["id"]]): signals_saved += 1

                elif last_update > active_cutoff and not unavail_types:
                    if matched.get("status") in ("red", "gray"):
                        log(f"  ACTIVE: {station_name} — all fuels available, reported {age_hrs:.1f}hrs ago")
                        apply_to_station([matched["id"]], "green",
                            f"Fair Fuel API: actively reporting, all fuels available",
                            None, price_data)
                        if save_signal(suburb.lower(), "green", 8,
                            f"Fair Fuel API: {station_name} — all fuels available, reported {age_hrs:.1f}hrs ago. Match: {matched['name']}",
                            [matched["id"]]): signals_saved += 1
            except: pass

    # Update prices for ALL matched stations
    update_all_prices(records, our_stations)
    return unavail_count, stale_count, signals_saved

def update_all_prices(records, our_stations):
    """Update fuel_prices for every matched station regardless of status."""
    updated = 0
    now = datetime.now(timezone.utc).isoformat()
    for record in records:
        station    = record.get("fuelStation", {})
        fuel_prices = record.get("fuelPrices", [])
        if not station or not fuel_prices: continue

        matched = match_station(station, our_stations)
        if not matched: continue

        price_data = [
            {
                "type": fp["fuelType"],
                "price": fp.get("price"),
                "available": fp.get("isAvailable", True)
            }
            for fp in fuel_prices
            if fp.get("fuelType") in ["U91","P95","P98","DSL","PDSL","E10","E85","LPG","LNG","CNG"]
        ]
        if not price_data: continue

        try:
            data = json.dumps({"fuel_prices": json.dumps(price_data), "updated_at": now}).encode()
            req = urllib.request.Request(
                f"{SUPABASE_URL}/rest/v1/stations?id=eq.{matched['id']}",
                data=json.dumps({"fuel_prices": json.dumps(price_data)}).encode(),
                method="PATCH"
            )
            for k, v in SB_HEADERS.items():
                req.add_header(k, v)
            with urllib.request.urlopen(req, timeout=15, context=ssl_ctx) as r:
                if r.status < 300:
                    updated += 1
        except Exception as e:
            pass

    log(f"  Updated fuel prices for {updated} stations")

def main():
    log("=" * 60)
    log("  Fuel Sentinel — Fair Fuel Open Data API Monitor")
    log(f"  Endpoint: {BASE_URL}/fuel/prices")
    log(f"  Stale threshold: {STALE_HOURS}hrs | Interval: {RUN_INTERVAL_HRS}hrs")
    log("=" * 60)

    cycle = 0
    while True:
        cycle += 1
        log(f"\n=== Cycle #{cycle} — {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")
        try:
            our_stations = load_our_stations()
            log(f"Our stations: {len(our_stations)}")
            if our_stations:
                unavail, stale, saved = run_cycle(our_stations)
                log(f"\nResults: {unavail} unavailable | {stale} stale | {saved} signals saved")
                log("Check admin → Scraper Signals tab to review")
        except Exception as e:
            log(f"ERROR: {e}")
            import traceback; traceback.print_exc()

        log(f"\nSleeping {RUN_INTERVAL_HRS}hrs...")
        time.sleep(RUN_INTERVAL_HRS * 3600)

if __name__ == "__main__":
    main()
