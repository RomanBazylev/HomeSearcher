#!/usr/bin/env python3
"""
Enrich raw Otodom listings with proximity scores using OpenStreetMap Overpass API.
Calculates distance to parks, schools, kindergartens, shopping, and transport.

Usage:
    python enrich_listings.py                # process all missing
    python enrich_listings.py --batch 10     # process 10 at a time
    python enrich_listings.py --force        # re-process all listings
"""

import json
import math
import os
import sys
import time
import urllib.request
import urllib.error

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
RAW_FILE = os.path.join(DATA_DIR, "listings_raw.json")
OUT_FILE = os.path.join(DATA_DIR, "listings.json")

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
DELAY = 2.0  # seconds between Overpass requests

# POI categories with search tags and radii
POI_CONFIG = {
    "parks": {
        "tags": [
            ('node["leisure"="park"]', 1000),
            ('way["leisure"="park"]', 1000),
            ('relation["leisure"="park"]', 1000),
        ],
        "weight": 1.0,
    },
    "schools": {
        "tags": [
            ('node["amenity"="school"]', 1000),
            ('way["amenity"="school"]', 1000),
        ],
        "weight": 1.0,
    },
    "kindergartens": {
        "tags": [
            ('node["amenity"="kindergarten"]', 1000),
            ('way["amenity"="kindergarten"]', 1000),
        ],
        "weight": 1.0,
    },
    "shopping": {
        "tags": [
            ('node["shop"="mall"]', 1500),
            ('way["shop"="mall"]', 1500),
            ('node["shop"="supermarket"]', 1000),
        ],
        "weight": 1.0,
    },
    "transport": {
        "tags": [
            ('node["highway"="bus_stop"]', 500),
            ('node["railway"="tram_stop"]', 800),
            ('node["railway"="station"]', 1500),
            ('node["station"="subway"]', 1500),
        ],
        "weight": 1.0,
    },
}


def haversine(lat1, lon1, lat2, lon2):
    """Distance in metres between two lat/lng points."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def build_overpass_query(lat, lng):
    """Build a combined Overpass query for all POI categories."""
    parts = []
    for category, cfg in POI_CONFIG.items():
        for tag_expr, radius in cfg["tags"]:
            parts.append(f'  {tag_expr}(around:{radius},{lat},{lng});')

    query = "[out:json][timeout:25];\n(\n"
    query += "\n".join(parts)
    query += "\n);\nout center;"
    return query


def query_overpass(lat, lng):
    """Query Overpass API and return raw elements."""
    query = build_overpass_query(lat, lng)
    data = urllib.parse.urlencode({"data": query}).encode("utf-8")

    req = urllib.request.Request(OVERPASS_URL, data=data, method="POST", headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "HomeSearcher-Enricher/1.0",
    })

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return result.get("elements", [])
    except urllib.error.HTTPError as e:
        if e.code == 429:
            return "ratelimit"
        print(f"    Overpass HTTP {e.code}")
        return []
    except Exception as e:
        print(f"    Overpass error: {e}")
        return []


def classify_element(el):
    """Classify an Overpass element into one of our categories."""
    tags = el.get("tags", {})

    # Get coordinates (node: lat/lon directly, way/relation: center)
    if el["type"] == "node":
        lat = el.get("lat")
        lng = el.get("lon")
    else:
        center = el.get("center", {})
        lat = center.get("lat")
        lng = center.get("lon")

    if lat is None or lng is None:
        return None, None, None

    # Classify
    if tags.get("leisure") == "park":
        return "parks", lat, lng
    if tags.get("amenity") == "school":
        return "schools", lat, lng
    if tags.get("amenity") == "kindergarten":
        return "kindergartens", lat, lng
    if tags.get("shop") in ("mall", "supermarket"):
        return "shopping", lat, lng
    if tags.get("highway") == "bus_stop":
        return "transport", lat, lng
    if tags.get("railway") in ("station", "tram_stop"):
        return "transport", lat, lng
    if tags.get("station") == "subway":
        return "transport", lat, lng

    return None, None, None


def compute_score(count, nearest_m, max_radius=1000):
    """
    Compute a 0-10 score based on POI count and nearest distance.
    More POIs + closer = higher score.
    """
    if count == 0:
        return 0

    # Distance factor: 10 at 0m, ~1 at max_radius
    dist_factor = max(0, 1 - (nearest_m / max_radius)) * 6

    # Count factor: caps at ~5 POIs = 4 points
    count_factor = min(4, count * 0.8)

    return round(min(10, dist_factor + count_factor))


def compute_proximity(listing_lat, listing_lng, elements):
    """Compute proximity scores from Overpass elements."""
    categories = {
        "parks": [],
        "schools": [],
        "kindergartens": [],
        "shopping": [],
        "transport": [],
    }

    for el in elements:
        cat, lat, lng = classify_element(el)
        if cat and cat in categories:
            dist = haversine(listing_lat, listing_lng, lat, lng)
            categories[cat].append(dist)

    proximity = {}
    total = 0
    for cat, distances in categories.items():
        distances.sort()
        count = len(distances)
        nearest = round(distances[0]) if distances else 0
        max_r = 1500 if cat == "shopping" else (500 if cat == "transport" else 1000)
        score = compute_score(count, nearest, max_r)
        proximity[cat] = {
            "count": count,
            "nearest_m": nearest,
            "score": score,
        }
        total += score

    proximity["total"] = total
    return proximity


def main_args(argv=None):
    """Entry point accepting argv list (for programmatic use)."""
    args = argv or sys.argv[1:]
    batch_size = None
    force = "--force" in args

    if "--batch" in args:
        idx = args.index("--batch")
        if idx + 1 < len(args):
            batch_size = int(args[idx + 1])

    # Load raw listings
    if not os.path.exists(RAW_FILE):
        print(f"Файл {RAW_FILE} не найден. Сначала запустите fetch_listings.py")
        sys.exit(1)

    with open(RAW_FILE, "r", encoding="utf-8") as f:
        listings = json.load(f)

    # Load existing enriched data to preserve previous results
    existing = {}
    if os.path.exists(OUT_FILE) and not force:
        with open(OUT_FILE, "r", encoding="utf-8") as f:
            for item in json.load(f):
                existing[item["id"]] = item

    # Merge: start from raw, overlay existing proximity data
    for listing in listings:
        if listing["id"] in existing and existing[listing["id"]].get("proximity"):
            listing["proximity"] = existing[listing["id"]]["proximity"]

    # Find listings needing enrichment
    if force:
        to_process = [l for l in listings if l.get("lat") and l.get("lng")]
    else:
        to_process = [
            l for l in listings
            if l.get("lat") and l.get("lng") and not l.get("proximity")
        ]

    if batch_size:
        to_process = to_process[:batch_size]

    total_to_process = len(to_process)
    total_listings = len(listings)
    already_done = sum(1 for l in listings if l.get("proximity"))

    print(f"Всего объявлений: {total_listings}")
    print(f"Уже обогащено: {already_done}")
    print(f"К обработке: {total_to_process}")

    if not to_process:
        print("Все объявления уже обогащены!")
        # Still save the merged result
        with open(OUT_FILE, "w", encoding="utf-8") as f:
            json.dump(listings, f, ensure_ascii=False, indent=2)
        print(f"Сохранено в {OUT_FILE}")
        return

    ok = 0
    fail = 0

    for i, listing in enumerate(to_process, 1):
        lat, lng = listing["lat"], listing["lng"]
        print(f"\n  [{i}/{total_to_process}] {listing['title'][:50]}")
        print(f"    {listing['district']} — {lat}, {lng}")

        elements = query_overpass(lat, lng)

        if elements == "ratelimit":
            print("  *** Rate limited — останавливаем. Запустите снова позже.")
            fail += 1
            break

        if not isinstance(elements, list):
            fail += 1
            continue

        proximity = compute_proximity(lat, lng, elements)
        listing["proximity"] = proximity

        parks = proximity["parks"]
        schools = proximity["schools"]
        kinder = proximity["kindergartens"]
        shop = proximity["shopping"]
        trans = proximity["transport"]

        print(f"    🌳 Парки: {parks['count']} ({parks['nearest_m']}м) = {parks['score']}/10")
        print(f"    🏫 Школы: {schools['count']} ({schools['nearest_m']}м) = {schools['score']}/10")
        print(f"    👶 Садики: {kinder['count']} ({kinder['nearest_m']}м) = {kinder['score']}/10")
        print(f"    🛒 Магазины: {shop['count']} ({shop['nearest_m']}м) = {shop['score']}/10")
        print(f"    🚌 Транспорт: {trans['count']} ({trans['nearest_m']}м) = {trans['score']}/10")
        print(f"    ✅ ИТОГО: {proximity['total']}/50")

        ok += 1

        if i < total_to_process:
            time.sleep(DELAY)

    print(f"\n{'='*60}")
    print(f"  Обработано: {ok} успешно, {fail} ошибок")
    print(f"{'='*60}")

    # Save
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(listings, f, ensure_ascii=False, indent=2)
    print(f"Сохранено в {OUT_FILE}")


if __name__ == "__main__":
    main_args()
