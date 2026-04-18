#!/usr/bin/env python3
"""
Fetch real estate listings from Otodom.pl and Gratka.pl for Warsaw.
Parses server-rendered HTML with BeautifulSoup.
Supports: apartments & houses, rent & sale.
Geocodes addresses to get lat/lng via Photon (komoot).

Usage:
    python fetch_listings.py                    # fetch all (sale focused)
    python fetch_listings.py --max-pages 3      # limit pages per category
    python fetch_listings.py --source otodom    # only Otodom
    python fetch_listings.py --source gratka    # only Gratka
    python fetch_listings.py --no-geocode       # skip geocoding step
"""

import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

try:
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("beautifulsoup4 not installed. Run: pip install -r requirements.txt")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
RAW_FILE = os.path.join(DATA_DIR, "listings_raw.json")

MAX_PAGES = 10
DELAY = 1.5       # seconds between page requests
GEO_DELAY = 1.0   # seconds between geocoding requests

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

# ─── Search configurations ───────────────────────────────────────────────
# Priority order: sale first (user's main interest), then rent
SEARCH_CONFIGS = [
    {"deal": "sale", "type": "apartment"},
    {"deal": "sale", "type": "house"},
    {"deal": "rent", "type": "apartment"},
    {"deal": "rent", "type": "house"},
]


# ═══════════════════════════════════════════════════════════════════════════
#  HTTP helpers
# ═══════════════════════════════════════════════════════════════════════════

def fetch_html(url):
    """Fetch a page and return HTML string or None."""
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.5",
        "Accept-Encoding": "identity",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        print(f"    HTTP {e.code} for {url}")
        return None
    except Exception as e:
        print(f"    Error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════
#  OTODOM SCRAPER  (uses __NEXT_DATA__ JSON embedded in HTML)
# ═══════════════════════════════════════════════════════════════════════════

OTODOM_BASE = "https://www.otodom.pl/pl/wyniki/{deal}/{prop_type}/mazowieckie/warszawa/warszawa/warszawa"

OTODOM_DEAL_MAP = {"sale": "sprzedaz", "rent": "wynajem"}
OTODOM_TYPE_MAP = {"apartment": "mieszkanie", "house": "dom"}


def otodom_build_url(deal, prop_type, page=1):
    url = OTODOM_BASE.format(
        deal=OTODOM_DEAL_MAP[deal],
        prop_type=OTODOM_TYPE_MAP[prop_type],
    )
    params = {"limit": 36, "page": page}
    return f"{url}?{urllib.parse.urlencode(params)}"


def otodom_parse_page(html, deal, prop_type):
    """Extract listings from __NEXT_DATA__ JSON in the Otodom page."""
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        print("    ⚠ __NEXT_DATA__ не найден")
        return []

    data = json.loads(script.string)
    items = (data.get("props", {}).get("pageProps", {})
             .get("data", {}).get("searchAds", {}).get("items", []))

    listings = []
    for item in items:
        slug = item.get("slug", "")
        listing_id = f"otodom-{item['id']}"
        url = f"https://www.otodom.pl/pl/oferta/{slug}"
        title = item.get("title", "")

        price_obj = item.get("totalPrice") or {}
        price = price_obj.get("value")

        area = item.get("areaInSquareMeters")
        rooms = item.get("roomsNumber")
        floor_val = item.get("floorNumber")
        floor = None
        if floor_val is not None:
            try:
                floor = int(floor_val)
            except (ValueError, TypeError):
                pass

        # Location
        loc = item.get("location") or {}
        addr_obj = loc.get("address") or {}
        street_obj = addr_obj.get("street") or {}
        address = ""
        if street_obj.get("name"):
            address = street_obj["name"]
            if street_obj.get("number"):
                address += " " + street_obj["number"]

        district = ""
        rev_locs = (loc.get("reverseGeocoding") or {}).get("locations") or []
        for rl in rev_locs:
            if rl.get("locationLevel") == "district":
                district = rl.get("name", "")
                break

        # Image (first available)
        image = ""
        images = item.get("images", [])
        if images:
            img = images[0]
            if isinstance(img, dict):
                image = img.get("large", img.get("medium", img.get("small", "")))
            elif isinstance(img, str):
                image = img

        if title:
            listings.append({
                "id": listing_id,
                "source": "otodom",
                "url": url,
                "title": title,
                "type": prop_type,
                "deal": deal,
                "price": price,
                "currency": "PLN",
                "area": round(area, 2) if area else None,
                "rooms": rooms,
                "floor": floor,
                "lat": None,
                "lng": None,
                "district": district,
                "address": address,
                "image": image,
                "fetched_at": NOW,
            })

    return listings


def otodom_has_next_page(html, current_page):
    """Check if there's a next page of results."""
    return f"page={current_page + 1}" in html


def fetch_otodom(deal, prop_type, max_pages):
    """Fetch listings from Otodom for a given category."""
    all_listings = []

    for page in range(1, max_pages + 1):
        url = otodom_build_url(deal, prop_type, page)
        print(f"  [Otodom] Стр.{page}: {url}")

        html = fetch_html(url)
        if not html:
            break

        listings = otodom_parse_page(html, deal, prop_type)
        if not listings:
            print(f"    Пустая страница, завершаем")
            break

        all_listings.extend(listings)
        print(f"    → {len(listings)} объявлений (итого: {len(all_listings)})")

        if not otodom_has_next_page(html, page):
            break

        time.sleep(DELAY)

    return all_listings


# ═══════════════════════════════════════════════════════════════════════════
#  GRATKA SCRAPER
# ═══════════════════════════════════════════════════════════════════════════

GRATKA_BASE = "https://gratka.pl/nieruchomosci/{prop_type}/warszawa/{deal}"
GRATKA_DEAL_MAP = {"sale": "sprzedaz", "rent": "wynajem"}
GRATKA_TYPE_MAP = {"apartment": "mieszkania", "house": "domy"}


def gratka_build_url(deal, prop_type, page=1):
    url = GRATKA_BASE.format(
        deal=GRATKA_DEAL_MAP[deal],
        prop_type=GRATKA_TYPE_MAP[prop_type],
    )
    if page > 1:
        return f"{url}?page={page}"
    return url


def gratka_parse_page(html, deal, prop_type):
    """Parse listings from a Gratka search results page."""
    soup = BeautifulSoup(html, "html.parser")
    listings = []

    # Gratka listing links use relative paths: /nieruchomosci/mieszkanie-*/ob/NNN
    links = soup.find_all("a", href=re.compile(
        r"/nieruchomosci/.+/o[bi]/\d+"
    ))

    seen_urls = set()
    for link in links:
        href = link.get("href", "")
        if not href.startswith("http"):
            href = "https://gratka.pl" + href

        if href in seen_urls:
            continue
        seen_urls.add(href)

        # Extract ID from URL: .../ob/12345678 or .../oi/12345678
        id_match = re.search(r"/o[bi]/(\d+)", href)
        listing_id = f"gratka-{id_match.group(1)}" if id_match else f"gratka-{abs(hash(href)) % 10**8}"

        # Card text from the link and surrounding context
        card = link
        for _ in range(8):
            parent = card.parent
            if parent is None:
                break
            card = parent
            card_text = card.get_text(" ", strip=True)
            if len(card_text) > 100 and "zł" in card_text:
                break

        card_text = card.get_text(" ", strip=True)

        # Title — first substantial text in the link
        title_parts = []
        for el in link.find_all(string=True, recursive=True):
            t = el.strip()
            if len(t) > 10 and "zdjęcie" not in t.lower() and "aparat" not in t.lower():
                title_parts.append(t)
        title = title_parts[0] if title_parts else link.get_text(strip=True)[:100]

        # Clean up title — remove image/badge prefixes
        title = re.sub(r"^(Aparat\s*\d*\s*|Plan\s*|Strzałka[^A-Z]*|Nowe ogłoszenie\s*|Polecana oferta\s*|Tylko u nas\s*|Oferta na wyłączność\s*|PROMOCJA[^A-Z]*|0%[^A-Z]*)", "", title).strip()
        title = re.sub(r"\s*Dodaj do ulubionych.*$", "", title).strip()

        # Price — look for "XXX XXX zł" pattern
        price = None
        price_matches = re.findall(r"([\d\s]+)\s*zł(?!/m)", card_text)
        if price_matches:
            for pm in price_matches:
                p = int(re.sub(r"\s", "", pm))
                if p > 100:  # filter out noise
                    price = p
                    break

        # Area
        area = None
        area_match = re.search(r"(\d+(?:[.,]\d+)?)\s*m²", card_text)
        if area_match:
            try:
                area = float(area_match.group(1).replace(",", "."))
            except ValueError:
                pass

        # Rooms
        rooms = None
        rooms_match = re.search(r"(\d+)\s*poko[ij]", card_text)
        if rooms_match:
            rooms = int(rooms_match.group(1))

        # Floor — "piętro X/Y" or just "parter"
        floor = None
        floor_match = re.search(r"piętro\s*(\d+)", card_text)
        if floor_match:
            floor = int(floor_match.group(1))
        elif "parter" in card_text.lower():
            floor = 0

        # Location from Gratka: "Street, District, City, mazowieckie"
        district = ""
        address = ""
        loc_match = re.search(
            r"Lokalizacja\s+(.+?),\s*Warszawa",
            card_text
        )
        if loc_match:
            loc_str = loc_match.group(1).strip()
            parts = [p.strip() for p in loc_str.split(",")]
            if len(parts) >= 2:
                address = parts[0]
                district = parts[-1]  # last part before "Warszawa"
            else:
                district = parts[0]

        if title and len(title) > 5 and (price or area):
            listings.append({
                "id": listing_id,
                "source": "gratka",
                "url": href,
                "title": title,
                "type": prop_type,
                "deal": deal,
                "price": price,
                "currency": "PLN",
                "area": area,
                "rooms": rooms,
                "floor": floor,
                "lat": None,
                "lng": None,
                "district": district,
                "address": address,
                "image": "",
                "fetched_at": NOW,
            })

    return listings


def gratka_get_total_pages(html):
    """Extract total page count from Gratka pagination."""
    match = re.search(r'page=(\d+)"[^>]*>\s*\d+\s*</a>\s*(?:<a[^>]*class="[^"]*next)', html)
    if not match:
        # Fallback: find highest page number in pagination links
        pages = re.findall(r"page=(\d+)", html)
        if pages:
            return max(int(p) for p in pages)
    return int(match.group(1)) if match else 1


def fetch_gratka(deal, prop_type, max_pages):
    """Fetch listings from Gratka for a given category."""
    all_listings = []

    for page in range(1, max_pages + 1):
        url = gratka_build_url(deal, prop_type, page)
        print(f"  [Gratka] Стр.{page}: {url}")

        html = fetch_html(url)
        if not html:
            break

        listings = gratka_parse_page(html, deal, prop_type)
        if not listings:
            print(f"    Пустая страница, завершаем")
            break

        all_listings.extend(listings)
        print(f"    → {len(listings)} объявлений (итого: {len(all_listings)})")

        # Check if there's a next page
        if f"page={page + 1}" not in html:
            break

        time.sleep(DELAY)

    return all_listings


# ═══════════════════════════════════════════════════════════════════════════
#  GEOCODING (Photon / komoot)
# ═══════════════════════════════════════════════════════════════════════════

def geocode(query):
    """Geocode an address via Photon API. Returns (lat, lng) or (None, None)."""
    params = urllib.parse.urlencode({
        "q": query, "limit": 1, "lang": "pl",
        "lat": 52.23, "lon": 21.01,  # bias toward Warsaw
    }, quote_via=urllib.parse.quote)
    url = f"https://photon.komoot.io/api/?{params}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "HomeSearcher-Geocoder/1.0",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            feats = data.get("features", [])
            if feats:
                c = feats[0]["geometry"]["coordinates"]
                return round(c[1], 6), round(c[0], 6)
    except urllib.error.HTTPError as e:
        if e.code == 429:
            return "ratelimit", "ratelimit"
    except Exception:
        pass
    return None, None


def geocode_listings(listings):
    """Add lat/lng to listings that don't have coordinates."""
    to_geocode = [l for l in listings if not l.get("lat")]
    if not to_geocode:
        print("  Все объявления уже имеют координаты")
        return

    print(f"\n  Геокодирование {len(to_geocode)} адресов...")
    ok = 0
    for i, l in enumerate(to_geocode, 1):
        # Build geocoding query from available address info
        parts = []
        if l.get("address"):
            parts.append(l["address"])
        if l.get("district"):
            parts.append(l["district"])
        parts.append("Warszawa")

        query = ", ".join(parts)
        lat, lng = geocode(query)

        if lat == "ratelimit":
            print(f"    Rate limit на {i}/{len(to_geocode)}, стоп")
            break

        if lat is not None:
            l["lat"] = lat
            l["lng"] = lng
            ok += 1
        else:
            # Fallback: geocode district only
            if l.get("district"):
                time.sleep(GEO_DELAY)
                lat2, lng2 = geocode(f"{l['district']}, Warszawa")
                if lat2 and lat2 != "ratelimit":
                    l["lat"] = lat2
                    l["lng"] = lng2
                    ok += 1

        if i % 20 == 0:
            print(f"    ...{i}/{len(to_geocode)} ({ok} успешно)")

        time.sleep(GEO_DELAY)

    print(f"  Геокодировано: {ok}/{len(to_geocode)}")


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════

def deduplicate(listings):
    """Remove duplicate listings by URL."""
    seen = set()
    unique = []
    for item in listings:
        key = item.get("url") or item.get("id")
        if key and key not in seen:
            seen.add(key)
            unique.append(item)
    return unique


def main():
    max_pages = MAX_PAGES
    source_filter = None
    skip_geocode = "--no-geocode" in sys.argv

    if "--max-pages" in sys.argv:
        idx = sys.argv.index("--max-pages")
        if idx + 1 < len(sys.argv):
            max_pages = int(sys.argv[idx + 1])

    if "--source" in sys.argv:
        idx = sys.argv.index("--source")
        if idx + 1 < len(sys.argv):
            source_filter = sys.argv[idx + 1].lower()

    os.makedirs(DATA_DIR, exist_ok=True)

    all_listings = []

    for config in SEARCH_CONFIGS:
        deal = config["deal"]
        prop_type = config["type"]

        deal_label = "продажа" if deal == "sale" else "аренда"
        type_label = "квартиры" if prop_type == "apartment" else "дома"

        print(f"\n{'='*60}")
        print(f"  {type_label.upper()} / {deal_label.upper()}")
        print(f"{'='*60}")

        # Otodom
        if source_filter in (None, "otodom"):
            listings = fetch_otodom(deal, prop_type, max_pages)
            all_listings.extend(listings)
            time.sleep(DELAY)

        # Gratka
        if source_filter in (None, "gratka"):
            listings = fetch_gratka(deal, prop_type, max_pages)
            all_listings.extend(listings)
            time.sleep(DELAY)

    # Deduplicate
    before = len(all_listings)
    all_listings = deduplicate(all_listings)
    after = len(all_listings)

    print(f"\n{'='*60}")
    print(f"  ИТОГО: {after} уникальных объявлений (из {before})")
    print(f"{'='*60}")

    # Stats by source
    for src in ("otodom", "gratka"):
        count = sum(1 for x in all_listings if x["source"] == src)
        print(f"    {src}: {count}")
    for deal in ("sale", "rent"):
        for ltype in ("apartment", "house"):
            count = sum(1 for x in all_listings if x["deal"] == deal and x["type"] == ltype)
            d = "продажа" if deal == "sale" else "аренда"
            t = "квартиры" if ltype == "apartment" else "дома"
            print(f"    {t}/{d}: {count}")

    # Geocode
    if not skip_geocode:
        geocode_listings(all_listings)

    with_coords = sum(1 for x in all_listings if x.get("lat") and x.get("lng"))
    print(f"  С координатами: {with_coords}/{after}")

    # Save
    with open(RAW_FILE, "w", encoding="utf-8") as f:
        json.dump(all_listings, f, ensure_ascii=False, indent=2)
    print(f"\nСохранено в {RAW_FILE}")


if __name__ == "__main__":
    main()
