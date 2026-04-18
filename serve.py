#!/usr/bin/env python3
"""
HomeSearcher — lightweight HTTP server.
Serves the static HTML frontend and provides an API to trigger live fetching.

Endpoints:
    GET /                           → index.html
    GET /data/*                     → static JSON files
    GET /api/fetch?source=...       → trigger scraper, return JSON results
    GET /api/listings               → return current listings_raw.json
    GET /api/enrich                 → trigger proximity enrichment

Usage:
    python serve.py                  # start on port 8080
    python serve.py --port 3000      # custom port
"""

import json
import os
import sys
import threading
import time
import urllib.parse
from http.server import HTTPServer, SimpleHTTPRequestHandler
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
RAW_FILE = os.path.join(DATA_DIR, "listings_raw.json")
ENRICHED_FILE = os.path.join(DATA_DIR, "listings.json")

# Track ongoing fetch jobs
_fetch_lock = threading.Lock()
_fetch_status = {"running": False, "source": None, "started": None, "message": ""}


def _run_fetch(source, max_pages):
    """Run the scraper in a background thread."""
    global _fetch_status
    try:
        # Import the scraper functions from fetch_listings.py
        sys.path.insert(0, SCRIPT_DIR)
        import fetch_listings as fl
        # Reload to pick up any changes
        import importlib
        importlib.reload(fl)

        fl.NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        all_listings = []
        for config in fl.SEARCH_CONFIGS:
            deal = config["deal"]
            prop_type = config["type"]

            if source in (None, "all", "otodom"):
                listings = fl.fetch_otodom(deal, prop_type, max_pages)
                all_listings.extend(listings)
                time.sleep(fl.DELAY)

            if source in (None, "all", "gratka"):
                listings = fl.fetch_gratka(deal, prop_type, max_pages)
                all_listings.extend(listings)
                time.sleep(fl.DELAY)

        all_listings = fl.deduplicate(all_listings)

        # Geocode
        fl.geocode_listings(all_listings)

        # Merge with existing data (keep old listings that weren't re-fetched)
        existing = []
        if os.path.exists(RAW_FILE):
            with open(RAW_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)

        if source and source not in (None, "all"):
            # Keep listings from other sources, replace only the fetched source
            other = [l for l in existing if l.get("source") != source]
            merged = other + all_listings
        else:
            merged = all_listings

        merged = fl.deduplicate(merged)

        os.makedirs(DATA_DIR, exist_ok=True)
        with open(RAW_FILE, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)

        with _fetch_lock:
            _fetch_status["running"] = False
            _fetch_status["message"] = f"Готово: {len(all_listings)} новых, {len(merged)} всего"
            _fetch_status["count"] = len(merged)
            _fetch_status["listings"] = merged

    except Exception as e:
        with _fetch_lock:
            _fetch_status["running"] = False
            _fetch_status["message"] = f"Ошибка: {e}"
            _fetch_status["listings"] = []


class HomeSearcherHandler(SimpleHTTPRequestHandler):
    """Handle both static files and API requests."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=SCRIPT_DIR, **kwargs)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        if path == "/api/fetch":
            self._handle_fetch(params)
        elif path == "/api/status":
            self._handle_status()
        elif path == "/api/listings":
            self._handle_listings()
        elif path == "/api/enrich":
            self._handle_enrich(params)
        else:
            super().do_GET()

    def _send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _handle_fetch(self, params):
        global _fetch_status
        source = params.get("source", [None])[0] or "all"
        max_pages = int(params.get("pages", [3])[0])
        # Clamp max_pages to prevent abuse
        max_pages = min(max(1, max_pages), 20)

        with _fetch_lock:
            if _fetch_status["running"]:
                self._send_json({
                    "status": "busy",
                    "message": f"Уже идёт загрузка ({_fetch_status['source']})"
                }, 429)
                return
            _fetch_status = {
                "running": True,
                "source": source,
                "started": datetime.now(timezone.utc).isoformat(),
                "message": f"Загрузка из {source}...",
                "listings": [],
            }

        # Run in background thread
        t = threading.Thread(target=_run_fetch, args=(source, max_pages), daemon=True)
        t.start()

        self._send_json({
            "status": "started",
            "source": source,
            "max_pages": max_pages,
            "message": f"Загрузка из {source} началась (до {max_pages} стр.)"
        })

    def _handle_status(self):
        with _fetch_lock:
            status = {
                "running": _fetch_status["running"],
                "source": _fetch_status.get("source"),
                "message": _fetch_status.get("message", ""),
            }
        self._send_json(status)

    def _handle_listings(self):
        # Return the current listings data
        target = ENRICHED_FILE if os.path.exists(ENRICHED_FILE) else RAW_FILE
        if os.path.exists(target):
            with open(target, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._send_json(data)
        else:
            self._send_json([])

    def _handle_enrich(self, params):
        """Trigger proximity enrichment."""
        batch = int(params.get("batch", [50])[0])
        batch = min(max(1, batch), 200)

        try:
            sys.path.insert(0, SCRIPT_DIR)
            import enrich_listings as el
            import importlib
            importlib.reload(el)
            # Run enrichment synchronously (it's I/O bound but fast per item)
            el.main_args(["--batch", str(batch)])
            self._send_json({"status": "ok", "message": f"Обогащено до {batch} объявлений"})
        except Exception as e:
            self._send_json({"status": "error", "message": str(e)}, 500)

    def log_message(self, format, *args):
        # Cleaner logging — skip static file noise
        path = args[0].split()[1] if args else ""
        if path.startswith("/api/"):
            super().log_message(format, *args)


def main():
    port = 8080
    if "--port" in sys.argv:
        idx = sys.argv.index("--port")
        if idx + 1 < len(sys.argv):
            port = int(sys.argv[idx + 1])

    server = HTTPServer(("127.0.0.1", port), HomeSearcherHandler)
    print(f"  🏠 HomeSearcher server: http://localhost:{port}")
    print(f"  📂 Serving from: {SCRIPT_DIR}")
    print(f"  API endpoints:")
    print(f"    GET /api/fetch?source=otodom|gratka|all&pages=3")
    print(f"    GET /api/status")
    print(f"    GET /api/listings")
    print(f"    GET /api/enrich?batch=50")
    print(f"\n  Ctrl+C to stop\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")
        server.server_close()


if __name__ == "__main__":
    main()
