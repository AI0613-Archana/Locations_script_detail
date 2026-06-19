# -*- coding: utf-8 -*-
import json
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import airportsdata
import psycopg2
from curl_cffi import requests
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor, execute_values

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}

BASE_URL = "https://web-api.orange.sixt.com/v1/locations"

COUNTRY_CONFIG = {
    "AE": ("sixt.ae",     "AE"),
    "AU": ("sixt.com.au", "AU"),
    "BE": ("sixt.be",     "BE"),
    "BH": ("sixt.com",    "BH"),
    "BR": ("sixt.com.br", "BR"),
    "CA": ("sixt.ca",     "CA"),
    "CH": ("sixt.ch",     "CH"),
    "CN": ("sixt.com.cn", "CN"),
    "DE": ("sixt.de",     "DE"),
    "DK": ("sixt.dk",     "DK"),
    "EE": ("sixt.ee",     "EE"),
    "EG": ("sixt.com",    "EG"),
    "ES": ("sixt.es",     "ES"),
    "FI": ("sixt.fi",     "FI"),
    "FR": ("sixt.fr",     "FR"),
    "GB": ("sixt.co.uk",  "GB"),
    "GE": ("sixt.com",    "GE"),
    "HR": ("sixt.hr",     "HR"),
    "HU": ("sixt.hu",     "HU"),
    "IT": ("sixt.it",     "IT"),
    "JP": ("sixt.jp",     "JP"),
    "KW": ("sixt.com",    "KW"),
    "LB": ("sixt.com",    "LB"),
    "LT": ("sixt.lt",     "LT"),
    "LV": ("sixt.lv",     "LV"),
    "MT": ("sixt.com",    "MT"),
    "MX": ("sixt.mx",     "MX"),
    "NL": ("sixt.nl",     "NL"),
    "NO": ("sixt.no",     "NO"),
    "PL": ("sixt.pl",     "PL"),
    "PT": ("sixt.pt",     "PT"),
    "QA": ("sixt.com",    "QA"),
    "RO": ("sixt.ro",     "RO"),
    "RS": ("sixt.rs",     "RS"),
    "SA": ("sixt.com",    "SA"),
    "SE": ("sixt.se",     "SE"),
    "SG": ("sixt.com.sg", "SG"),
    "SI": ("sixt.si",     "SI"),
    "SK": ("sixt.sk",     "SK"),
    "TR": ("sixt.com.tr", "TR"),
    "UA": ("sixt.ua",     "UA"),
    "US": ("sixt.com",    "US"),
}

LOCALE_MAP = {
    "AE": "ar-AE,ar;q=0.9",
    "AU": "en-AU,en;q=0.9",
    "BE": "nl-BE,nl;q=0.9",
    "BH": "ar-BH,ar;q=0.9",
    "BR": "pt-BR,pt;q=0.9",
    "CA": "en-CA,en;q=0.9",
    "CH": "de-CH,de;q=0.9",
    "CN": "zh-CN,zh;q=0.9",
    "DE": "de-DE,de;q=0.9",
    "DK": "da-DK,da;q=0.9",
    "EE": "et-EE,et;q=0.9",
    "EG": "ar-EG,ar;q=0.9",
    "ES": "es-ES,es;q=0.9",
    "FI": "fi-FI,fi;q=0.9",
    "FR": "fr-FR,fr;q=0.9",
    "GB": "en-GB,en;q=0.9",
    "GE": "ka-GE,ka;q=0.9",
    "HR": "hr-HR,hr;q=0.9",
    "HU": "hu-HU,hu;q=0.9",
    "IT": "it-IT,it;q=0.9",
    "JP": "ja-JP,ja;q=0.9",
    "KW": "ar-KW,ar;q=0.9",
    "LB": "ar-LB,ar;q=0.9",
    "LT": "lt-LT,lt;q=0.9",
    "LV": "lv-LV,lv;q=0.9",
    "MT": "mt-MT,mt;q=0.9",
    "MX": "es-MX,es;q=0.9",
    "NL": "nl-NL,nl;q=0.9",
    "NO": "nb-NO,nb;q=0.9",
    "PL": "pl-PL,pl;q=0.9",
    "PT": "pt-PT,pt;q=0.9",
    "QA": "ar-QA,ar;q=0.9",
    "RO": "ro-RO,ro;q=0.9",
    "RS": "sr-RS,sr;q=0.9",
    "SA": "ar-SA,ar;q=0.9",
    "SE": "sv-SE,sv;q=0.9",
    "SG": "en-SG,en;q=0.9",
    "SI": "sl-SI,sl;q=0.9",
    "SK": "sk-SK,sk;q=0.9",
    "TR": "tr-TR,tr;q=0.9",
    "UA": "uk-UA,uk;q=0.9",
    "US": "en-US,en;q=0.9",
}


def build_input_data():
    """Build (ss, domain, bookingcountry, city, airport_name) rows from airportsdata."""
    airports_db = airportsdata.load("IATA")
    rows = []
    for iata, v in sorted(airports_db.items()):
        country = v["country"]
        if country not in COUNTRY_CONFIG:
            continue
        domain, bookingcountry = COUNTRY_CONFIG[country]
        rows.append({
            "ss":             iata,
            "domain":         domain,
            "bookingcountry": bookingcountry,
            "city":           v["city"],
            "airport_name":   v["name"],
        })
    rows.sort(key=lambda r: (r["bookingcountry"], r["ss"]))
    return rows


class sixt:
    def __init__(
        self, status, startid, endid, inputtable, outputtable, offline, proxyid,
        max_workers=10,
    ):
        self.inputtable  = inputtable
        self.outputtable = outputtable
        self.startid     = startid
        self.endid       = endid
        self.proxyid     = proxyid
        self.conn        = psycopg2.connect(**DB_CONFIG)
        self.cursor      = self.conn.cursor(cursor_factory=RealDictCursor)
        self.websitecode = 55          # ← update to Sixt's actual websitecode
        self.max_workers = max_workers

        self.api_cache   = {}           # (term, bookingcountry) → location list
        self.cache_lock  = threading.Lock()
        self.seen_lock   = threading.Lock()
        self.rows_lock   = threading.Lock()

        self.cursor.execute(
            f"SELECT proxy FROM proxy_list WHERE status IN ({self.proxyid})"
        )
        self.proxyset = self.cursor.fetchall()

        self.cursor.execute(
            f"""
            SELECT * FROM {self.inputtable}
            WHERE websitecode = %s::text AND status = %s AND id BETWEEN %s AND %s
            """,
            (str(self.websitecode), status, startid, endid),
        )
        resultset = self.cursor.fetchall()
        self.main(resultset)

    # ── PROXY ─────────────────────────────────────────────────────────────────
    def get_proxy(self):
        if not self.proxyset:
            return {}
        proxy_str = (
            self.proxyset[random.randrange(0, len(self.proxyset))].get("proxy") or ""
        ).strip()
        if not proxy_str:
            return {}
        proxy_url = proxy_str if "://" in proxy_str else f"http://{proxy_str}"
        return {"http": proxy_url, "https": proxy_url}

    # ── HTTP ──────────────────────────────────────────────────────────────────
    def make_headers(self, bookingcountry):
        domain  = COUNTRY_CONFIG[bookingcountry][0]
        locale  = LOCALE_MAP.get(bookingcountry, "en-GB,en;q=0.9")
        referer = f"https://www.{domain}/"
        return {
            "accept":             "application/json, text/plain, */*",
            "accept-language":    locale,
            "origin":             f"https://www.{domain}",
            "referer":            referer,
            "sec-ch-ua":          '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            "sec-ch-ua-mobile":   "?0",
            "sec-ch-ua-platform": '"Linux"',
            "sec-fetch-dest":     "empty",
            "sec-fetch-mode":     "cors",
            "sec-fetch-site":     "cross-site",
            "user-agent":         "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                                  "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        }

    def load(self, term, bookingcountry, proxies):
        headers = self.make_headers(bookingcountry)
        return requests.get(
            BASE_URL,
            params={"term": term},
            headers=headers,
            proxies=proxies,
            timeout=30,
        )

    # ── DB ────────────────────────────────────────────────────────────────────
    def insert(self, chunks):
        if not chunks:
            print("No rows supplied for insert.")
            return

        print("INSERT INITIATED")
        columns = [c for c in chunks[0].keys() if c != "id"]
        colnames = ",".join(columns)
        sql = f"INSERT INTO {self.outputtable} ({colnames}) VALUES %s"
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT column_name, character_maximum_length
                    FROM information_schema.columns
                    WHERE table_name = %s
                      AND character_maximum_length IS NOT NULL
                    """,
                    (self.outputtable.split(".")[-1],),
                )
                length_limits = {
                    row["column_name"]: row["character_maximum_length"]
                    for row in cursor.fetchall()
                }

                values = []
                for row in chunks:
                    value_row = []
                    for col in columns:
                        value     = row.get(col)
                        max_len   = length_limits.get(col)
                        if isinstance(value, str) and max_len and len(value) > max_len:
                            print(
                                "Truncated", col,
                                "from", len(value), "to", max_len,
                                "for location_code", row.get("location_code"),
                            )
                            value = value[:max_len]
                        value_row.append(value)
                    values.append(tuple(value_row))

                execute_values(cursor, sql, values, page_size=500)
            self.conn.commit()
        except Exception:
            try:
                self.conn.rollback()
            except Exception:
                pass
            raise
        print("INSERTED")

    def update(self, upstatus, refid):
        updateq = f"UPDATE {self.inputtable} SET status=%s WHERE id=%s"
        self._execute_commit(updateq, (upstatus, refid))
        print(self.websitecode, "updated as", upstatus, "for id", refid)

    def conn_close(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            pass

    def eHandling(self):
        import traceback
        traceback.print_exc()

    def _execute_commit(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            self.conn.commit()
        except Exception:
            try:
                self.conn.rollback()
            except Exception:
                pass
            raise

    # ── SIXT API HELPERS ──────────────────────────────────────────────────────
    def fetch_location_list(self, term, bookingcountry, proxies):
        ck = (term, bookingcountry)

        with self.cache_lock:
            if ck in self.api_cache:
                return self.api_cache[ck]

        try:
            resp = self.load(term, bookingcountry, proxies)
            print("Status:", resp.status_code, "| term:", term, "| country:", bookingcountry)
        except Exception as exc:
            print("Proxy failed:", proxies.get("https", ""), "error:", exc)
            resp = self.load(term, bookingcountry, {})
            print("Status:", resp.status_code, "(no proxy)")

        resp.raise_for_status()
        data = resp.json()
        result = (
            data if isinstance(data, list)
            else data.get("data", data.get("results", []))
        )

        with self.cache_lock:
            self.api_cache[ck] = result

        time.sleep(0.3)
        return result

    def find_match(self, location_list, ss):
        def clean_id(raw_id):
            return raw_id[2:] if raw_id.upper().startswith("S_") else raw_id

        stations = [loc for loc in location_list if loc.get("type") == "station"]

        # Priority 1 – airport station whose title/subtitle contains the IATA code
        for loc in stations:
            combined = (loc.get("title", "") + " " + loc.get("subtitle", "")).upper()
            if "airport" in loc.get("subtypes", []) and ss.upper() in combined:
                return clean_id(loc["id"]), loc.get("title", "")

        # Priority 2 – any station whose title/subtitle contains the IATA code
        for loc in stations:
            combined = (loc.get("title", "") + " " + loc.get("subtitle", "")).upper()
            if ss.upper() in combined:
                return clean_id(loc["id"]), loc.get("title", "")

        # Priority 3 – first airport station
        for loc in stations:
            if "airport" in loc.get("subtypes", []):
                return clean_id(loc["id"]), loc.get("title", "")

        # Priority 4 – first station
        if stations:
            return clean_id(stations[0]["id"]), stations[0].get("title", "")
        return None, None

    # ── EXTRACTION ────────────────────────────────────────────────────────────
    def extraction(
        self, item, refid, websitecode, source_name, rows, seen_location_codes
    ):
        ss             = item["ss"]
        domain         = item["domain"]
        bookingcountry = item["bookingcountry"]
        city           = item["city"]
        airport_name   = item["airport_name"]

        proxies        = self.get_proxy()

        # Attempt 1: IATA code
        location_list = self.fetch_location_list(ss, bookingcountry, proxies)
        locationcode, locationterm = self.find_match(location_list, ss)

        # Attempt 2: city name fallback
        if not locationcode:
            location_list = self.fetch_location_list(city, bookingcountry, proxies)
            locationcode, locationterm = self.find_match(location_list, ss)

        if not locationcode:
            return

        with self.seen_lock:
            if locationcode in seen_location_codes:
                return
            seen_location_codes.add(locationcode)

        created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        row = {
            "id":               refid,
            "source_name":      source_name,
            "website_code":     websitecode,
            "pickup_location":  ss,
            "location_country": bookingcountry,
            "location_code":    locationcode,
            "is_airport":       True if "AIRPORT" in (locationterm or airport_name).upper() else False,
            "created_date":     created_date,
            "location_type":    "Airport" if "AIRPORT" in (locationterm or airport_name).upper() else "City",
            "city":             city,
            "region":           "",
            "priority_level":   "",
            "location_term":    locationterm or "",
            "location_name":    locationterm or airport_name,
        }

        with self.rows_lock:
            rows.append(row)

    # ── MAIN ──────────────────────────────────────────────────────────────────
    def main(self, resultset):
        input_data           = build_input_data()
        seen_location_codes  = set()

        for result in resultset:
            refid       = result["id"]
            websitecode = result["websitecode"]
            source_name = result["source_name"]

            rows = []
            try:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(
                            self.extraction, item, refid, websitecode, source_name,
                            rows, seen_location_codes,
                        )
                        for item in input_data
                    ]
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception:
                            self.eHandling()
                    if rows:
                        self.insert(rows)
                        self.update(1, refid)
                    else:
                        self.update(2, refid)

            except Exception:
                self.eHandling()
                self.update(2, refid)


# ── ENTRY POINT ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    SC = None
    try:
        SC = sixt(1, 140, 140, "input_locations", "locations", False, "20", max_workers=5)

        # (
        #     script,
        #     status,
        #     startid,
        #     endid,
        #     inputtable,
        #     outputtable,
        #     offline,
        #     proxyid,
        # ) = sys.argv
        # SC = sixt(
        #     status,
        #     startid,
        #     endid,
        #     inputtable,
        #     outputtable,
        #     offline,
        #     proxyid,
        # )
    except Exception:
        if SC:
            SC.eHandling()
        else:
            exc_type, exc_obj, tb = sys.exc_info()
            print("Startup error:", exc_obj)
    finally:
        if SC:
            SC.conn_close()
    time.sleep(3)