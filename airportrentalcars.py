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
from psycopg2.extras import RealDictCursor

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}

SEARCH_URL_TMPL = "https://www.airportrentalcars.com/pws/v0/index/drive/search/{term}"

# Static params copied from the captured request. cguid/rguid/vid/pxcts/etc are
# session-scoped identifiers tied to the cookies below - if the site starts
# rejecting requests, these (and the cookies) are the first thing to refresh
# by re-capturing a fresh request from DevTools.
STATIC_PARAMS = {
    "apc": "DESKTOP_RC",
    "apv": "",
    "at": "",
    "cguid": "uvlWLBrVEk3Y5ADoN3VjQkyoHZa23Hnv",
    "locale": "en-us",
    "ltr": "false",
    "numAirports": "50",   # bumped up from the captured 6 to pull more per term
    "numCities": "50",
    "numPOIs": "50",
    "numPartnerLocations": "50",
    "personalizeSearch": "true",
    "rcid": "",
    "rguid": "2026082102344871045720",
    "rid": "",
    "source-id": "RCWEB_TYPEAHEAD",
    "vid": "v2026082102344871045720",
}

# NOTE: these cookies are session/bot-check tokens (Perimeterx __px*, Cloudflare
# __cf_bm, Forter forterToken, etc). They WILL expire - if requests start
# failing or returning empty results, re-capture a fresh cURL from DevTools
# and swap this dict out.
STATIC_COOKIES = {
    "PL_CINFO": "uvlWLBrVEk3Y5ADoN3VjQkyoHZa23Hnv~1787294088~v2",
    "SITESERVER": "ID=uvlWLBrVEk3Y5ADoN3VjQkyoHZa23Hnv",
    "vid": "v2026082102344871045720",
    "selcur": "INR",
}

BASE_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "priority": "u=1, i",
    "referer": "https://www.airportrentalcars.com/landing/",
    "sec-ch-ua": '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36",
}

# Confirmed from a real response: results come back as a flat "searchItems"
# list, each item carrying a "type" field. Map that type to (location_type, is_airport).
TYPE_MAP = {
    "AIRPORT": ("Airport", True),
    "CITY": ("City", False),
    "POI": ("POI", False),
    "PARTNER_LOC": ("Partner", False),
}

DEBUG_PRINT_FIRST_RESPONSE = True  # set False once you've eyeballed one run


def get_first(d, *keys, default=""):
    if not isinstance(d, dict):
        return default
    for k in keys:
        v = d.get(k)
        if v not in (None, ""):
            return v
    return default


def build_terms():
    """All IATA airport codes - same approach as the Expedia script."""
    airports_db = airportsdata.load("IATA")
    return sorted(airports_db.keys())


class airportrentalcars:
    def __init__(
        self,
        status,
        startid,
        endid,
        inputtable,
        outputtable,
        offline,
        proxyid,
        max_workers=10,
        target_terms=None,
    ):
        self.inputtable = inputtable
        self.outputtable = outputtable
        self.startid = startid
        self.endid = endid
        self.proxyid = proxyid
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        self.websitecode = 7
        self.max_workers = max_workers
        self.target_terms = target_terms or []

        self._debug_printed = False
        self.debug_lock = threading.Lock()
        self.seen_lock = threading.Lock()
        self.rows_lock = threading.Lock()
        self.db_lock = threading.Lock()
        self.failure_lock = threading.Lock()
        self.failed_requests = []

        self.cursor.execute(
            f"SELECT proxy FROM proxy_list WHERE status IN ({self.proxyid})"
        )
        self.proxyset = self.cursor.fetchall()

        self.length_limits = self._get_length_limits(self.cursor)

        if str(status).strip().lower() == "any":
            self.cursor.execute(
                f"""
                SELECT * FROM {self.inputtable}
                WHERE id BETWEEN %s AND %s
                """,
                (startid, endid),
            )
        else:
            self.cursor.execute(
                f"""
                SELECT * FROM {self.inputtable}
                WHERE status = %s AND id BETWEEN %s AND %s
                """,
                (status, startid, endid),
            )
        resultset = self.cursor.fetchall()
        self.main(resultset)

    # -- PROXY --------------------------------------------------------------
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

    # -- HTTP -----------------------------------------------------------------
    def load(self, term, proxies):
        url = SEARCH_URL_TMPL.format(term=term)
        return requests.get(
            url,
            params=STATIC_PARAMS,
            cookies=STATIC_COOKIES,
            headers=BASE_HEADERS,
            proxies=proxies,
            timeout=30,
        )

    def fetch_locations(self, term, proxies):
        attempts = (proxies, {})
        errors = []
        for attempt, current_proxies in enumerate(attempts, start=1):
            try:
                resp = self.load(term, current_proxies)
                print(
                    "Status:", resp.status_code,
                    "| term:", term,
                    "| attempt:", attempt,
                )
                resp.raise_for_status()
                data = resp.json()

                if DEBUG_PRINT_FIRST_RESPONSE:
                    with self.debug_lock:
                        if not self._debug_printed:
                            self._debug_printed = True
                            print("\n===== RAW RESPONSE SAMPLE (term=%s) =====" % term)
                            # print(json.dumps(data, indent=2)[:4000])
                            print("===== END SAMPLE =====\n")

                time.sleep(0.3)
                return data
            except Exception as exc:
                errors.append(f"attempt {attempt}: {exc}")
                if attempt == 1:
                    print("Request failed; retrying without proxy:", term)

        with self.failure_lock:
            self.failed_requests.append(
                {"term": term, "error": " | ".join(errors)}
            )
        raise RuntimeError(f"All attempts failed for term {term}: {' | '.join(errors)}")

    # -- DB ---------------------------------------------------------------------
    def _get_length_limits(self, cursor):
        cursor.execute(
            """
            SELECT column_name, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = %s
              AND character_maximum_length IS NOT NULL
            """,
            (self.outputtable.split(".")[-1],),
        )
        return {
            row["column_name"]: row["character_maximum_length"]
            for row in cursor.fetchall()
        }

    def insert_one(self, row):
        columns = [c for c in row.keys() if c != "id"]
        colnames = ",".join(columns)
        placeholders = ",".join(["%s"] * len(columns))
        sql = f"INSERT INTO {self.outputtable} ({colnames}) VALUES ({placeholders})"

        value_row = []
        for col in columns:
            value = row.get(col)
            max_len = self.length_limits.get(col)
            if isinstance(value, str) and max_len and len(value) > max_len:
                value = value[:max_len]
            value_row.append(value)

        with self.db_lock:
            try:
                self.cursor.execute(sql, tuple(value_row))
                self.conn.commit()
                print(
                    "INSERTED |",
                    "pickup:", row.get("pickup_location"),
                    "| type:", row.get("location_type"),
                    "| code:", row.get("location_code"),
                )
                return True
            except Exception:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                print("INSERT FAILED for location_code", row.get("location_code"))
                self.eHandling()
                return False

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

    # -- EXTRACTION ----------------------------------------------------------
    def _build_row(
        self, refid, websitecode, source_name, pickup_location, location_country,
        location_code, is_airport, loctype, city, region, term, location_name,
        latitude, longitude, created_date,
    ):
        return {
            "id": refid,
            "source_name": source_name,
            "website_code": websitecode,
            "pickup_location": pickup_location,
            "location_country": location_country,
            "location_code": location_code,
            "is_airport": is_airport,
            "created_date": created_date,
            "location_type": loctype,
            "city": city,
            "region": region,
            "priority_level": "",
            "location_term": location_name,
            "location_name": term,
            "latitude": latitude,
            "longitude": longitude,
        }

    def extraction(
        self, term, refid, country, websitecode, source_name, rows, seen_location_codes
    ):
        proxies = self.get_proxy()
        data = self.fetch_locations(term, proxies)
        if not isinstance(data, dict):
            return

        items = data.get("searchItems")
        if not isinstance(items, list):
            return

        created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        for item in items:
            if not isinstance(item, dict):
                continue

            item_type = str(item.get("type") or "").strip().upper()
            loctype, is_airport = TYPE_MAP.get(item_type, (item_type.title() or "Other", False))

            location_code = str(get_first(item, "id")).strip()
            location_name = re.sub(
                r"\s+", " ", str(get_first(item, "displayName", "itemName"))
            ).strip()
            city = re.sub(r"\s+", " ", str(get_first(item, "cityName"))).strip()
            region = re.sub(
                r"\s+", " ", str(get_first(item, "stateCode", "provinceName"))
            ).strip()
            # isoCountryCode is the standard 2-letter code (countryCode is
            # sometimes blank or non-ISO, e.g. "AG" for Algeria above).
            location_country = str(
                get_first(item, "isoCountryCode", "countryCode", default=country)
            ).strip()
            latitude = item.get("lat")
            longitude = item.get("lon")

            # pickup_location: the airport code for airports, city name otherwise
            if item_type == "AIRPORT":
                pickup_location = location_code
            else:
                pickup_location = city

            if not location_code or not location_name:
                continue

            seen_key = (location_country, location_code, loctype)
            with self.seen_lock:
                if seen_key in seen_location_codes:
                    continue
                seen_location_codes.add(seen_key)

            row = self._build_row(
                refid, websitecode, source_name, pickup_location,
                location_country, location_code, is_airport, loctype,
                city, region, term, location_name, latitude, longitude,
                created_date,
            )

            inserted = self.insert_one(row)
            if inserted:
                with self.rows_lock:
                    rows.append(row)

    # -- MAIN -------------------------------------------------------------------
    def main(self, resultset):
        terms = self.target_terms or build_terms()
        print("Total terms to search:", len(terms))

        for result in resultset:
            refid = result["id"]
            websitecode = result["websitecode"]
            self.websitecode = websitecode
            source_name = result["source_name"]
            country = result["country"]
            rows = []
            seen_location_codes = set()

            try:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(
                            self.extraction,
                            term, refid, country, websitecode, source_name,
                            rows, seen_location_codes,
                        )
                        for term in terms
                    ]
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception:
                            self.eHandling()

                print("Extracted:", len(rows))
                if rows:
                    self.update(1, refid)
                else:
                    self.update(2, refid)
            except Exception:
                self.eHandling()
                self.update(2, refid)

        if self.failed_requests:
            print("\nFAILED REQUESTS:")
            for failure in self.failed_requests:
                print("-", failure["term"], "| error:", failure["error"])


if __name__ == "__main__":
    STATUS = 0
    STARTID = 264
    ENDID = 264
    INPUTTABLE = "input_locations"
    OUTPUTTABLE = "locations"
    PROXYID = "60"
    MAX_WORKERS = 10

    # 0 = normal run for all IATA codes.
    # 1 = retry only the failed/missing IATA codes below.
    RUN_MISSING_ONLY = 0
    MISSING_IATA_TERMS = ["ABU"]

    target_terms = MISSING_IATA_TERMS if RUN_MISSING_ONLY else []
    if RUN_MISSING_ONLY:
        STATUS = "any"

    SC = None
    try:
        SC = airportrentalcars(
            STATUS, STARTID, ENDID, INPUTTABLE, OUTPUTTABLE, False, PROXYID,
            max_workers=MAX_WORKERS,
            target_terms=target_terms,
        )
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