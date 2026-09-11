# -*- coding: utf-8 -*-
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import airportsdata
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from curl_cffi import requests
browser_type = [
    "chrome99",
    "chrome100",
    "chrome101",
    "chrome104",
    "chrome107",
    "chrome110",
    "chrome99_android",
    "edge99",
    "edge101",
    "safari15_3",
    "safari15_5",
]
browser = random.choice(browser_type)
ses = requests.Session()
ses.impersonate = browser
ses.timeout = 30
ses.verify = False


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
    # ISO2: (domain, bookingcountry)
    # "AE": ("sixt.ae",     "AE"),
    # "AT": ("sixt.at",     "AT"),
    # "AU": ("sixt.com.au", "AU"),
    # "BE": ("sixt.be",     "BE"),
    # "BH": ("sixt.com",    "BH"),
    # "BR": ("sixt.com.br", "BR"),
    # "CA": ("sixt.ca",     "CA"),
    # "CH": ("sixt.ch",     "CH"),
    # "CN": ("sixt.com.cn", "CN"),
    "CZ": ("sixt.com",    "CZ"),
    # "DE": ("sixt.de",     "DE"),
    # "DK": ("sixt.dk",     "DK"),
    # "EE": ("sixt.ee",     "EE"),
    # "EG": ("sixt.com",    "EG"),
    # "ES": ("sixt.es",     "ES"),
    # "FI": ("sixt.fi",     "FI"),
    # "FR": ("sixt.fr",     "FR"),
    # "GB": ("sixt.co.uk",  "GB"),
    # "GE": ("sixt.com",    "GE"),
    # "HR": ("sixt.hr",     "HR"),
    # "HU": ("sixt.hu",     "HU"),
    # "IT": ("sixt.it",     "IT"),
    # "JP": ("sixt.jp",     "JP"),
    # "KW": ("sixt.com",    "KW"),
    # "LB": ("sixt.com",    "LB"),
    # "LT": ("sixt.lt",     "LT"),
    # "LV": ("sixt.lv",     "LV"),
    # "MT": ("sixt.com",    "MT"),
    # "MX": ("sixt.mx",     "MX"),
    # "NL": ("sixt.nl",     "NL"),
    # "NO": ("sixt.no",     "NO"),
    # "PL": ("sixt.pl",     "PL"),
    # "PT": ("sixt.pt",     "PT"),
    # "QA": ("sixt.com",    "QA"),
    # "RO": ("sixt.ro",     "RO"),
    # "RS": ("sixt.rs",     "RS"),
    # "SA": ("sixt.com",    "SA"),
    # "SE": ("sixt.se",     "SE"),
    # "SG": ("sixt.com.sg", "SG"),
    # "SI": ("sixt.si",     "SI"),
    # "SK": ("sixt.sk",     "SK"),
    # "TR": ("sixt.com.tr", "TR"),
    # "UA": ("sixt.ua",     "UA"),
    # "US": ("sixt.com", "US"),
}

LOCALE_MAP = {
    # "AE": "ar-AE,ar",
    # "AT": "de-AT,de",
    # "AU": "en-AU,en",
    # "BE": "nl-BE,nl",
    # "CA": "en-CA,en",  # confirmed from live capture
    # "CH": "de-CH,de",
    # "CN": "zh-CN,zh",
    "CZ": "cs-CZ,cs",
    # "DE": "de-DE,de",
    # "DK": "da-DK,da",
    # "ES": "es-ES,es",
    # "FR": "fr-FR,fr",
    # "GB": "en-GB,en",
    # "IT": "it-IT,it",
    # "LU": "fr-LU,fr",
    # "NO": "nb-NO,no-NO,nb,no,en",  # confirmed from live capture — NO is the outlier
    # "PL": "pl-PL,pl",
    # "PT": "pt-PT,pt",
    # "SE": "sv-SE,sv",
    # "US": "en-US,en",
}


def build_input_data(target_terms=None):
    """Build (ss, domain, bookingcountry, airport_name, country) rows from
    airportsdata only. City has been intentionally dropped — we only need the
    airport (IATA) code plus the airport's own ISO2 country (used for
    location_country).
    """
    airports_db = airportsdata.load("IATA")
    target_terms = {
        term.strip().upper() for term in (target_terms or []) if term.strip()
    }

    rows = []

    for iata, v in sorted(airports_db.items()):
        if target_terms and iata.upper() not in target_terms:
            continue

        for country, (domain, bookingcountry) in COUNTRY_CONFIG.items():
            rows.append(
                {
                    "ss": iata,
                    "domain": domain,
                    "bookingcountry": bookingcountry,
                    "airport_name": v["name"],
                    "country": v["country"],  # airport's own ISO2 country code
                }
            )

    rows.sort(key=lambda r: (r["bookingcountry"], r["ss"]))

    return rows


class sixt:
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
        self.websitecode = 53  # update to Sixt's actual websitecode
        self.max_workers = max_workers
        self.target_terms = target_terms or []

        self.api_cache = {}  # (term, bookingcountry) -> raw API response
        self.cache_lock = threading.Lock()
        self.failed_requests = []
        self.failure_lock = threading.Lock()
        self.seen_lock = threading.Lock()
        self.rows_lock = threading.Lock()
        self.db_lock = threading.Lock()

        self.cursor.execute(
            f"SELECT proxy FROM proxy_list WHERE status IN ({self.proxyid})"
        )
        self.proxyset = self.cursor.fetchall()

        self.length_limits = self._get_length_limits(self.cursor)

        if str(status).strip().lower() == "any":
            self.cursor.execute(
                f"""
                SELECT * FROM {self.inputtable}
                WHERE websitecode = %s AND id BETWEEN %s AND %s
                """,
                (self.websitecode, startid, endid),
            )
        else:
            self.cursor.execute(
                f"""
                SELECT * FROM {self.inputtable}
                WHERE websitecode = %s AND status = %s AND id BETWEEN %s AND %s
                """,
                (self.websitecode, status, startid, endid),
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
    def make_headers(self, bookingcountry):
        domain = COUNTRY_CONFIG[bookingcountry][0]
        locale = LOCALE_MAP.get(bookingcountry, "en-GB,en;q=0.9")
        referer = f"https://www.{domain}/"
        return {
            "accept": "application/json, text/plain, */*",
            "accept-language": locale,
            "origin": f"https://www.{domain}",
            "referer": referer,
            "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        }

    def load(self, term, bookingcountry, proxies):
        headers = self.make_headers(bookingcountry)
        return ses.get(
            BASE_URL,
            params={"term": term},
            headers=headers,
            proxies=proxies,
            timeout=30,
        )

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
        """Insert a single row into outputtable immediately (thread-safe)."""
        columns = [c for c in row.keys() if c != "id"]
        colnames = ",".join(columns)
        placeholders = ",".join(["%s"] * len(columns))
        sql = f"INSERT INTO {self.outputtable} ({colnames}) VALUES ({placeholders})"

        value_row = []
        for col in columns:
            value = row.get(col)
            max_len = self.length_limits.get(col)
            if isinstance(value, str) and max_len and len(value) > max_len:
                print(
                    "Truncated",
                    col,
                    "from",
                    len(value),
                    "to",
                    max_len,
                    "for location_code",
                    row.get("location_code"),
                )
                value = value[:max_len]
            value_row.append(value)

        with self.db_lock:
            try:
                self.cursor.execute(sql, tuple(value_row))
                self.conn.commit()
                print(
                    "INSERTED |",
                    "pickup:",
                    row.get("pickup_location"),
                    "locationcountry:",
                    row.get("location_country"),
                    "| type:",
                    row.get("location_type"),
                    "| code:",
                    row.get("location_code"),
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

    # -- SIXT API HELPERS -----------------------------------------------------
    def fetch_location_list(self, term, bookingcountry, proxies):
        ck = (term, bookingcountry)
        with self.cache_lock:
            if ck in self.api_cache:
                return self.api_cache[ck]

        errors = []
        attempts = (proxies, {})

        for attempt, current_proxies in enumerate(attempts, start=1):
            try:
                resp = self.load(term, bookingcountry, current_proxies)
                print(
                    "Status:",
                    resp.status_code,
                    "| term:",
                    term,
                    "| country:",
                    bookingcountry,
                    "| attempt:",
                    attempt,
                )
                resp.raise_for_status()
                data = resp.json()
                result = (
                    data
                    if isinstance(data, list)
                    else data.get("data", data.get("results", []))
                )

                with self.cache_lock:
                    self.api_cache[ck] = result

                time.sleep(0.3)
                return result
            except Exception as exc:
                errors.append(f"attempt {attempt}: {exc}")
                if attempt == 1:
                    print("Request failed; retrying without proxy:", term)

        with self.failure_lock:
            self.failed_requests.append(
                {
                    "term": term,
                    "bookingcountry": bookingcountry,
                    "url": BASE_URL,
                    "error": " | ".join(errors),
                }
            )
        raise RuntimeError(
            f"All request attempts failed for term {term}: {' | '.join(errors)}"
        )

    def find_match(self, location_list, ss):
        """Airport-only matching. Non-airport stations are never returned —
        we only want airport locations, so there is no 'first station'
        fallback to a City-type result anymore."""

        def clean_id(raw_id):
            return raw_id[2:] if raw_id.upper().startswith("S_") else raw_id

        stations = [loc for loc in location_list if loc.get("type") == "station"]
        airport_stations = [
            loc for loc in stations if "airport" in loc.get("subtypes", [])
        ]

        # Priority 1 - airport station whose title/subtitle contains the IATA code
        for loc in airport_stations:
            combined = (loc.get("title", "") + " " + loc.get("subtitle", "")).upper()
            if ss.upper() in combined:
                return clean_id(loc["id"]), loc.get("title", "")

        # Priority 2 - first airport station (subtitle/title didn't contain the code)
        if airport_stations:
            return clean_id(airport_stations[0]["id"]), airport_stations[0].get(
                "title", ""
            )

        return None, None

    # -- EXTRACTION ----------------------------------------------------------
    def _build_row(
        self,
        refid,
        websitecode,
        source_name,
        ss,
        location_country,
        booking_country,
        locationcode,
        is_airport,
        loctype,
        region,
        term,
        location_name,
        created_date,
    ):
        return {
            "id": refid,
            "source_name": source_name,
            "website_code": websitecode,
            "pickup_location": ss,
            "location_country": location_country,
            "booking_country": booking_country,
            "location_code": locationcode,
            "is_airport": is_airport,
            "created_date": created_date,
            "location_type": loctype,
            "region": region,
            "priority_level": "",
            "location_term": term,
            "location_name": location_name,
        }

    def extraction(
        self, item, refid, websitecode, source_name, rows, seen_location_codes
    ):
        ss = item["ss"]
        domain = item["domain"]
        bookingcountry = item["bookingcountry"]
        airport_name = item["airport_name"]
        airport_country = item["country"]  # ISO2 country the airport itself is in
        proxies = self.get_proxy()

        # Attempt 1: IATA code
        location_list = self.fetch_location_list(ss, bookingcountry, proxies)
        locationcode, locationterm = self.find_match(location_list, ss)

        # Attempt 2: airport name fallback (city has been dropped — airport only)
        if not locationcode:
            location_list = self.fetch_location_list(
                airport_name, bookingcountry, proxies
            )
            locationcode, locationterm = self.find_match(location_list, ss)

        if not locationcode:
            return

        seen_key = (bookingcountry, locationcode)
        with self.seen_lock:
            if seen_key in seen_location_codes:
                return
            seen_location_codes.add(seen_key)

        # location_country is the airport's own ISO2 country code from
        # airportsdata (e.g. AAD -> "DE", CDG -> "FR") — this is location-accurate,
        # unlike GetBranchRecommendations, which ignores the search term/country
        # and only reflects the requesting proxy's IP geolocation.
        location_country = airport_country

        # booking_country is the country we actually queried the Sixt locations
        # API under (from COUNTRY_CONFIG / item["bookingcountry"]) — kept as its
        # own separate field, distinct from location_country.
        booking_country = bookingcountry

        is_airport = True  # find_match() now only ever returns airport stations
        loctype = "Airport"

        # Hard guard: never insert a non-airport row, even if the matched
        # title/term text is unexpected.
        if not is_airport:
            return

        created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        row = self._build_row(
            refid,
            websitecode,
            source_name,
            ss,
            location_country,
            booking_country,
            locationcode,
            is_airport,
            loctype,
            "",
            locationterm or "",
            locationterm or airport_name,
            created_date,
        )

        inserted = self.insert_one(row)
        if inserted:
            with self.rows_lock:
                rows.append(row)

    # -- MAIN -------------------------------------------------------------------
    def main(self, resultset):
        input_data = build_input_data(self.target_terms)
        if self.target_terms:
            print(
                "Target retry terms:",
                ", ".join(sorted({t.upper() for t in self.target_terms})),
            )

        for result in resultset:
            refid = result["id"]
            websitecode = result["websitecode"]
            source_name = result["source_name"]
            rows = []
            seen_location_codes = set()
            try:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(
                            self.extraction,
                            item,
                            refid,
                            websitecode,
                            source_name,
                            rows,
                            seen_location_codes,
                        )
                        for item in input_data
                    ]
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception:
                            self.eHandling()

                    if rows:
                        self.update(1, refid)
                    else:
                        continue
                        # self.update(2, refid)

            except Exception:
                self.eHandling()
                self.update(2, refid)

        if self.failed_requests:
            print("\nFAILED REQUESTS:")
            for failure in self.failed_requests:
                print(
                    "-",
                    failure["url"],
                    "| term:",
                    failure["term"],
                    "| country:",
                    failure["bookingcountry"],
                    "| error:",
                    failure["error"],
                )


if __name__ == "__main__":
    STATUS = "1"
    STARTID = 301
    ENDID = 301
    INPUTTABLE = "input_locations"
    OUTPUTTABLE = "locations"
    PROXYID = "60"
    MAX_WORKERS = 15

    # 0 = normal run for all IATA codes.
    # 1 = retry only the failed/missing IATA codes below.
    RUN_MISSING_ONLY = 0
    MISSING_IATA_TERMS = ["ATL"]

    target_terms = MISSING_IATA_TERMS if RUN_MISSING_ONLY else []
    if RUN_MISSING_ONLY:
        STATUS = "any"

    SC = None
    try:
        SC = sixt(
            STATUS,
            STARTID,
            ENDID,
            INPUTTABLE,
            OUTPUTTABLE,
            False,
            PROXYID,
            max_workers=MAX_WORKERS,
            target_terms=target_terms,
        )
    except Exception:
        raise
        if SC:
            SC.eHandling()
        else:
            exc_type, exc_obj, tb = sys.exc_info()
            print("Startup error:", exc_obj)
    finally:
        if SC:
            SC.conn_close()
    time.sleep(3)