# -*- coding: utf-8 -*-
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import airportsdata
try:
    import geonamescache
except ImportError:
    geonamescache = None
import psycopg2
import tls_client
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from urllib.parse import quote, urlsplit, urlunsplit

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}

BASE_URL = "https://www.thrifty.fr/api/geodata/search"

COUNTRY_CONFIG = {
   
    "FR": ("thrifty.fr", "ZT", "FR", "fr")

}

SESSION_COOKIES = {
    
}


_GC = geonamescache.GeonamesCache() if geonamescache else None
_COUNTRIES = _GC.get_countries() if _GC else {}
_CITY_INDEX = {}

if _GC:
    for city in _GC.get_cities().values():
        key = (
            city.get("name", "").strip().lower(),
            city.get("countrycode", "").strip().upper(),
        )
        if not key[0] or not key[1]:
            continue
        current = _CITY_INDEX.get(key)
        if current is None or int(city.get("population") or 0) > int(
            current.get("population") or 0
        ):
            _CITY_INDEX[key] = city


def resolve_city_location(city_name, country_code):
    city_name = (city_name or "").strip()
    country_code = (country_code or "").strip().upper()
    if not city_name:
        return ""

    if _GC:
        match = _CITY_INDEX.get((city_name.lower(), country_code))
        if match is None:
            for (name, _country), city in _CITY_INDEX.items():
                if name == city_name.lower():
                    match = city
                    break

        if match:
            country_name = _COUNTRIES.get(match.get("countrycode", ""), {}).get(
                "name", ""
            )
            if country_name:
                return f"{match.get('name', city_name)}, {country_name}"
            return match.get("name", city_name)

    return city_name


def build_input_data(target_terms=None):
    """Build (ss, domain, vendor, rate_source_group, locale, bookingcountry,
    city, airport_name) rows from airportsdata."""
    airports_db = airportsdata.load("IATA")
    target_terms = {
        term.strip().upper() for term in (target_terms or []) if term.strip()
    }

    rows = []
    for iata, v in sorted(airports_db.items()):
        if target_terms and iata.upper() not in target_terms:
            continue

        for bookingcountry, (domain, vendor, rate_source_group, locale) in COUNTRY_CONFIG.items():
            rows.append(
                {
                    "ss": iata,
                    "domain": domain,
                    "vendor": vendor,
                    "rate_source_group": rate_source_group,
                    "locale": locale,
                    "bookingcountry": bookingcountry,
                    "city": v["city"],
                    "airport_name": v["name"],
                }
            )

    rows.sort(key=lambda r: (r["bookingcountry"], r["ss"]))
    return rows


class thrifty:
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
        self.websitecode = 58 
        self.max_workers = max_workers
        self.target_terms = target_terms or []
        self.api_cache = {}  
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

    @staticmethod
    def _sanitize_proxy_url(proxy_str):
        """Percent-encode the userinfo (user:pass) segment of a proxy URL.

        Rows in proxy_list can contain raw characters (spaces, colons inside
        the password, etc. e.g. '...BT6W_country-latin america@host:port')
        that Go's net/url (used internally by tls_client) rejects with
        'invalid userinfo'. Encoding user/pass individually fixes that
        without touching the host/port/scheme.
        """
        proxy_str = proxy_str if "://" in proxy_str else f"http://{proxy_str}"
        parts = urlsplit(proxy_str)

        if "@" not in (parts.netloc or ""):
            return proxy_str  

        userinfo, _, hostport = parts.netloc.rpartition("@")
        if ":" in userinfo:
            user, _, pwd = userinfo.partition(":")
        else:
            user, pwd = userinfo, ""

        safe_user = quote(user, safe="")
        safe_pwd = quote(pwd, safe="")
        safe_userinfo = f"{safe_user}:{safe_pwd}" if pwd else safe_user

        new_netloc = f"{safe_userinfo}@{hostport}"
        return urlunsplit((parts.scheme, new_netloc, parts.path, parts.query, parts.fragment))

    def get_proxy(self):
        if not self.proxyset:
            return {}
        proxy_str = (
            self.proxyset[random.randrange(0, len(self.proxyset))].get("proxy") or ""
        ).strip()
        if not proxy_str:
            return {}
        try:
            proxy_url = self._sanitize_proxy_url(proxy_str)
        except Exception:
            print("Skipping malformed proxy entry:", proxy_str)
            return {}
        return {"http": proxy_url, "https": proxy_url}

    def make_headers(self, domain):
        return {
            'sec-ch-ua-platform': '"Linux"',
            'Referer': f"https://www.{domain}/",
            'sec-ch-ua': '"Chromium";v="152", "Not?A_Brand";v="24", "Google Chrome";v="152"',
            'sec-ch-ua-mobile': '?0',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        }


    def make_session(self):

        return tls_client.Session(
            client_identifier="chrome112",
            random_tls_extension_order=True,
        )

    def load(self, term, item, proxies):
        session = self.make_session()
        headers = self.make_headers(item["domain"])
        data = {
            "term": term.lower(),
            "locale": item["locale"],
            "vendor": item["vendor"],
            "rate_source_group": item["rate_source_group"],
        }
        kwargs = {}
        if proxies:
            kwargs["proxy"] = proxies.get("https") or proxies.get("http")
        return session.post(
            BASE_URL,
            cookies=SESSION_COOKIES,
            headers=headers,
            data=data,
            **kwargs,
        )

    @staticmethod
    def _raise_for_status(resp):
        status = getattr(resp, "status_code", None)
        if status is None or status >= 400:
            body_preview = ""
            try:
                body_preview = (resp.text or "")[:200]
            except Exception:
                pass
            raise RuntimeError(f"HTTP {status}: {body_preview}")

    def _connect(self):
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        return conn, cursor

    def _ensure_connection(self):
        try:
            if self.conn.closed:
                raise psycopg2.InterfaceError("connection closed")
            # cheap liveness probe
            self.cursor.execute("SELECT 1")
        except Exception:
            try:
                self.conn.close()
            except Exception:
                pass
            print("DB connection lost — reconnecting...")
            self.conn, self.cursor = self._connect()

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
                print(
                    "Truncated", col, "from", len(value), "to", max_len,
                    "for location_code", row.get("location_code"),
                )
                value = value[:max_len]
            value_row.append(value)

        with self.db_lock:
            for attempt in (1, 2):
                try:
                    self._ensure_connection()
                    self.cursor.execute(sql, tuple(value_row))
                    self.conn.commit()
                    print(
                        "INSERTED |", "pickup:", row.get("pickup_location"),
                        "| type:", row.get("location_type"),
                        "| code:", row.get("location_code"),
                    )
                    return True
                except (psycopg2.InterfaceError, psycopg2.OperationalError) as exc:
                    print(
                        "DB connection error on insert (attempt", attempt, "):", exc,
                    )
                    try:
                        self.conn.close()
                    except Exception:
                        pass
                    if attempt == 2:
                        print("INSERT FAILED for location_code", row.get("location_code"))
                        self.eHandling()
                        return False
                    # loop again: _ensure_connection() will reconnect
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
        with self.db_lock:
            for attempt in (1, 2):
                try:
                    self._ensure_connection()
                    self.cursor.execute(query, params)
                    self.conn.commit()
                    return
                except (psycopg2.InterfaceError, psycopg2.OperationalError) as exc:
                    print("DB connection error on execute (attempt", attempt, "):", exc)
                    try:
                        self.conn.close()
                    except Exception:
                        pass
                    if attempt == 2:
                        raise
                except Exception:
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass
                    raise

    def fetch_location_list(self, term, item, proxies):
        ck = (term, item["bookingcountry"])
        with self.cache_lock:
            if ck in self.api_cache:
                return self.api_cache[ck]

        errors = []
        attempts = (proxies, {})

        for attempt, current_proxies in enumerate(attempts, start=1):
            try:
                resp = self.load(term, item, current_proxies)
                print(
                    "Status:", resp.status_code,
                    "| term:", term, "| country:", item["bookingcountry"],
                    "| attempt:", attempt,
                )
                self._raise_for_status(resp)
                payload = resp.json() if resp.text else None
                result = self._extract_raw_entries(payload)

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
                    "bookingcountry": item["bookingcountry"],
                    "url": BASE_URL,
                    "error": " | ".join(errors),
                }
            )
        raise RuntimeError(
            f"All request attempts failed for term {term}: {' | '.join(errors)}"
        )

    def _extract_raw_entries(self, payload):
        """Return the raw dict entries (not just the renamed subset) so we
        still have access to any id/code/type fields the API returns."""
        if isinstance(payload, list):
            candidates = payload
        elif isinstance(payload, dict):
            candidates = payload.get("data") or payload.get("results") or []
        else:
            candidates = []

        return [item for item in candidates if isinstance(item, dict) and "geo_name" in item]

    def find_match(self, location_list, ss):

        def combined_text(loc):
            return f"{loc.get('geo_name','')} {loc.get('translated_name','')}".upper()

        def is_airport_entry(loc):
            t = str(loc.get("type", "")).upper()
            if t:
                return "AIRPORT" in t
            return "AIRPORT" in combined_text(loc)

        for loc in location_list:
            if ss.upper() in combined_text(loc) and is_airport_entry(loc):
                return loc

        for loc in location_list:
            if ss.upper() in combined_text(loc):
                return loc

        for loc in location_list:
            if is_airport_entry(loc):
                return loc

        return location_list[0] if location_list else None

    def _location_code(self, loc, fallback):

        for key in ("code", "id", "station_id", "location_code", "geo_id"):
            if loc.get(key):
                return str(loc[key])
        return fallback

    def _build_row(
        self, refid, websitecode, source_name, ss, bookingcountry,
        locationcode, is_airport, loctype, city_location, region,
        term, location_name, created_date, latitude, longitude,
    ):
        return {
            "id": refid,
            "source_name": source_name,
            "website_code": websitecode,
            "pickup_location": ss,
            "location_country": bookingcountry,
            "location_code": locationcode,
            "is_airport": is_airport,
            "created_date": created_date,
            "location_type": loctype,
            "city": city_location,
            "region": region,
            "priority_level": "",
            "location_term": term,
            "location_name": location_name,
            "latitude": latitude,
            "longitude": longitude,
        }

    def extraction(self, item, refid, websitecode, source_name, rows, seen_location_codes):
        ss = item["ss"]
        bookingcountry = item["bookingcountry"]
        city = item["city"]
        airport_name = item["airport_name"]
        city_location = resolve_city_location(city, bookingcountry)
        proxies = self.get_proxy()

        location_list = self.fetch_location_list(ss, item, proxies)
        match = self.find_match(location_list, ss)

        if not match:
            location_list = self.fetch_location_list(city, item, proxies)
            match = self.find_match(location_list, ss)

        if not match:
            return

        locationcode = self._location_code(match, fallback=ss)
        locationterm = match.get("translated_name") or ""
        geo_name = match.get("geo_name") or airport_name

        seen_key = (bookingcountry, locationcode)
        with self.seen_lock:
            if seen_key in seen_location_codes:
                return
            seen_location_codes.add(seen_key)

        created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        is_airport = "AIRPORT" in (locationterm or geo_name).upper()
        loctype = "Airport" if is_airport else "City"

        row = self._build_row(
            refid, websitecode, source_name, ss, bookingcountry,
            locationcode, is_airport, loctype, city_location, "",
            locationterm, geo_name, created_date,
            match.get("latitude"), match.get("longitude"),
        )

        inserted = self.insert_one(row)
        if inserted:
            with self.rows_lock:
                rows.append(row)

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
                            self.extraction, item, refid, websitecode,
                            source_name, rows, seen_location_codes,
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
                    "-", failure["url"], "| term:", failure["term"],
                    "| country:", failure["bookingcountry"],
                    "| error:", failure["error"],
                )


if __name__ == "__main__":
    STATUS = "0"
    STARTID = 269
    ENDID = 269
    INPUTTABLE = "input_locations"
    OUTPUTTABLE = "locations"
    PROXYID = "59"
    MAX_WORKERS = 20

    # 0 = normal run for all IATA codes.
    # 1 = retry only the failed/missing IATA codes below.
    RUN_MISSING_ONLY = 0
    MISSING_IATA_TERMS = []

    target_terms = MISSING_IATA_TERMS if RUN_MISSING_ONLY else []
    if RUN_MISSING_ONLY:
        STATUS = "any"

    TC = None
    try:
        TC = thrifty(
            STATUS, STARTID, ENDID, INPUTTABLE, OUTPUTTABLE, False, PROXYID,
            max_workers=MAX_WORKERS, target_terms=target_terms,
        )
    except Exception:
        raise
    finally:
        if TC:
            TC.conn_close()
    time.sleep(3)