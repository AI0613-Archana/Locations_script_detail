# -*- coding: utf-8 -*-
import os
import random
import sys
import threading
import time
from hashlib import sha256
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait, as_completed
from datetime import datetime, timezone
from queue import Queue

import airportsdata
try:
    import geonamescache
except ImportError:
    geonamescache = None
import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor, execute_batch
from psycopg2.pool import ThreadedConnectionPool

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}

BASE_URL = "https://prd.location.enterprise.com/enterprise-sls/search/location/alamo/web/text/{ss}"

COUNTRY_CONFIG = {
    # Keep exactly one country uncommented for each run.
    # After it finishes, comment it and uncomment the next country before rerunning.
    # "GB": {"countryCode": "GB", "cor": "GB", "locale": "en_GB", "domain": "enterprise.co.uk"},
    # "US": {"countryCode": "US", "cor": "US", "locale": "en_US", "domain": "enterprise.com"},
    "DE": {"countryCode": "DE", "cor": "DE", "locale": "de_DE", "domain": "enterprise.de"},
    # "FR": {"countryCode": "FR", "cor": "FR", "locale": "fr_FR", "domain": "enterprise.fr"},
    # "ES": {"countryCode": "ES", "cor": "ES", "locale": "es_ES", "domain": "enterprise.es"},
    # "IT": {"countryCode": "IT", "cor": "IT", "locale": "it_IT", "domain": "enterprise.it"},
    # "DK": {"countryCode": "DK", "cor": "DK", "locale": "da_DK", "domain": "enterprise.dk"},
    # "IE": {"countryCode": "IE", "cor": "IE", "locale": "en_IE", "domain": "enterprise.ie"},
    # "NL": {"countryCode": "NL", "cor": "NL", "locale": "nl_NL", "domain": "enterprise.nl"},
    # "BE": {"countryCode": "BE", "cor": "BE", "locale": "nl_BE", "domain": "enterprise.be"},
    # "AT": {"countryCode": "AT", "cor": "AT", "locale": "de_AT", "domain": "enterprise.at"},
    # "CH": {"countryCode": "CH", "cor": "CH", "locale": "de_CH", "domain": "enterprise.ch"},
    # "PT": {"countryCode": "PT", "cor": "PT", "locale": "pt_PT", "domain": "enterprise.pt"},
    # "SE": {"countryCode": "SE", "cor": "SE", "locale": "sv_SE", "domain": "enterprise.se"},
    # "NO": {"countryCode": "NO", "cor": "NO", "locale": "nb_NO", "domain": "enterprise.no"},
    # "FI": {"countryCode": "FI", "cor": "FI", "locale": "fi_FI", "domain": "enterprise.fi"},
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
    """Return a normalized city name, with country when geonamescache is available."""
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


def build_input_data(target_terms=None, city_id_from=None, city_id_to=None):
    """Build one API-search row per airport record for the enabled country."""
    if len(COUNTRY_CONFIG) != 1:
        raise RuntimeError(
            "Enable exactly one country in COUNTRY_CONFIG before starting a run."
        )

    import airportsdata
    airports = airportsdata.load("IATA")

    target_terms = {
        term.strip().upper() for term in (target_terms or []) if term.strip()
    }
    processed_cities = set()
    if os.path.exists("processed_iatas.txt"):
        with open("processed_iatas.txt", "r", encoding="utf-8") as f:
            for line in f:
                processed_cities.add(line.strip())

    rows = []
    for iata, data in airports.items():
        city_name = data.get("city", "")
        if not iata:
            continue
        if iata in processed_cities:
            continue
        if target_terms and iata.upper() not in target_terms:
            continue
        # Like Expedia, search every airport through the enabled booking country.
        for country, config in COUNTRY_CONFIG.items():
            rows.append({
                "city_id":        iata,
                "ss":             iata,
                "domain":         config["domain"],
                "bookingcountry": country,
                "city":           city_name,
                "airport_name":   data.get("name", ""),
            })
    rows.sort(key=lambda r: (r["bookingcountry"], r["city_id"]))
    return rows


class alamo:
    def __init__(
        self, status, startid, endid, inputtable, outputtable, offline, proxyid,
        max_workers=10, target_terms=None,
    ):
        self.inputtable  = inputtable
        self.outputtable = outputtable
        self.startid     = startid
        self.endid       = endid
        self.proxyid     = proxyid
        self.websitecode = 6           # ← update to Enterprise's actual websitecode
        self.max_workers = max_workers
        self.target_terms = target_terms or []

        self.db_pool = ThreadedConnectionPool(1, max_workers * 2, **DB_CONFIG)

        self.api_cache   = {}           # (ss, bookingcountry) → full typeahead response
        self.cache_lock  = threading.Lock()
        self.failed_requests = []
        self.failure_lock = threading.Lock()
        self.seen_lock   = threading.Lock()
        self.rows_lock   = threading.Lock()

        conn = self.db_pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    f"SELECT proxy FROM proxy_list WHERE status IN ({self.proxyid})"
                )
                self.proxyset = cursor.fetchall()
        
                self.length_limits = self._get_length_limits(cursor)
                self.known_location_keys = self._load_existing_location_keys(cursor)
        
                if str(status).strip().lower() == "any":
                    cursor.execute(
                        f"""
                        SELECT * FROM {self.inputtable}
                        WHERE websitecode = %s AND id BETWEEN %s AND %s
                        """,
                        (str(self.websitecode), startid, endid),
                    )
                else:
                    cursor.execute(
                        f"""
                        SELECT * FROM {self.inputtable}
                        WHERE websitecode = %s AND status = %s AND id BETWEEN %s AND %s
                        """,
                        (str(self.websitecode), status, startid, endid),
                    )
                resultset = cursor.fetchall()
        finally:
            self.db_pool.putconn(conn)

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
    def RandUA(self, chrome_major):
        chrome_build = (
            f"{chrome_major}.0.{random.randint(6000, 7300)}.{random.randint(0, 200)}"
        )
        return (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            f"(KHTML, like Gecko) Chrome/{chrome_build} Safari/537.36"
        )

    def make_headers(self, bookingcountry):
        cfg    = COUNTRY_CONFIG[bookingcountry]
        domain = cfg["domain"]
        locale = cfg["locale"].replace("_", "-")
        chrome_major = random.randint(120, 141)
        return {
            "accept":             "application/json, text/plain, */*",
            "accept-language":    f"{locale},{locale.split('-')[0]};q=0.9",
            "origin":             f"https://www.{domain}",
            "referer":            f"https://www.{domain}/",
            "sec-ch-ua":          f'"Chromium";v="{chrome_major}", "Not=A?Brand";v="24", "Google Chrome";v="{chrome_major}"',
            "sec-ch-ua-mobile":   "?0",
            "sec-ch-ua-platform": '"Linux"',
            "sec-fetch-dest":     "empty",
            "sec-fetch-mode":     "cors",
            "sec-fetch-site":     "same-site",
            "user-agent":         self.RandUA(chrome_major),
        }

    def load(self, ss, bookingcountry, proxies):
        cfg = COUNTRY_CONFIG[bookingcountry]
        params = {
            "countryCode":    cfg["countryCode"],
            "includeExotics": "true",
            "brand":          "ALAMO",
            "dto":            "true",
            "cor":            cfg["cor"],
            "locale":         cfg["locale"],
        }
        # print("headers")
        # print(self.make_headers(bookingcountry))
        # print("params")
        # print(params)
        # print("BASE_URL")
        # print(BASE_URL.format(ss=ss))
        return requests.get(
            BASE_URL.format(ss=ss),
            params=params,
            headers=self.make_headers(bookingcountry),
            proxies=proxies,
            timeout=30,
        )
        

    # ── DB ────────────────────────────────────────────────────────────────────
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

    def _load_existing_location_keys(self, cursor):
        """Load rows already stored for this website, so reruns do not duplicate them."""
        booking_country = list(COUNTRY_CONFIG.keys())[0] if COUNTRY_CONFIG else ""
        cursor.execute(
            f"""
            SELECT location_name, location_code
            FROM {self.outputtable}
            WHERE website_code = %s
              AND booking_country = %s
              AND location_name IS NOT NULL
              AND location_code IS NOT NULL
            """,
            (self.websitecode, booking_country),
        )
        keys = {
            (str(row["location_name"]), str(row["location_code"]))
            for row in cursor.fetchall()
            if row["location_name"] not in (None, "")
            and row["location_code"] not in (None, "")
        }
        print("Existing Alamo locations loaded:", len(keys))
        return keys

    def reserve_location_key(self, location_name, locationcode):
        """Reserve a key before inserting so worker threads cannot insert it twice."""
        key = (str(location_name), str(locationcode))
        with self.seen_lock:
            if key in self.known_location_keys:
                return False
            self.known_location_keys.add(key)
        return True

    def release_location_key(self, location_name, locationcode):
        """Allow another result to try again if this location's insert failed."""
        key = (str(location_name), str(locationcode))
        with self.seen_lock:
            self.known_location_keys.discard(key)

    def insert_many(self, rows_list):
        """Insert multiple completed locations in a single batch query."""
        if not rows_list:
            return True
            
        columns = [c for c in rows_list[0] if c != "id"]
        colnames = ",".join(columns)
        placeholders = ",".join(["%s"] * len(columns))
        sql = f"INSERT INTO {self.outputtable} ({colnames}) VALUES ({placeholders})"

        values_list = []
        for row in rows_list:
            value_row = []
            for col in columns:
                value = row.get(col)
                max_len = self.length_limits.get(col)
                if isinstance(value, str) and max_len and len(value) > max_len:
                    value = value[:max_len]
                value_row.append(value)
            values_list.append(tuple(value_row))

        conn = self.db_pool.getconn()
        try:
            with conn.cursor() as cursor:
                execute_batch(cursor, sql, values_list)
                conn.commit()
                print(f"BULK INSERTED {len(rows_list)} locations successfully.")
                return True
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"BULK INSERT FAILED for {len(rows_list)} locations")
            self.eHandling()
            return False
        finally:
            self.db_pool.putconn(conn)

    def update(self, upstatus, refid):
        updateq = f"UPDATE {self.inputtable} SET status=%s WHERE id=%s"
        self._execute_commit(updateq, (upstatus, refid))
        print(self.websitecode, "updated as", upstatus, "for id", refid)

    def conn_close(self):
        try:
            self.db_pool.closeall()
        except Exception:
            pass

    def eHandling(self):
        import traceback
        traceback.print_exc()

    def _execute_commit(self, query, params=None):
        conn = self.db_pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            self.db_pool.putconn(conn)

    # ── ENTERPRISE API HELPERS ───────────────────────────────────────────────
    def fetch_location_list(self, ss, bookingcountry, proxies):
        ck = (ss, bookingcountry)

        with self.cache_lock:
            if ck in self.api_cache:
                return self.api_cache[ck]

        url = BASE_URL.format(ss=ss)
        errors = []
        # Attempt once through a proxy and once directly.  HTTP errors, invalid JSON,
        # and transport errors all advance to the direct retry.
        attempts = (proxies, {})

        for attempt, current_proxies in enumerate(attempts, start=1):
            try:
                resp = self.load(ss, bookingcountry, current_proxies)
                print(
                    "Status:", resp.status_code,
                    "| Input_location:", ss,
                    "| country:", bookingcountry,
                    "| attempt:", attempt,
                )
                resp.raise_for_status()
                result = resp.json()
                if not isinstance(result, dict):
                    raise ValueError("Alamo response is not a JSON object")

                with self.cache_lock:
                    self.api_cache[ck] = result

                time.sleep(0.3)
                return result
            except Exception as exc:
                errors.append(f"attempt {attempt}: {exc}")
                if attempt == 1:
                    print("Request failed; retrying without proxy:", url, "| IATA:", ss)

        failure = {
            "iata": ss,
            "bookingcountry": bookingcountry,
            "url": url,
            "error": " | ".join(errors),
        }
        with self.failure_lock:
            self.failed_requests.append(failure)

        raise RuntimeError(
            f"All request attempts failed for {url} | IATA: {ss}: {failure['error']}"
        )

    def iter_locations(self, response_data):
        """Yield every location category returned by Alamo's typeahead endpoint."""
        location_groups = {
            "airports": "Airport",
            "cities": "City",
            "branches": "Branch",
            "trucks": "Truck",
            "portsOfCall": "Port of Call",
            "railStations": "Rail Station",
        }
        for response_key, default_type in location_groups.items():
            locations = response_data.get(response_key) or []
            if not isinstance(locations, list):
                continue
            for location in locations:
                if isinstance(location, dict):
                    yield location, default_type

    @staticmethod
    def get_first(data, *keys, default=""):
        if not isinstance(data, dict):
            return default
        for key in keys:
            value = data.get(key)
            if value not in (None, ""):
                return value
        return default

    def make_location_code(self, location, default_type):
        """Use Alamo's ID; create a stable fallback only if it is absent."""
        location_code = self.get_first(
            location, "id", "station_id", "stationId", "location_id", "locationId"
        )
        if location_code not in (None, ""):
            return str(location_code)

        address = location.get("address") or {}
        fingerprint = "|".join(
            str(value)
            for value in (
                default_type,
                location.get("name", ""),
                address.get("city", ""),
                address.get("country_code", ""),
            )
        )
        return f"generated-{sha256(fingerprint.encode('utf-8')).hexdigest()[:24]}"

    def build_location_details(self, location, default_type):
        """Map one Alamo response item to the shared locations-table fields."""
        address = location.get("address") or {}
        additional_data = location.get("additional_data") or {}
        gps = location.get("gps") or {}
        latitude = self.get_first(gps, "latitude", "lat")
        longitude = self.get_first(gps, "longitude", "lon", "long")
        location_name = self.get_first(
            location,
            "name",
            default=self.get_first(additional_data, "long_name", "short_name"),
        )
        location_type = self.get_first(location, "location_type", "type", default=default_type)
        city = self.get_first(address, "city", default=self.get_first(additional_data, "short_name"))
        region = self.get_first(address, "country_subdivision_code", "country_subdivision_name")
        location_country = address.get("country_code", "")
        is_airport = default_type == "Airport" or bool(location.get("airport_code"))
        airport_code = location.get("airport_code") or self.make_location_code(location, default_type)
        return {
            "location_code": self.make_location_code(location, default_type),
            "location_name": str(location_name or ""),
            "location_type": str(location_type or default_type),
            "city": str(city or ""),
            "region": str(region or ""),
            "location_country": str(location_country or ""),
            "is_airport": is_airport,
            "airport_code": str(airport_code),
            "latitude": str(latitude),
            "longitude": str(longitude),
        }

    def _build_row(
        self,
        refid,
        websitecode,
        source_name,
        ss,
        bookingcountry,
        location_details,
        created_date,
    ):
        if location_details["is_airport"]:
            pickup_location = location_details.get("airport_code", location_details["location_code"])
        else:
            pickup_location = location_details["location_name"]

        return {
            "id": refid,
            "source_name": source_name,
            "website_code": websitecode,
            "pickup_location": pickup_location,
            "location_country": location_details["location_country"],
            "booking_country": bookingcountry,
            "location_code": location_details["location_code"],
            "is_airport": location_details["is_airport"],
            "created_date": created_date,
            "location_type": location_details["location_type"],
            "city": location_details["city"],
            "region": location_details["region"],
            "priority_level": "",
            "location_term": location_details["location_name"],
            "location_name": location_details["location_name"],
            "latitude": location_details["latitude"],
            "longitude": location_details["longitude"],
        }

    # ── EXTRACTION ────────────────────────────────────────────────────────────
    def extraction(self, item, refid, websitecode, source_name, rows):
        ss             = item["ss"]
        bookingcountry = item["bookingcountry"]

        proxies = self.get_proxy()
        local_rows = []

        # Keep every airport, city, branch, truck, port, and rail station returned
        # for the IATA search; do not restrict results to the matching airport only.
        response_data = self.fetch_location_list(ss, bookingcountry, proxies)
        # print(response_data)
        for location, default_type in self.iter_locations(response_data):
            location_details = self.build_location_details(location, default_type)
            
            if not location_details["is_airport"] or location_details.get("airport_code", "").upper() != ss.upper():
                continue

            locationcode = location_details["location_code"]
            locationname = location_details["location_name"]

            if not self.reserve_location_key(locationname, locationcode):
                print(
                    "DUPLICATE SKIPPED | IATA:", ss,
                    "| country:", bookingcountry,
                    "| code:", locationcode,
                )
                # A pre-existing row is a successful result for this input record.
                with self.rows_lock:
                    rows.append({"location_code": locationcode})
                continue

            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            row = self._build_row(
                refid,
                websitecode,
                source_name,
                ss,
                bookingcountry,
                location_details,
                created_date,
            )
            # print(row)
            local_rows.append(row)
            with self.rows_lock:
                rows.append(row)
                
        return local_rows

    # ── MAIN ──────────────────────────────────────────────────────────────────
    def main(self, resultset):
        input_data = build_input_data(self.target_terms)
        if self.target_terms:
            print(
                "Target retry terms:",
                ", ".join(sorted({term.upper() for term in self.target_terms})),
            )

        for result in resultset:
            refid       = result["id"]
            websitecode = result["websitecode"]
            source_name = result["source_name"]

            rows = []
            try:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = {
                        executor.submit(
                            self.extraction, item, refid, websitecode, source_name,
                            rows,
                        ): item
                        for item in input_data
                    }
                    
                    batch_rows = []
                    batch_city_ids = []
                    
                    for future in as_completed(futures):
                        item = futures[future]
                        try:
                            result_rows = future.result()
                            if result_rows:
                                batch_rows.extend(result_rows)
                            batch_city_ids.append(item["city_id"])
                            
                            if len(batch_city_ids) >= 50:
                                if self.insert_many(batch_rows):
                                    with open("processed_iatas.txt", "a", encoding="utf-8") as f:
                                        for cid in batch_city_ids:
                                            f.write(str(cid) + "\n")
                                else:
                                    print("Failed to bulk insert 50 cities.")
                                
                                batch_rows = []
                                batch_city_ids = []
                                
                        except Exception:
                            self.eHandling()
                            
                    if batch_city_ids:
                        if self.insert_many(batch_rows):
                            with open("processed_iatas.txt", "a", encoding="utf-8") as f:
                                for cid in batch_city_ids:
                                    f.write(str(cid) + "\n")
                if rows:
                    self.update(1, refid)
                else:
                    continue

            except Exception:
                self.eHandling()
                self.update(2, refid)

    def print_failures(self):
        if self.failed_requests:
            print(f"\nSaving {len(self.failed_requests)} FAILED REQUESTS to failed_requests.log...")
            with open("failed_requests.log", "a", encoding="utf-8") as f:
                for failure in self.failed_requests:
                    log_line = f"- {failure['url']} | IATA: {failure['iata']} | country: {failure['bookingcountry']} | error: {failure['error']}\n"
                    f.write(log_line)
                    print(log_line.strip())
            # Clear the list so we don't write them twice if called multiple times
            self.failed_requests = []

# ── ENTRY POINT ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    STATUS = "0"
    STARTID = 171
    ENDID = 171
    INPUTTABLE = "input_locations"
    OUTPUTTABLE = "locations"
    PROXYID = "60"
    MAX_WORKERS = 10

    # Set to 1 to run only the IATA codes listed below, regardless of DB status.
    RUN_MISSING_ONLY = 0
    MISSING_IATA_TERMS = []

    target_terms = MISSING_IATA_TERMS if RUN_MISSING_ONLY else []
    if RUN_MISSING_ONLY:
        STATUS = "any"

    EC = None
    try:
        EC = alamo(
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

    except BaseException as e:
        if EC:
            EC.eHandling()
        else:
            exc_type, exc_obj, tb = sys.exc_info()
            print("Startup error:", exc_obj)
    finally:
        if EC:
            EC.print_failures()
            EC.conn_close()
    time.sleep(3)
