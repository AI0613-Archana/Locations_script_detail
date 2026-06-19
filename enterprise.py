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
import requests
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

BASE_URL = "https://prd.location.enterprise.com/enterprise-sls/search/location/enterprise/web/text/{ss}"

COUNTRY_CONFIG = {
    "GB": {"countryCode": "GB", "cor": "GB", "locale": "en_GB", "domain": "enterprise.co.uk"},
    "US": {"countryCode": "US", "cor": "US", "locale": "en_US", "domain": "enterprise.com"},
    "DE": {"countryCode": "DE", "cor": "DE", "locale": "de_DE", "domain": "enterprise.de"},
    "FR": {"countryCode": "FR", "cor": "FR", "locale": "fr_FR", "domain": "enterprise.fr"},
    "ES": {"countryCode": "ES", "cor": "ES", "locale": "es_ES", "domain": "enterprise.es"},
    "IT": {"countryCode": "IT", "cor": "IT", "locale": "it_IT", "domain": "enterprise.it"},
    "DK": {"countryCode": "DK", "cor": "DK", "locale": "da_DK", "domain": "enterprise.dk"},
    "IE": {"countryCode": "IE", "cor": "IE", "locale": "en_IE", "domain": "enterprise.ie"},
    "NL": {"countryCode": "NL", "cor": "NL", "locale": "nl_NL", "domain": "enterprise.nl"},
    "BE": {"countryCode": "BE", "cor": "BE", "locale": "nl_BE", "domain": "enterprise.be"},
    "AT": {"countryCode": "AT", "cor": "AT", "locale": "de_AT", "domain": "enterprise.at"},
    "CH": {"countryCode": "CH", "cor": "CH", "locale": "de_CH", "domain": "enterprise.ch"},
    "PT": {"countryCode": "PT", "cor": "PT", "locale": "pt_PT", "domain": "enterprise.pt"},
    "SE": {"countryCode": "SE", "cor": "SE", "locale": "sv_SE", "domain": "enterprise.se"},
    "NO": {"countryCode": "NO", "cor": "NO", "locale": "nb_NO", "domain": "enterprise.no"},
    "FI": {"countryCode": "FI", "cor": "FI", "locale": "fi_FI", "domain": "enterprise.fi"},
}


def build_input_data():
    """Build (ss, domain, bookingcountry, city, airport_name) rows from airportsdata."""
    airports_db = airportsdata.load("IATA")
    rows = []
    for iata, v in sorted(airports_db.items()):
        country = v["country"]
        if country not in COUNTRY_CONFIG:
            continue
        rows.append({
            "ss":             iata,
            "domain":         COUNTRY_CONFIG[country]["domain"],
            "bookingcountry": country,
            "city":           v["city"],
            "airport_name":   v["name"],
        })
    rows.sort(key=lambda r: (r["bookingcountry"], r["ss"]))
    return rows


class enterprise:
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
        self.websitecode = 27          # ← update to Enterprise's actual websitecode
        self.max_workers = max_workers

        self.api_cache   = {}           # (ss, bookingcountry) → airports list
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
        cfg    = COUNTRY_CONFIG.get(bookingcountry, COUNTRY_CONFIG["US"])
        domain = cfg["domain"]
        return {
            "accept":             "application/json, text/plain, */*",
            "accept-language":    "en-GB,en;q=0.9",
            "origin":             f"https://www.{domain}",
            "referer":            f"https://www.{domain}/",
            "sec-ch-ua":          '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            "sec-ch-ua-mobile":   "?0",
            "sec-ch-ua-platform": '"Linux"',
            "sec-fetch-dest":     "empty",
            "sec-fetch-mode":     "cors",
            "sec-fetch-site":     "same-site",
            "user-agent":         "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                                  "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        }

    def load(self, ss, bookingcountry, proxies):
        cfg = COUNTRY_CONFIG.get(bookingcountry, COUNTRY_CONFIG["US"])
        params = {
            "countryCode":    cfg["countryCode"],
            "includeExotics": "true",
            "brand":          "ENTERPRISE",
            "dto":            "true",
            "cor":            cfg["cor"],
            "locale":         cfg["locale"],
        }
        return requests.get(
            BASE_URL.format(ss=ss),
            params=params,
            headers=self.make_headers(bookingcountry),
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

    # ── ENTERPRISE API HELPERS ───────────────────────────────────────────────
    def fetch_location_list(self, ss, bookingcountry, proxies):
        ck = (ss, bookingcountry)

        with self.cache_lock:
            if ck in self.api_cache:
                return self.api_cache[ck]

        try:
            resp = self.load(ss, bookingcountry, proxies)
            print("Status:", resp.status_code, "| ss:", ss, "| country:", bookingcountry)
        except Exception as exc:
            print("Proxy failed:", proxies.get("https", ""), "error:", exc)
            resp = self.load(ss, bookingcountry, {})
            print("Status:", resp.status_code, "(no proxy)")

        resp.raise_for_status()
        result = resp.json().get("airports", [])

        with self.cache_lock:
            self.api_cache[ck] = result

        time.sleep(0.3)
        return result

    def find_match(self, airports, ss):
        """
        Priority:
          1. airport_code == ss   (exact IATA match)
          2. name contains ss     (loose match)
          3. first result         (fallback)
        """
        for a in airports:
            if (a.get("airport_code") or "").upper() == ss.upper():
                return a.get("id", ""), a.get("name", "")

        for a in airports:
            if ss.upper() in (a.get("name") or "").upper():
                return a.get("id", ""), a.get("name", "")

        if airports:
            return airports[0].get("id", ""), airports[0].get("name", "")

        return None, None

    # ── EXTRACTION ────────────────────────────────────────────────────────────
    def extraction(
        self, item, refid, websitecode, source_name, rows, seen_location_codes
    ):
        ss             = item["ss"]
        bookingcountry = item["bookingcountry"]
        city           = item.get("city", "")

        proxies = self.get_proxy()

        # Attempt 1: IATA + booking country
        airports = self.fetch_location_list(ss, bookingcountry, proxies)
        locationcode, locationterm = self.find_match(airports, ss)

        # Attempt 2: fallback country (GB/US) if not found
        if not locationcode:
            fallback = "GB" if bookingcountry != "GB" else "US"
            airports = self.fetch_location_list(ss, fallback, proxies)
            locationcode, locationterm = self.find_match(airports, ss)

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
            "is_airport":       True,        # endpoint only returns airport stations
            "created_date":     created_date,
            "location_type":    "Airport",
            "city":             city,
            "region":           "",
            "priority_level":   "",
            "location_term":    locationterm or "",
            "location_name":    locationterm or "",
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
    EC = None
    try:
        EC = enterprise(1, 169, 169, "input_locations", "locations", False, "20", max_workers=10)

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
        # EC = enterprise(
        #     status,
        #     startid,
        #     endid,
        #     inputtable,
        #     outputtable,
        #     offline,
        #     proxyid,
        # )
    except Exception:
        if EC:
            EC.eHandling()
        else:
            exc_type, exc_obj, tb = sys.exc_info()
            print("Startup error:", exc_obj)
    finally:
        if EC:
            EC.conn_close()
    time.sleep(3)