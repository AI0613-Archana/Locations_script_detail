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

COUNTRY_CONFIG = {
    # ISO2: (domain, bookingcountry)
    "AT": ("expedia.at",     "AT"),
    "AU": ("expedia.com.au", "AU"),
    "BR": ("expedia.com.br", "BR"),
    "CA": ("expedia.ca",     "CA"),
    "CH": ("expedia.ch",     "CH"),
    "DE": ("expedia.de",     "DE"),
    "DK": ("expedia.dk",     "DK"),
    "ES": ("expedia.es",     "ES"),
    "FI": ("expedia.fi",     "FI"),
    "FR": ("expedia.fr",     "FR"),
    "GB": ("expedia.co.uk",  "GB"),
    "IE": ("expedia.ie",     "IE"),
    "IT": ("expedia.it",     "IT"),
    "JP": ("expedia.co.jp",  "JP"),
    "MX": ("expedia.mx",     "MX"),
    "NL": ("expedia.nl",     "NL"),
    "NO": ("expedia.no",     "NO"),
    "NZ": ("expedia.co.nz",  "NZ"),
    "SE": ("expedia.se",     "SE"),
    "SG": ("expedia.com.sg", "SG"),
    "TH": ("expedia.co.th",  "TH"),
    "US": ("expedia.com",    "US"),
}

LOCALE_MAP = {
    "AT": "de-AT,de;q=0.9",
    "AU": "en-AU,en;q=0.9",
    "BR": "pt-BR,pt;q=0.9",
    "CA": "en-CA,en;q=0.9",
    "CH": "de-CH,de;q=0.9",
    "DE": "de-DE,de;q=0.9",
    "DK": "da-DK,da;q=0.9",
    "ES": "es-ES,es;q=0.9",
    "FI": "fi-FI,fi;q=0.9",
    "FR": "fr-FR,fr;q=0.9",
    "GB": "en-GB,en;q=0.9",
    "IE": "en-IE,en;q=0.9",
    "IT": "it-IT,it;q=0.9",
    "JP": "ja-JP,ja;q=0.9",
    "MX": "es-MX,es;q=0.9",
    "NL": "nl-NL,nl;q=0.9",
    "NO": "nb-NO,nb;q=0.9",
    "NZ": "en-NZ,en;q=0.9",
    "SE": "sv-SE,sv;q=0.9",
    "SG": "en-SG,en;q=0.9",
    "TH": "th-TH,th;q=0.9",
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


class expedia:
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
        self.websitecode = 1          # TODO: set Expedia's actual websitecode
        self.max_workers = max_workers

        self.api_cache   = {}           # (term, bookingcountry) -> raw API response
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

    def RandUA(self, chrome_major):
        """Desktop Linux Chrome UA, matching the sec-ch-ua / platform headers below."""
        chrome_build = f"{chrome_major}.0.{random.randint(6000, 7300)}.{random.randint(0, 200)}"
        return (
            f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{chrome_build} Safari/537.36"
        )

    # -- HTTP -----------------------------------------------------------------
    def make_headers(self, bookingcountry):
        domain  = COUNTRY_CONFIG[bookingcountry][0]
        locale  = LOCALE_MAP.get(bookingcountry, "en-US,en;q=0.9")
        referer = f"https://www.{domain}/"
        chrome_major = random.randint(120, 141)
        return {
            "accept":             "application/json, text/plain, */*",
            "accept-language":    locale,
            "origin":             f"https://www.{domain}",
            "referer":            referer,
            "sec-ch-ua":          f'"Chromium";v="{chrome_major}", "Not=A?Brand";v="24", "Google Chrome";v="{chrome_major}"',
            "sec-ch-ua-mobile":   "?0",
            "sec-ch-ua-platform": '"Linux"',
            "sec-fetch-dest":     "empty",
            "sec-fetch-mode":     "cors",
            "sec-fetch-site":     "same-origin",
            "user-agent":         self.RandUA(chrome_major),
        }

    def load(self, term, bookingcountry, proxies, url):
        headers = self.make_headers(bookingcountry)
        return requests.get(
            url,
            headers=headers,
            proxies=proxies,
            timeout=30,
        )

    # -- DB ---------------------------------------------------------------------
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

    # -- EXPEDIA API HELPERS -----------------------------------------------------
    def get_first(self, d, *keys, default=""):
        """Fallback-key reader: returns the first present, non-empty value among keys."""
        if not isinstance(d, dict):
            return default
        for k in keys:
            v = d.get(k)
            if v not in (None, ""):
                return v
        return default

    def fetch_location_list(self, term, bookingcountry, proxies, url):
        ck = (term, bookingcountry)

        with self.cache_lock:
            if ck in self.api_cache:
                return self.api_cache[ck]

        try:
            resp = self.load(term, bookingcountry, proxies, url)
            print("Status:", resp.status_code, "| term:", term, "| country:", bookingcountry)
        except Exception as exc:
            print("Proxy failed:", proxies.get("https", ""), "error:", exc)
            resp = self.load(term, bookingcountry, {}, url)
            print("Status:", resp.status_code, "(no proxy)")

        resp.raise_for_status()
        data = resp.json()

        with self.cache_lock:
            self.api_cache[ck] = data

        time.sleep(0.3)
        return data

    # -- EXTRACTION ----------------------------------------------------------
    def extraction(self, item, refid, websitecode, source_name, rows, seen_location_codes):
        ss             = item["ss"]
        domain         = item["domain"]
        bookingcountry = item["bookingcountry"]
        city           = item["city"]

        proxies        = self.get_proxy()
        url            = f"https://{domain}/api/v4/typeahead/{ss}"

        location_list = self.fetch_location_list(ss, bookingcountry, proxies, url)

        if not location_list.get("sr"):
            location_list = self.fetch_location_list(city, bookingcountry, proxies, url)

        created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for i in location_list.get("sr", []):
            loctype      = i.get("type")
            region_names = i.get("regionNames") or {}
            term         = region_names.get("fullName", "")
            region       = region_names.get("shortName", "")

            # Match only results that contain the IATA code in parentheses
            if f"({ss}-" in term or f"({ss})" in term:
                ess_id       = i.get("essId") or {}
                locationcode = str(ess_id.get("sourceId", ""))
                if not locationcode:
                    continue

                with self.seen_lock:
                    if locationcode in seen_location_codes:
                        return
                    seen_location_codes.add(locationcode)

                row = {
                    "id":               refid,
                    "source_name":      source_name,
                    "website_code":     websitecode,
                    "pickup_location":  ss,
                    "location_country": bookingcountry,
                    "location_code":    locationcode,
                    "is_airport":       True,
                    "created_date":     created_date,
                    "location_type":    loctype,
                    "city":             city,
                    "region":           region,
                    "priority_level":   "",
                    "location_term":    bookingcountry,
                    "location_name":    term,
                }
                with self.rows_lock:
                    rows.append(row)
                return

    # -- MAIN -------------------------------------------------------------------
    def main(self, resultset):
        input_data = build_input_data()

        for result in resultset:
            refid       = result["id"]
            websitecode = result["websitecode"]
            source_name = result["source_name"]
            rows = []
            seen_location_codes = set()
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
                        continue
                        # self.update(2, refid)

            except Exception:
                self.eHandling()
                self.update(2, refid)


# -- ENTRY POINT -----------------------------------------------------------------
if __name__ == "__main__":
    SC = None
    try:
        SC = expedia(2, 196, 196, "input_locations", "locations", False, "20", max_workers=10)

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
        # SC = expedia(
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