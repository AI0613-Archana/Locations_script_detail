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

WORDWHEEL_URL = "https://loc.hertz.com/locations/WordWheel"

DIALECT_MAP = {
    "AT": "deAT",
    "BE": "nlNL",
    "BG": "bgBG",
    "CH": "deCH",
    "CN": "zhCN",
    "CZ": "csCZ",
    "DE": "deDE",
    "DK": "daDK",
    "EE": "etEE",
    "ES": "esES",
    "FI": "fiFI",
    "FR": "frFR",
    "GB": "enGB",
    "GR": "elGR",
    "HR": "hrHR",
    "IE": "enIE",
    "IN": "enIN",
    "IT": "itIT",
    "JO": "enJO",
    "LV": "lvLV",
    "MT": "enMT",
    "MU": "enMU",
    "NL": "nlNL",
    "NO": "nbNO",
    "QA": "enQA",
    "RO": "roRO",
    "RS": "srRS",
    "RU": "ruRU",
    "SA": "enSA",
    "SE": "svSE",
    "SG": "enSG",
    "SI": "slSI",
    "TH": "thTH",
    "TN": "frTN",
    "UA": "ukUA",
    "US": "enUS",
}


class hertz_2:
    def __init__(
        self,
        status,
        startid,
        endid,
        inputtable,
        outputtable,
        offline,
        proxyid,
        max_workers=50,
        target_terms=None,
    ):
        self.inputtable = inputtable
        self.outputtable = outputtable
        self.startid = startid
        self.endid = endid
        self.proxyid = proxyid
        self.max_workers = int(max_workers)
        self.target_terms = [
            term.strip().upper() for term in (target_terms or []) if term.strip()
        ]
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        self.websitecode = 37
        self.is_dc_input = False
        self.iata_codes = set(airportsdata.load("IATA").keys())
        self.active_iata_codes = self.build_iata_codes()
        
        self.db_lock = threading.Lock()
        self.rows_lock = threading.Lock()
        self.seen_lock = threading.Lock()
        self.length_limits = self._get_length_limits(self.cursor)

        self.cursor.execute(
            f"SELECT proxy FROM proxy_list WHERE status IN ({self.proxyid})"
        )
        self.proxyset = self.cursor.fetchall()

        if str(status).strip().lower() == "any":
            self.cursor.execute(
                f"""
                SELECT * FROM {self.inputtable}
                WHERE websitecode = %s AND id BETWEEN %s AND %s
                ORDER BY id
                """,
                (self.websitecode, startid, endid),
            )
        else:
            self.cursor.execute(
                f"""
                SELECT * FROM {self.inputtable}
                WHERE websitecode = %s AND status = %s AND id BETWEEN %s AND %s
                ORDER BY id
                """,
                (self.websitecode, status, startid, endid),
            )
        resultset = self.cursor.fetchall()
        self.main(resultset)

    def build_iata_codes(self):
        if not self.target_terms:
            return sorted(self.iata_codes)

        valid_terms = []
        invalid_terms = []
        for term in self.target_terms:
            if term in self.iata_codes:
                valid_terms.append(term)
            else:
                invalid_terms.append(term)

        if invalid_terms:
            print("Skipping invalid IATA terms:", ", ".join(sorted(invalid_terms)))

        print("Target retry terms:", ", ".join(sorted(valid_terms)))
        return sorted(set(valid_terms))

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

    def normalize_domain(self, domain):
        domain = self.clean_text(domain)
        domain = re.sub(r"^https?://", "", domain, flags=re.I).strip("/")
        return domain

    def load(self, source_url, headers, params, proxies):
        return requests.get(
            source_url,
            params=params,
            headers=headers,
            proxies=proxies,
            timeout=30,
        )

    def build_headers(self, domain):
        domain = self.normalize_domain(domain)
        chrome_major = 151
        return {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "referer": f"https://{domain}/",
            "sec-ch-ua": (
                f'"Not=A?Brand";v="99", "Google Chrome";v="{chrome_major}", '
                f'"Chromium";v="{chrome_major}"'
            ),
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
            "sec-fetch-dest": "script",
            "sec-fetch-mode": "no-cors",
            "sec-fetch-site": "cross-site",
            "sec-fetch-storage-access": "active",
            "user-agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                f"(KHTML, like Gecko) Chrome/{chrome_major}.0.0.0 Safari/537.36"
            ),
        }

    def get_dialect(self, country_code):
        country_code = self.clean_text(country_code).upper()
        return DIALECT_MAP.get(country_code, f"en{country_code}")

    def fetch_location_response(self, country_code, domain, iata):
        params = {
            "callback": "parse",
            "dialect": self.get_dialect(country_code),
            "systemId": "IRAC",
            "subSystemId": "IRAC",
            "searchText": iata.lower(),
        }
        headers = self.build_headers(domain)
        proxies = self.get_proxy()
        try:
            response = self.load(WORDWHEEL_URL, headers, params, proxies)
            print(country_code, "Query:", iata, "status:", response.status_code)
            return iata, response.status_code, response.text
        except Exception as exc:
            print(
                country_code,
                "Query:",
                iata,
                "proxy failed:",
                proxies.get("https", ""),
                "error:",
                exc,
            )
            response = self.load(WORDWHEEL_URL, headers, params, {})
            print(
                country_code,
                "Query:",
                iata,
                "status:",
                response.status_code,
                "without proxy",
            )
            return iata, response.status_code, response.text

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

    def clean_text(self, value):
        return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()

    def parse_jsonp(self, text):
        text = (text or "").strip()
        match = re.match(r"^[^(]*\((.*)\)\s*;?\s*$", text, flags=re.S)
        if match:
            text = match.group(1)
        return json.loads(text)

    def extract_iata(self, location):
        display_text = self.clean_text(location.get("displayText"))
        preferred_oag = self.clean_text(location.get("preferredOag")).upper()

        prefix_match = re.match(r"^([A-Z]{3})\s*,", display_text)
        if prefix_match and prefix_match.group(1) in self.iata_codes:
            return prefix_match.group(1)

        suffix_match = re.search(r"\(([A-Z]{3})\)\s*$", display_text)
        if suffix_match and suffix_match.group(1) in self.iata_codes:
            return suffix_match.group(1)

        if len(preferred_oag) >= 3 and preferred_oag[:3] in self.iata_codes:
            return preferred_oag[:3]

        return ""

    def should_process_result(self, result):
        source_url = self.clean_text(result.get("source_url"))
        if not source_url:
            return True
        return "loc.hertz.com/locations/WordWheel" in source_url

    def main(self, resultset):
        if not resultset:
            print("No input rows found.")
            return

        for result in resultset:
            print(result)
            if not self.should_process_result(result):
                print(
                    "Skipping non-WordWheel Hertz row:",
                    result.get("id"),
                    result.get("country"),
                    result.get("source_url"),
                )
                continue

            refid = result["id"]
            websitecode = result["websitecode"]
            source_name = result["source_name"]
            country = self.clean_text(result["country"]).upper()
            domain = self.normalize_domain(
                result.get("domainname") or result.get("website_url")
            )
            rows = []
            seen_location_codes = set()

            try:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(
                            self.fetch_location_response,
                            country,
                            domain,
                            iata,
                        )
                        for iata in self.active_iata_codes
                    ]
                    for future in as_completed(futures):
                        iata, status_code, response_text = future.result()
                        if status_code == 200:
                            self.extraction(
                                response_text,
                                refid,
                                country,
                                websitecode,
                                source_name,
                                rows,
                                seen_location_codes,
                            )

                print("Extracted:", len(rows), "country:", country)
                if rows:
                    self.update(1, refid)
                else:
                    self.update(2, refid)
            except Exception:
                self.eHandling()
                self.update(2, refid)

    def extraction(
        self, html, refid, country, websitecode, source_name, rows, seen_location_codes
    ):
        if not html:
            return

        response_data = self.parse_jsonp(html)
        locations = (
            response_data.get("locationList", [])
            if isinstance(response_data, dict)
            else []
        )
        if not isinstance(locations, list):
            return

        for location in locations:
            if not isinstance(location, dict):
                continue

            location_code = self.clean_text(location.get("preferredOag"))
            location_term = self.clean_text(location.get("displayText"))
            seen_key = (country, location_code)
            
            if not location_code or not location_term:
                continue

            with self.seen_lock:
                if seen_key in seen_location_codes:
                    continue
                seen_location_codes.add(seen_key)

            location_title = self.clean_text(location.get("locationTitle"))
            search_term = self.clean_text(location.get("searchTerm"))
            city = self.clean_text(location.get("city"))
            region = self.clean_text(
                location.get("stateCode") or location.get("stateName")
            )
            location_country = country
            iata_code = self.extract_iata(location)
            airport_text = " ".join(
                [location_term, location_title, search_term]
            ).lower()
            is_airport = bool(iata_code) or "airport" in airport_text
            location_type = "Airport" if is_airport else "City"
            location_name = (
                iata_code
                if is_airport and iata_code
                else location_title or search_term or location_term
            )
            pickup_location = location_name
            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            row = {
                "id": refid,
                "source_name": source_name,
                "website_code": websitecode,
                "pickup_location": pickup_location,
                "location_country": location_country,
                "location_code": location_code,
                "is_airport": is_airport,
                "created_date": created_date,
                "location_type": location_type,
                "city": city,
                "region": region,
                "priority_level": "",
                "location_term": location_term,
                "location_name": location_name,
            }

            inserted = self.insert_one(row)
            if inserted:
                with self.rows_lock:
                    rows.append(row)


if __name__ == "__main__":
    STATUS = "any"
    STARTID = 95
    ENDID = 95
    INPUTTABLE = "input_locations"
    OUTPUTTABLE = "locations"
    OFFLINE = False
    PROXYID = "60"
    MAX_WORKERS = 25

    # 0 = normal run for all IATA codes.
    # 1 = retry only the failed/missing IATA codes below.
    RUN_MISSING_ONLY = 0
    MISSING_IATA_TERMS = [
        "AUR",
        "AUS",
        "AYO",
        "AYP",
        "AYQ",
        "AYR",
    ]

    target_terms = MISSING_IATA_TERMS if RUN_MISSING_ONLY else []
    if RUN_MISSING_ONLY:
        STATUS = "any"

    SC = None
    try:
        if len(sys.argv) >= 8:
            (
                script,
                STATUS,
                STARTID,
                ENDID,
                INPUTTABLE,
                OUTPUTTABLE,
                OFFLINE,
                PROXYID,
                *extra_args,
            ) = sys.argv
            MAX_WORKERS = int(extra_args[0]) if extra_args else MAX_WORKERS

        SC = hertz_2(
            STATUS,
            STARTID,
            ENDID,
            INPUTTABLE,
            OUTPUTTABLE,
            OFFLINE,
            PROXYID,
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
