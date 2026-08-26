# -*- coding: utf-8 -*-
import json
import os
import random
import re
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import airportsdata
import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from tls_chameleon import TLSSession

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}

DOMAIN_CONFIG = {
    "BG": {"domain": "https://www.hertz.bg", "lang": "bg", "country_name": "Bulgaria"},
    "GR": {"domain": "https://www.hertz.gr", "lang": "el", "country_name": "Greece"},
    "HR": {"domain": "https://www.hertz.hr", "lang": "hr", "country_name": "Croatia"},
    "CY": {
        "domain": "https://www.hertz.com.cy",
        "lang": "el",
        "country_name": "Cyprus",
    },
    "RO": {"domain": "https://www.hertz.ro", "lang": "ro", "country_name": "Romania"},
    "RS": {"domain": "https://www.hertz.rs", "lang": "rs", "country_name": "Serbia"},
    "ME": {
        "domain": "https://www.hertz.me",
        "lang": "me",
        "country_name": "Montenegro",
    },
    "UA": {"domain": "https://www.hertz.ua", "lang": "ua", "country_name": "Ukraine"},
}

COUNTRY_TO_COB = {
    "BG": "BG",
    "BGR": "BG",
    "BULGARIA": "BG",
    "GR": "GR",
    "GRC": "GR",
    "GREECE": "GR",
    "HR": "HR",
    "HRV": "HR",
    "CROATIA": "HR",
    "CY": "CY",
    "CYP": "CY",
    "CYPRUS": "CY",
    "RO": "RO",
    "ROU": "RO",
    "ROMANIA": "RO",
    "RS": "RS",
    "SRB": "RS",
    "SERBIA": "RS",
    "ME": "ME",
    "MNE": "ME",
    "MONTENEGRO": "ME",
    "UA": "UA",
    "UKR": "UA",
    "UKRAINE": "UA",
}

DOMAIN_TO_COB = {
    "hertz.bg": "BG",
    "www.hertz.bg": "BG",
    "hertz.gr": "GR",
    "www.hertz.gr": "GR",
    "hertz.hr": "HR",
    "www.hertz.hr": "HR",
    "hertz.com.cy": "CY",
    "www.hertz.com.cy": "CY",
    "hertz.ro": "RO",
    "www.hertz.ro": "RO",
    "hertz.rs": "RS",
    "www.hertz.rs": "RS",
    "hertz.me": "ME",
    "www.hertz.me": "ME",
    "hertz.ua": "UA",
    "www.hertz.ua": "UA",
}


class hertz_3:
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

    def clean_text(self, value):
        return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()

    def resolve_domain_and_lang(
        self, country, domainname="", website_url="", source_url=""
    ):
        norm_country = self.clean_text(country).upper()
        cob = COUNTRY_TO_COB.get(norm_country, norm_country)

        candidates = [domainname, website_url, source_url]
        for candidate in candidates:
            cand = self.clean_text(candidate)
            if not cand:
                continue
            if not re.match(r"^https?://", cand, flags=re.I):
                cand = f"https://{cand.lstrip('/')}"
            try:
                host = urllib.parse.urlparse(cand).netloc.lower()
                if host in DOMAIN_TO_COB:
                    cob = DOMAIN_TO_COB[host]
                    break
            except Exception:
                pass

        if cob in DOMAIN_CONFIG:
            info = DOMAIN_CONFIG[cob]
            base_domain = info["domain"]
            lang = info["lang"]
            country_name = info["country_name"]
            return base_domain, lang, cob, country_name

        return "https://www.hertz.com.cy", "en", "CY", "Cyprus"

    def build_headers(self, base_domain, lang=None, cob=None):
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Origin": base_domain,
            "Referer": f"{base_domain}/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
            ),
            "sec-ch-ua": '"Not=A?Brand";v="99", "Google Chrome";v="151", "Chromium";v="151"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
        }

    def load(self, url, headers, json_data, proxies):
        try:
            return requests.post(
                url, headers=headers, json=json_data, proxies=proxies, timeout=30
            )
        except Exception:
            session = TLSSession(
                profile="chrome_120",
                proxies=proxies,
                on_block="none",
                max_retries=0,
            )
            return session.post(url, headers=headers, json=json_data, timeout=30)

    def fetch_location_response(self, base_domain, lang, cob, iata):
        endpoint_url = f"{base_domain}/{lang}/Resources/SearchBranchLocations"
        headers = self.build_headers(base_domain, lang, cob)
        json_data = {"term": iata.lower()}
        proxies = self.get_proxy()
        try:
            response = self.load(endpoint_url, headers, json_data, proxies)
            print(cob, "Query:", iata, "status:", response.status_code)
            return iata, response.status_code, response.text
        except Exception as exc:
            print(
                cob,
                "Query:",
                iata,
                "proxy failed:",
                proxies.get("https", ""),
                "error:",
                exc,
            )
            try:
                response = self.load(endpoint_url, headers, json_data, {})
                print(
                    cob,
                    "Query:",
                    iata,
                    "status:",
                    response.status_code,
                    "without proxy",
                )
                return iata, response.status_code, response.text
            except Exception as direct_exc:
                print(cob, "Query:", iata, "direct failed:", direct_exc)
                return iata, 0, ""

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

    def main(self, resultset):
        if not resultset:
            print("No input rows found.")
            return

        for result in resultset:
            print(result)
            refid = result["id"]
            websitecode = result["websitecode"]
            source_name = result["source_name"]
            country = self.clean_text(result.get("country")).upper()
            domainname = self.clean_text(result.get("domainname"))
            website_url = self.clean_text(result.get("website_url"))
            source_url = self.clean_text(result.get("source_url"))

            base_domain, lang, cob, country_name = self.resolve_domain_and_lang(
                country=country,
                domainname=domainname,
                website_url=website_url,
                source_url=source_url,
            )
            rows = []
            seen_location_codes = set()

            try:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(
                            self.fetch_location_response,
                            base_domain,
                            lang,
                            cob,
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
                                iata,
                                cob=cob,
                                country_name=country_name,
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
        self,
        html,
        refid,
        country,
        websitecode,
        source_name,
        rows,
        seen_location_codes,
        iata,
        cob="",
        country_name="",
    ):
        if not html:
            return

        try:
            response_data = json.loads(html)
        except Exception:
            return

        locations = []
        if isinstance(response_data, dict):
            locations = (
                response_data.get("Locations")
                or response_data.get("locations")
                or response_data.get("data")
                or []
            )
        elif isinstance(response_data, list):
            locations = response_data

        if not isinstance(locations, list):
            return

        for location in locations:
            if not isinstance(location, dict):
                continue

            location_code = self.clean_text(
                location.get("Value")
                or location.get("value")
                or location.get("LocationCode")
            )
            location_term = self.clean_text(
                location.get("Label")
                or location.get("label")
                or location.get("LocationName")
            )
            seen_key = (country, location_code)

            if not location_code or not location_term:
                continue

            with self.seen_lock:
                if seen_key in seen_location_codes:
                    continue
                seen_location_codes.add(seen_key)

            location_country = self.clean_text(location.get("Country"))
            booking_country = country

            iata_match = re.match(r"^([A-Z]{3})\s*,", location_term)
            iata_code = (
                iata_match.group(1)
                if iata_match and iata_match.group(1) in self.iata_codes
                else ""
            )
            is_airport = bool(iata_code) or "AIRPORT" in location_term.upper()
            location_type = "Airport" if is_airport else "City"
            location_name = iata_code if is_airport and iata_code else location_term
            pickup_location = location_name
            city = ""
            label_parts = [
                self.clean_text(part)
                for part in location_term.split(",")
                if self.clean_text(part)
            ]
            if len(label_parts) >= 3:
                city = label_parts[-2]
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
                "region": "",
                "priority_level": "",
                "location_term": location_term,
                "location_name": location_name,
                "booking_country": booking_country,
            }

            inserted = self.insert_one(row)
            if inserted:
                with self.rows_lock:
                    rows.append(row)


if __name__ == "__main__":
    STATUS = "any"
    STARTID = 108
    ENDID = 108
    INPUTTABLE = "input_locations"
    OUTPUTTABLE = "locations"
    OFFLINE = False
    PROXYID = "59"
    MAX_WORKERS = 50

    # 0 = normal run for all IATA codes.
    # 1 = retry only the failed/missing IATA codes below.
    RUN_MISSING_ONLY = 1
    MISSING_IATA_TERMS = [
        "ATH",
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

        SC = hertz_3(
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
