# -*- coding: utf-8 -*-
import json
import os
import random
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import airportsdata
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor, execute_values
from tls_chameleon import TLSSession


load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}


class driveaway:
    def __init__(
        self, status, startid, endid, inputtable, outputtable, offline, proxyid
    ):
        self.inputtable = inputtable
        self.outputtable = outputtable
        self.startid = startid
        self.endid = endid
        self.proxyid = proxyid
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        self.websitecode = 22
        self.is_dc_input = False
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

    def get_proxy(self):
        if not self.proxyset:
            return {}
        proxy_str = (self.proxyset[random.randrange(0, len(self.proxyset))].get("proxy") or "").strip()
        if not proxy_str:
            return {}
        proxy_url = proxy_str if "://" in proxy_str else f"http://{proxy_str}"
        return {"http": proxy_url, "https": proxy_url}

    def load(self, url, headers, proxies, params):
        session = TLSSession(proxies=proxies)
        return session.get(url, params=params, headers=headers, timeout=30)

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
                        value = row.get(col)
                        max_length = length_limits.get(col)
                        if (
                            isinstance(value, str)
                            and max_length
                            and len(value) > max_length
                        ):
                            print(
                                "Truncated",
                                col,
                                "from",
                                len(value),
                                "to",
                                max_length,
                                "for location_code",
                                row.get("location_code"),
                            )
                            value = value[:max_length]
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

    def main(self, resultset):
        for result in resultset:
            print(result)
            refid = result["id"]
            websitecode = result["websitecode"]
            source_name = result["source_name"]
            country = result["country"]
            source_url = (
                result["source_url"]
                or "https://www.driveaway.com.au/plugins/ae3/lib/searchController.cfc"
            )
            headers = {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.driveaway.com.au/",
                "User-Agent": (
                    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:151.0) "
                    "Gecko/20100101 Firefox/151.0"
                ),
                "X-Requested-With": "XMLHttpRequest",
            }

            try:
                rows = []
                seen_location_codes = set()
                rows_lock = threading.Lock()

                def fetch_iata(iata_code):
                    params = {
                        "method": "textsearch",
                        "criteria": iata_code.lower(),
                        "affiliateName": "DRIVEAWAY",
                        "operator_id_list": "",
                    }
                    if "{iata}" in source_url:
                        url = source_url.format(iata=iata_code.lower())
                        params = {}
                    else:
                        url = source_url

                    proxies = self.get_proxy()
                    try:
                        response = self.load(url, headers, proxies, params)
                        return iata_code, response.status_code, response.text
                    except Exception as exc:
                        print(
                            "IATA:",
                            iata_code,
                            "proxy failed:",
                            proxies.get("https", ""),
                            "error:",
                            exc,
                        )
                    try:
                        response = self.load(url, headers, {}, params)
                        return iata_code, response.status_code, response.text
                    except Exception as exc:
                        print("IATA:", iata_code, "without proxy failed:", exc)
                    return iata_code, None, ""

                with ThreadPoolExecutor(max_workers=50) as executor:
                    futures = [
                        executor.submit(fetch_iata, iata_code)
                        for iata_code in airportsdata.load("IATA").keys()
                    ]
                    for future in as_completed(futures):
                        iata_code, status_code, response_text = future.result()
                        before_count = len(rows)
                        if status_code == 200 and response_text:
                            with rows_lock:
                                before_count = len(rows)
                                self.extraction(
                                    response_text,
                                    refid,
                                    country,
                                    websitecode,
                                    source_name,
                                    rows,
                                    seen_location_codes,
                                )
                                location_count = len(rows) - before_count
                        else:
                            location_count = 0
                        print(
                            "IATA:",
                            iata_code,
                            "Location Count:",
                            location_count,
                            "Status Code:",
                            status_code,
                        )

                print("Extracted:", len(rows))
                if rows:
                    self.insert(rows)
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

        response_data = json.loads(html)
        locations_data = response_data.get("locations", {}).get("0", {})
        locations = locations_data.get("locations", [])
        if not isinstance(locations, list):
            return

        for location in locations:
            if not isinstance(location, dict):
                continue

            chaos_hub_id = re.sub(
                r"\s+", " ", str(location.get("chaos_hub_id") or "")
            ).strip()
            desk_list = location.get("desk_list")
            if isinstance(desk_list, (list, dict)):
                desk_value = json.dumps(desk_list, ensure_ascii=False)
            else:
                desk_value = re.sub(r"\s+", " ", str(desk_list or "")).strip()
            location_code = f"{chaos_hub_id}|{desk_value}".strip("|")
            if not chaos_hub_id or chaos_hub_id in seen_location_codes:
                continue

            iata = re.sub(r"\s+", " ", str(location.get("iata") or "")).strip().upper()
            city = re.sub(
                r"\s+", " ", str(location.get("chaos_city_name") or "")
            ).strip()
            location_country = re.sub(
                r"\s+", " ", str(location.get("country_label") or country or "")
            ).strip()
            location_type_source = re.sub(
                r"\s+", " ", str(location.get("category") or "")
            ).strip()
            location_term = re.sub(
                r"\s+", " ", str(location.get("display_label") or "")
            ).strip()
            is_airport = True if location_type_source.upper() == "AIRPORT" else False
            location_type = "Airport" if is_airport else "City"
            pickup_location = iata if is_airport and iata else location_term
            location_name = location_term or pickup_location
            if not pickup_location or not location_name:
                continue

            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            seen_location_codes.add(chaos_hub_id)
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
            }
            rows.append(row)


if __name__ == "__main__":
    SC = None
    try:
        # SC = driveaway(0, 161, 161, "input_locations", "locations", False, "20")

        (
            script,
            status,
            startid,
            endid,
            inputtable,
            outputtable,
            offline,
            proxyid,
        ) = sys.argv
        SC = driveaway(
            status,
            startid,
            endid,
            inputtable,
            outputtable,
            offline,
            proxyid,
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
