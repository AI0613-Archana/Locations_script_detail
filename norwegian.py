# -*- coding: utf-8 -*-
import json
import os
import random
import re
import sys
import time
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


class norwegian:
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
        self.websitecode = 48
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
        proxy_str = (
            self.proxyset[random.randrange(0, len(self.proxyset))].get("proxy") or ""
        ).strip()
        if not proxy_str:
            return {}
        proxy_url = proxy_str if "://" in proxy_str else f"http://{proxy_str}"
        return {"http": proxy_url, "https": proxy_url}

    def load(self, url, headers, params, proxies):
        session = TLSSession(
            profile="chrome_120",
            proxies=proxies,
            on_block="none",
            max_retries=0,
        )
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
            source_url = result["source_url"] or "https://www.norwegian.com/api/cartrawler"
            rows = []
            seen_location_codes = set()
            headers = {
                "accept": "application/json, text/plain, */*",
                "accept-language": "en-US,en;q=0.9",
                "referer": "https://www.norwegian.com/us/",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
                ),
            }

            try:
                def fetch_iata(iata):
                    params = {
                        "culture": "en-US",
                        "marketCode": "us",
                        "searchString": iata,
                    }
                    proxies = self.get_proxy()
                    try:
                        response = self.load(source_url, headers, params, proxies)
                        if response.status_code not in (403, 429):
                            return iata, response.status_code, response.text
                        print(
                            "IATA:",
                            iata,
                            "proxy returned:",
                            response.status_code,
                            "retrying without proxy",
                        )
                    except Exception as exc:
                        print(
                            "IATA:",
                            iata,
                            "proxy failed:",
                            proxies.get("https", ""),
                            "error:",
                            exc,
                        )
                    try:
                        response = self.load(source_url, headers, params, {})
                        print(
                            "IATA:",
                            iata,
                            "without proxy status:",
                            response.status_code,
                        )
                        return iata, response.status_code, response.text
                    except Exception as exc:
                        print("IATA:", iata, "without proxy failed:", exc)
                    return iata, None, ""

                iata_codes = list(airportsdata.load("IATA").keys())
                with ThreadPoolExecutor(max_workers=100) as executor:
                    futures = [
                        executor.submit(fetch_iata, iata) for iata in iata_codes
                    ]
                    for future in as_completed(futures):
                        iata, status_code, response_text = future.result()
                        location_count = 0
                        if status_code == 200 and response_text:
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
                        print(
                            "IATA:",
                            iata,
                            "Location Count:",
                            location_count,
                            "Status Code:",
                            status_code,
                        )

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
        locations = response_data.get("carRentalLocations", [])
        if not isinstance(locations, list):
            return

        for location in locations:
            if not isinstance(location, dict):
                continue

            location_code = re.sub(
                r"\s+", " ", str(location.get("code") or "")
            ).strip()
            display_name = re.sub(
                r"\s+", " ", str(location.get("displayName") or "")
            ).strip()
            if (
                not location_code
                or not display_name
                or location_code in seen_location_codes
            ):
                continue

            is_airport = True if bool(location.get("isAirport")) else False
            is_railway_station = bool(location.get("isRailwayStation"))
            airport_name = re.sub(
                r"\s+", " ", str(location.get("airportName") or "")
            ).strip().upper()

            if is_airport:
                pickup_location = airport_name or display_name
                location_type = "Airport"
            elif is_railway_station:
                pickup_location = display_name
                location_type = "Railway Station"
            else:
                pickup_location = display_name
                location_type = "City"

            location_country = re.sub(
                r"\s+",
                " ",
                str(location.get("countryName") or country or ""),
            ).strip()
            city = re.sub(r"\s+", " ", str(location.get("cityName") or "")).strip()
            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            seen_location_codes.add(location_code)
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
                "location_term": "",
                "location_name": display_name,
            }
            rows.append(row)


if __name__ == "__main__":
    SC = None
    try:
        # SC = norwegian(0, 148, 148, "input_locations", "locations", False, "20")

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
        SC = norwegian(
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
