# -*- coding: utf-8 -*-
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import airportsdata
import psycopg2
from curl_cffi import requests
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor, execute_values

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}

SOURCE_NAME = "thrifty_gr"
WEBSITE_CODE = 58
DOMAIN = "https://www.thrifty.com.gr/"
THREAD_COUNT = 20
SEARCH_URL = "https://www.thrifty.com.gr/el/Resources/SearchBranchLocations"

session = requests.Session()
airport_data = airportsdata.load("IATA")

COOKIES = {
    "cp_total_cart_items": "0",
    "cp_total_cart_value": "0",
    "cpab": "b0921252-eb2c-4c5d-c5db-6adb42bafb7d",
    "ASP.NET_SessionId": "f0uffhlxmgyavhp0tiw2k3nc",
    "__RequestVerificationToken": (
        "l-YGsFAEHfMPS0luw36UU62mrRrN8q5Pps7WdnfmuG0E8brgTMTF6wYqh7vINvkKZ2yOgcc9nv1qHxfAk7iWt7U6NUORwUn8q7yDq-MfZYA1"
    ),
    "cp_sessionTime": "1781853513759",
}

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://www.thrifty.com.gr",
    "Referer": DOMAIN,
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
    "sec-ch-ua": '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
}


class thrifty_gr:
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
        self.websitecode = WEBSITE_CODE
        self.is_dc_input = False
        self.cursor.execute(
            f"""
            SELECT * FROM {self.inputtable}
            WHERE websitecode = %s::text AND status = %s AND id BETWEEN %s AND %s
        """,
            (str(self.websitecode), status, startid, endid),
        )
        resultset = self.cursor.fetchall()
        self.main(resultset)

    def load(self, search_value, source_url):
        return session.post(
            source_url,
            cookies=COOKIES,
            headers=HEADERS,
            data={"term": str(search_value).lower()},
            timeout=30,
        )

    def insert(self, chunks):
        if not chunks:
            print("No rows supplied for insert.")
            return

        print("INSERT INITIATED")
        columns = [c for c in chunks[0].keys() if c != "id"]
        colnames = ",".join(columns)
        values = [tuple(row.get(col) for col in columns) for row in chunks]
        sql = f"INSERT INTO {self.outputtable} ({colnames}) VALUES %s"
        with self.conn.cursor() as cursor:
            execute_values(cursor, sql, values, page_size=500)
        self.conn.commit()
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
            websitecode = result.get("websitecode") or self.websitecode
            source_name = result.get("source_name", SOURCE_NAME)
            country = result.get("country", "") or result.get("location_country", "")
            source_url = result.get("source_url") or SEARCH_URL
            print("refid", refid, "source_url", source_url)
            try:
                rows = []
                airport_codes = [
                    code
                    for code, airport_meta in airport_data.items()
                    if airport_meta.get("country") == country
                ]
                if not airport_codes:
                    airport_codes = list(airport_data.keys())
                print("airport seed count", len(airport_codes), "for country", country)

                with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
                    futures = {
                        executor.submit(self.load, airport_code, source_url): airport_code
                        for airport_code in airport_codes
                    }
                    for future in as_completed(futures):
                        airport_code = futures[future]
                        try:
                            response = future.result()
                            print("search_value", airport_code, "status", response.status_code)
                            if response.status_code != 200:
                                continue
                            rows.extend(
                                self.extraction(
                                    response.json(),
                                    airport_code,
                                    refid,
                                    websitecode,
                                    source_name,
                                )
                            )
                        except Exception:
                            continue

                if rows:
                    self.insert(rows)
                    self.update(1, refid)
                else:
                    self.update(2, refid)
            except Exception:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                self.update(2, refid)

    def extraction(self, response_data, search_value, refid, websitecode, source_name):
        rows = []
        seen_keys = set()

        for item in response_data.get("Locations", []):
            label = (item.get("Label") or "").strip()
            location_code = item.get("Value", "")
            location_country = item.get("Country", "")
            airport_meta = airport_data.get(search_value.upper(), {})
            is_airport = not label.upper().startswith(f"{search_value.upper()},")
            location_type = "airport" if is_airport else "city"
            unique_key = (search_value.upper(), location_code, label)

            if unique_key in seen_keys:
                continue
            seen_keys.add(unique_key)
            region =  airport_meta.get("subd", "")

            rows.append(
                {
                    "id": refid,
                    "source_name": source_name,
                    "website_code": websitecode,
                    "pickup_location": search_value.upper(),
                    "location_country": location_country,
                    "location_code": location_code,
                    "is_airport": is_airport,
                    "created_date": '',
                    "location_type": location_type,
                    "city": "",
                    "region":region,
                    "priority_level": "",
                    "location_term": label,
                    "location_name": label,
                }
            )

        return rows


if __name__ == "__main__":
    RETRY = 1
    while RETRY < 20:
        SC = None
        try:
            SC = thrifty_gr(0, 171, 171, "input_locations", "locations", False, "20")
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
            # SC = thrifty_gr(
            #     status,
            #     startid,
            #     endid,
            #     inputtable,
            #     outputtable,
            #     offline,
            #     proxyid,
            # )
        except Exception as e:
            if SC:
                SC.eHandling()
            else:
                exc_type, exc_obj, tb = sys.exc_info()
                print("Startup error:", repr(e) or repr(exc_obj))
        finally:
            if SC:
                SC.conn_close()
        time.sleep(3)
        RETRY += 1
