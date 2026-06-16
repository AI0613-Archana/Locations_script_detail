# -*- coding: utf-8 -*-
import os
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape

import psycopg2
import requests
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

SOURCE_NAME = "avance"
WEBSITE_CODE = 10
DOMAIN = "https://avance.gr/"

HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
        "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-language": "en-US,en;q=0.9",
    "referer": "https://www.google.com/",
    "sec-ch-ua": '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "upgrade-insecure-requests": "1",
    "user-agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    ),
}

SELECT_PATTERN = re.compile(
    r'<select[^>]*name="(?:pickupLocation|dropOffLocation)"[^>]*>(.*?)</select>',
    re.IGNORECASE | re.DOTALL,
)
OPTION_PATTERN = re.compile(
    r'<option\s+value="([^"]*)"(?:\s+[^>]*)?>(.*?)</option>',
    re.IGNORECASE | re.DOTALL,
)


class avance:
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

    def load(self, source_url):
        response = requests.get(source_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.text

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
            source_url = result.get("source_url") or DOMAIN
            print("refid", refid, "source_url", source_url)
            try:
                html_text = self.load(source_url)
                rows = self.extraction(html_text, refid, websitecode, source_name)
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

    def extraction(self, html_text, refid, websitecode, source_name):
        rows = []
        seen_codes = set()

        for select_html in SELECT_PATTERN.findall(html_text):
            for location_code, location_name in OPTION_PATTERN.findall(select_html):
                created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                location_code = location_code.strip()
                location_name = unescape(re.sub(r"\s+", " ", location_name)).strip()

                if not location_code or location_code in seen_codes:
                    continue

                seen_codes.add(location_code)
                location_type = (
                    "airport" if "airport" in location_name.lower() else "city"
                )
                is_airport = location_type == "airport"

                rows.append(
                    {
                        "id": refid,
                        "source_name": source_name,
                        "website_code": websitecode,
                        "pickup_location": location_name,
                        "location_country": "GR",
                        "location_code": location_code,
                        "is_airport": is_airport,
                        "created_date": created_date,
                        "location_type": location_type,
                        "city": "",
                        "region": "",
                        "priority_level": "",
                        "location_term": "",
                        "location_name": location_name,
                    }
                )

        return rows


if __name__ == "__main__":
    RETRY = 1
    while RETRY < 20:
        SC = None
        try:
            SC = avance(2, 154, 154, "input_locations", "locations", False, "20")
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
            # SC = avance(
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
