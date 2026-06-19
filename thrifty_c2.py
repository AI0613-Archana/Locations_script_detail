# -*- coding: utf-8 -*-
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

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

SOURCE_NAME = "thrifty_co_uk"
WEBSITE_CODE = 58
DOMAIN = "https://www.thrifty.co.uk/"

HEADERS = {
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
        "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    ),
    "sec-ch-ua": '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
}

AVAILABLE_TAGS_PATTERN = re.compile(
    r"var\s+availableTags\s*=\s*\[(.*?)\];", re.IGNORECASE | re.DOTALL
)


class thrifty_co_uk:
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
                print("extracted rows", len(rows))
                if rows:
                    self.insert(rows)
                    self.update(1, refid)
                else:
                    self.update(2, refid)
            except Exception:
                self.eHandling()
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                self.update(2, refid)

    def extraction(self, html_text, refid, websitecode, source_name):
        match = AVAILABLE_TAGS_PATTERN.search(html_text)
        if not match:
            return []

        try:
            tags = json.loads(f"[{match.group(1)}]")
        except json.JSONDecodeError:
            return []

        rows = []
        seen_terms = set()

        for raw_term in tags:
            location_term = str(raw_term).strip()
            if not location_term:
                continue

            normalized_term = re.sub(r"\s+", " ", location_term)
            unique_key = normalized_term.upper()
            if unique_key in seen_terms:
                continue
            seen_terms.add(unique_key)

            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            location_type = (
                "airport" if "airport" in normalized_term.lower() else "city"
            )
            is_airport = location_type == "airport"

            rows.append(
                {
                    "id": refid,
                    "source_name": source_name,
                    "website_code": websitecode,
                    "pickup_location": normalized_term,
                    "location_country": "GB",
                    "location_code": normalized_term,
                    "is_airport": is_airport,
                    "created_date": created_date,
                    "location_type": location_type,
                    "city": "",
                    "region": "",
                    "priority_level": "",
                    "location_term": normalized_term,
                    "location_name": normalized_term,
                }
            )

        return rows


if __name__ == "__main__":
    RETRY = 1
    while RETRY < 20:
        SC = None
        try:
            SC = thrifty_co_uk(2, 172, 172, "input_locations", "locations", False, "20")
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
            # SC = thrifty_co_uk(
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
