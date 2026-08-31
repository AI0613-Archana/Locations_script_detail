# -*- coding: utf-8 -*-
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import psycopg2
import tls_client
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor, execute_values

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

SOURCE_NAME = "thriftyuae.com"
WEBSITE_CODE = 58
SOURCE_URL = "https://www.thriftyuae.com/api/branch"

DEBUG_DUMP_RESPONSE = os.getenv("DEBUG_DUMP_RESPONSE", "0") == "1"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}

COOKIES = {
    '__Host-next-auth.csrf-token': 'd495235640bfd15f44a4fb7cb7a08b4087d6088b7904e40bfb134f6c53b5f2fa%7C27afcbb606ac57573968406839c4e4cae0b5549b2747300fd333873100c64f83',
    '__Secure-next-auth.callback-url': 'https%3A%2F%2Fwww.thriftyuae.com',
    '_ga': 'GA1.1.844887717.1788155134',
    '_fbp': 'fb.1.1788155140762.261373895502443706',
    '_gcl_au': '1.1.712860749.1788155134.899420487.1788155185.1788155185.518287400.1788155185.1788155185',
    '_ga_9XYLGBCRGZ': 'GS2.1.s1788155134$o1$g1$t1788155228$j60$l0$h646182805',
    'cf_clearance': 'az_AFJar2WXmpgRvHHscBCFFsz8Yl5Cl2SPLuHRYrqI-1788156331-1.2.1.1-wphFCb1.4VWwtxgiX2Z5Gk8wJMvVUDjykb0rrJUfuAbSu3GW4AFrfP95FhCQVjEkrAM1jKYZmKNBLNO6ypcUsLPYRXz34dEG_wz.2JaPl_gJFscczkksBfefywSAjyBUthq2q.3TQoDm7eNKnIzSKep5OiyB9abSvvv4WXBlie9imY1flRbZYL3ZxFPUpTCu.tWSqsg9WqY8XG5LhYrAqQoXK5ua4qr9179rkRVTIKroHpF.oKFRlfsP9Np8C56_sM0qOrYtouCY7IQ0c8.muzCgbS4ve78GIQ18mf80Z1YohBbsX4EWIc1u6CUAD3UV7NUxczOtcHSqqseOpGKZoEDVW3Bnl0rIbA5xBkPsYYA',
}


HEADERS = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'priority': 'u=1, i',
    'referer': 'https://www.thriftyuae.com/',
    'sec-ch-ua': '"Not=A?Brand";v="99", "Google Chrome";v="151", "Chromium";v="151"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36',
    # 'cookie': '__Host-next-auth.csrf-token=d495235640bfd15f44a4fb7cb7a08b4087d6088b7904e40bfb134f6c53b5f2fa%7C27afcbb606ac57573968406839c4e4cae0b5549b2747300fd333873100c64f83; __Secure-next-auth.callback-url=https%3A%2F%2Fwww.thriftyuae.com; _ga=GA1.1.844887717.1788155134; _fbp=fb.1.1788155140762.261373895502443706; _gcl_au=1.1.712860749.1788155134.899420487.1788155185.1788155185.518287400.1788155185.1788155185; _ga_9XYLGBCRGZ=GS2.1.s1788155134$o1$g1$t1788155228$j60$l0$h646182805; cf_clearance=az_AFJar2WXmpgRvHHscBCFFsz8Yl5Cl2SPLuHRYrqI-1788156331-1.2.1.1-wphFCb1.4VWwtxgiX2Z5Gk8wJMvVUDjykb0rrJUfuAbSu3GW4AFrfP95FhCQVjEkrAM1jKYZmKNBLNO6ypcUsLPYRXz34dEG_wz.2JaPl_gJFscczkksBfefywSAjyBUthq2q.3TQoDm7eNKnIzSKep5OiyB9abSvvv4WXBlie9imY1flRbZYL3ZxFPUpTCu.tWSqsg9WqY8XG5LhYrAqQoXK5ua4qr9179rkRVTIKroHpF.oKFRlfsP9Np8C56_sM0qOrYtouCY7IQ0c8.muzCgbS4ve78GIQ18mf80Z1YohBbsX4EWIc1u6CUAD3UV7NUxczOtcHSqqseOpGKZoEDVW3Bnl0rIbA5xBkPsYYA',
}

session = tls_client.Session(
    client_identifier="chrome112",
    random_tls_extension_order=True,
)


class ThriftyUAE:
    def __init__(self, status, startid, endid, inputtable, outputtable, offline, proxyid):
        self.inputtable = inputtable
        self.outputtable = outputtable
        self.startid = startid
        self.endid = endid
        self.proxyid = proxyid
        self.websitecode = WEBSITE_CODE

        self.conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)

        self.cursor.execute(
            f"""
            SELECT * FROM {self.inputtable}
            WHERE websitecode = %s AND status = %s AND id BETWEEN %s AND %s
            """,
            (str(self.websitecode), status, startid, endid),
        )
        resultset = self.cursor.fetchall()
        self.main(resultset)

    # -- networking -----------------------------------------------------
    def load(self, source_url):
        return session.get(source_url, cookies=COOKIES, headers=HEADERS, timeout_seconds=30)

    # -- db helpers -------------------------------------------------------
    def insert(self, rows):
        if not rows:
            print("No rows supplied for insert.")
            return

        print("INSERT INITIATED")
        columns = [c for c in rows[0].keys() if c != "id"]
        colnames = ",".join(columns)
        values = [tuple(row.get(col) for col in columns) for row in rows]
        sql = f"INSERT INTO {self.outputtable} ({colnames}) VALUES %s"
        with self.conn.cursor() as cursor:
            execute_values(cursor, sql, values, page_size=500)
        self.conn.commit()
        print(f"INSERTED {len(rows)} rows")

    def update(self, upstatus, refid):
        self._execute_commit(
            f"UPDATE {self.inputtable} SET status=%s WHERE id=%s", (upstatus, refid)
        )
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
            refid = result["id"]
            websitecode = result.get("websitecode") or self.websitecode
            source_name = result.get("source_name") or SOURCE_NAME
            source_url = result.get("source_url") or SOURCE_URL
            print("refid", refid, "source_url", source_url)

            try:
                response = self.load(source_url)
                print("status", response.status_code)

                if DEBUG_DUMP_RESPONSE:
                    with open("response.json", "w") as f:
                        f.write(response.text)

                if response.status_code != 200:
                    self.update(2, refid)
                    continue

                response_data = self._parse_json(response.text)
                rows = self.extraction(response_data, refid, websitecode, source_name)

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

    @staticmethod
    def _parse_json(raw_text):
        """Parse JSON, tolerating stray raw control characters inside strings."""
        try:
            return json.loads(raw_text)
        except Exception:
            cleaned = []
            in_string = False
            escaped = False
            for char in raw_text:
                if in_string:
                    if escaped:
                        cleaned.append(char)
                        escaped = False
                        continue
                    if char == "\\":
                        cleaned.append(char)
                        escaped = True
                        continue
                    if char == '"':
                        cleaned.append(char)
                        in_string = False
                        continue
                    if char in "\n\r\t":
                        cleaned.append(" ")
                        continue
                    cleaned.append(char)
                    continue

                cleaned.append(char)
                if char == '"':
                    in_string = True

            return json.loads("".join(cleaned))

    def extraction(self, response_data, refid, websitecode, source_name):
        rows = []
        seen_codes = set()

        for state_name, state_data in response_data.items():
            branches = state_data.get("Branches", [])

            for branch in branches:
                location_code = re.sub(r"\s+", " ", str(branch.get("Id") or "")).strip()
                location_term = re.sub(r"\s+", " ", str(branch.get("Name") or "")).strip()

                if not location_code or not location_term or location_code in seen_codes:
                    continue
                seen_codes.add(location_code)

                location_country = re.sub(
                    r"\s+", " ", str(branch.get("CountryCode") or "")
                ).strip()
                city = re.sub(
                    r"\s+", " ", str(branch.get("StateName") or state_name or "")
                ).strip()
                region = re.sub(r"\s+", " ", str(state_name or "")).strip()

                location_name = location_term
                is_airport = "airport" in location_name.lower()
                location_type = "airport" if is_airport else "city"
                created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

                rows.append(
                    {
                        "id": refid,
                        "source_name": source_name,
                        "website_code": websitecode,
                        "pickup_location": location_name,
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
                )

        return rows


if __name__ == "__main__":
    RETRY = 1
    MAX_RETRIES = 20

    while RETRY < MAX_RETRIES:
        scraper = None
        try:
            if len(sys.argv) == 8:
                (
                    _,
                    status,
                    startid,
                    endid,
                    inputtable,
                    outputtable,
                    offline,
                    proxyid,
                ) = sys.argv
                scraper = ThriftyUAE(
                    status, startid, endid, inputtable, outputtable, offline, proxyid
                )
            else:
                scraper = ThriftyUAE(0, 267, 267, "input_locations", "locations", False, "60")
        except Exception as e:
            if scraper:
                scraper.eHandling()
            else:
                print("Startup error:", repr(e))
        finally:
            if scraper:
                scraper.conn_close()

        time.sleep(3)
        RETRY += 1