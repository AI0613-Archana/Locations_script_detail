# -*- coding: utf-8 -*-
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
import tls_client
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor, execute_values
from tls_chameleon import TLSSession
session = tls_client.Session(

    client_identifier="chrome112",

    random_tls_extension_order=True

)

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}

SOURCE_NAME = "thriftyuae"
WEBSITE_CODE = 58
SOURCE_URL = "https://www.thriftyuae.com/api/branch"

session = TLSSession(
    profile="chrome_124_linux",
    randomize=True,
    http2_priority="chrome",
)

COOKIES = {
    "cf_clearance": "Izi9PvARMticXL1TdvETglpgruEVadhTDWn_bYmhE0g-1781859805-1.2.1.1-8rSgSMqvnYSnAcDpzusRVHBBDsC9opjSSg0VF27Tjf5vrygbNneaGct8qcT8xQhcPHrTVcJL25MCWTFSMbJKZud9gFoOCu3c188ByOM0r6875QGYzQpDvtUJlJv1f84R8GMEEiSubNh4bblM95HPOdwZZT_lHtDRHt4yI9m9UYF_mYOaA4bjjIg_a0TRuiunTgYuSyoIl5RzqY1Mdw0WmOyulYxhuV2LMEqhvYrAeMv6SgmUhb9fJQiFBG0P3aM8.33orBMWoIXhnDg5ZgpfRN3XCrbZY5SVQDU.6DAK6RP0.UUm0RzADVtoVoV39SoHZNrvqdEeDdoDwtGHH88WbA",
}

HEADERS = {
    "accept": "*/*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "priority": "u=1, i",
    "referer": "https://www.thriftyuae.com/",
    "sec-ch-ua": '"Chromium";v="149", "Not)A;Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    ),
}


class thriftyuae:
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
        return session.get(source_url, cookies=COOKIES, headers=HEADERS, timeout=30)

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
            source_url = result.get("source_url") or SOURCE_URL
            print("refid", refid, "source_url", source_url)
            try:
                response = self.load(source_url)
                print("status", response.status_code)
                if response.status_code != 200:
                    self.update(2, refid)
                    continue

                try:
                    response_data = response.json()
                except Exception:
                    raw_text = response.text
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

                    response_data = json.loads("".join(cleaned))

                rows = self.extraction(response_data, refid, websitecode, source_name)
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

    def extraction(self, response_data, refid, websitecode, source_name):
        rows = []
        seen_codes = set()
        source_name=58

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
                    r"\s+",
                    " ",
                    str(branch.get("StateName") or state_name or ""),
                ).strip()
                region = re.sub(r"\s+", " ", str(state_name or "")).strip()
                location_name = location_term
                lowered_name = location_name.lower()
                is_airport = "airport" in lowered_name
                location_type = "airport" if is_airport else "city"
                created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                websitecode = 58
                source_name ="thriftyuae.com"
                    
               

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
    while RETRY < 20:
        SC = None
        try:
            SC = thriftyuae(0, 173, 173, "input_locations", "locations", False, "20")
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
            # SC = thriftyuae(
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
