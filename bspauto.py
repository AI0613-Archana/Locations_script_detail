# -*- coding: utf-8 -*-
import csv
import os
import re
import sys
import time
from datetime import datetime, timezone
from io import StringIO

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


class bsp_auto:
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
        self.websitecode = 16
        self.is_dc_input = False

        self.cursor.execute(
            f"""
            SELECT * FROM {self.inputtable}
            WHERE status = %s AND id BETWEEN %s AND %s
        """,
            (status, startid, endid),
        )
        resultset = self.cursor.fetchall()
        self.main(resultset)

    def load(self, url, headers):
        return requests.get(url, timeout=30, headers=headers)

    def is_valid_csv(self, text, first_column):
        if not text:
            return False
        first_line = text.lstrip("\ufeff").splitlines()[0].strip()
        return first_line.split(";")[0].strip() == first_column

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
            self.websitecode = websitecode
            source_name = result["source_name"]
            country = result["country"]
            location_url = (
                result["source_url"]
                or "https://www.bsp-auto.com/js/agences-fr-min-maps.csv"
            )
            country_url = "https://www.bsp-auto.com/js/pays-fr.csv"
            headers = {
                "Accept": "text/plain, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
                "Content-Type": "text/plain;charset=utf-8",
                "Referer": "https://www.bsp-auto.com/",
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
                ),
                "X-Requested-With": "XMLHttpRequest",
                "sec-ch-ua": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Linux"',
            }

            try:
                country_response = self.load(country_url, headers)
                location_response = self.load(location_url, headers)
                print(
                    "Status:",
                    country_response.status_code,
                    location_response.status_code,
                )

                if (
                    country_response.status_code == 200
                    and location_response.status_code == 200
                    and (
                        not self.is_valid_csv(country_response.text, "a")
                        or not self.is_valid_csv(location_response.text, "i")
                    )
                ):
                    print(
                        "Invalid CSV response.",
                        "country sample:",
                        repr(country_response.text[:80]),
                        "location sample:",
                        repr(location_response.text[:80]),
                    )

                if (
                    country_response.status_code == 200
                    and location_response.status_code == 200
                    and self.is_valid_csv(country_response.text, "a")
                    and self.is_valid_csv(location_response.text, "i")
                ):
                    rows = []
                    seen_location_codes = set()
                    self.extraction(
                        country_response.text,
                        location_response.text,
                        refid,
                        country,
                        websitecode,
                        source_name,
                        rows,
                        seen_location_codes,
                    )
                    print("Extracted:", len(rows))
                    if rows:
                        self.insert(rows)
                        self.update(1, refid)
                    else:
                        self.update(2, refid)
                else:
                    print(
                        "Invalid response body.",
                        "country sample:",
                        repr(country_response.text[:120]),
                        "location sample:",
                        repr(location_response.text[:120]),
                    )
                    self.update(2, refid)
            except Exception:
                self.eHandling()
                self.update(2, refid)

    def extraction(
        self,
        country_html,
        location_html,
        refid,
        country,
        websitecode,
        source_name,
        rows,
        seen_location_codes,
    ):
        countries = {}
        for row in csv.DictReader(StringIO(country_html), delimiter=";"):
            country_id = re.sub(r"\s+", " ", str(row.get("a") or "")).strip()
            if not country_id:
                continue
            countries[country_id] = {
                "country_name": re.sub(r"\s+", " ", str(row.get("b") or "")).strip(),
                "country_code": re.sub(r"\s+", " ", str(row.get("c") or "")).strip(),
            }

        for row in csv.DictReader(StringIO(location_html), delimiter=";"):
            location_code = re.sub(r"\s+", " ", str(row.get("i") or "")).strip()
            location_name = re.sub(r"\s+", " ", str(row.get("c") or "")).strip()
            country_id = re.sub(r"\s+", " ", str(row.get("b") or "")).strip()
            if not location_code or not location_name:
                continue
            unique_location_key = (location_code, country_id, location_name)
            if unique_location_key in seen_location_codes:
                continue

            country_data = countries.get(country_id, {})
            location_country = country_data.get("country_code") or country
            is_airport = True if "AEROPORT" in location_name.upper() else False
            location_type = "Airport" if is_airport else "City"
            pickup_location = location_name
            location_name_parts = location_name.split()
            if (
                is_airport
                and location_name_parts
                and len(location_name_parts[-1]) == 3
                and location_name_parts[-1].isalpha()
            ):
                pickup_location = location_name_parts[-1].upper()

            city = location_name
            for splitter in (" Aeroport", " Gare", " Centre", "-"):
                if splitter in city:
                    city = city.split(splitter, 1)[0].strip()
                    break

            region = re.sub(r"\s+", " ", str(row.get("d") or "")).strip()
            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            seen_location_codes.add(unique_location_key)
            rows.append(
                {
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
                    "location_term": location_name,
                    "location_name": location_name,
                }
            )


if __name__ == "__main__":
    SC = None
    try:
        print(
            "\n⚠️  Please connect to a VPN before starting.\n"
            "Note: Proxies are not configured in this loader because the target website "
            "blocks proxy traffic, which may result in failed requests or incomplete results.\n"
        )
        # SC = bsp_auto(2, 158, 158, "input_locations", "locations", False, "20")

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
        SC = bsp_auto(
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
