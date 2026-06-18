# -*- coding: utf-8 -*-
import os
import random
import re
import sys
import time
from datetime import datetime, timezone

import psycopg2
from bs4 import BeautifulSoup
from curl_cffi import requests
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


class first_loc:
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
        self.websitecode = 30
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

    def load(self, url, headers, params, proxies):
        return requests.get(
            url,
            params=params,
            timeout=30,
            headers=headers,
            proxies=proxies,
            impersonate="chrome",
        )

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
                or "https://weblink.firstcarrental.co.za/book-a-car/SinglePage.aspx"
            )
            params = {
                "A": "FIRST CAR RENTAL",
                "B": "WEBSITE",
                "C": "WEBSITE",
            }
            headers = {
                "accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,image/apng,*/*;q=0.8,"
                    "application/signed-exchange;v=b3;q=0.7"
                ),
                "accept-language": "en-US,en;q=0.9",
                "priority": "u=0, i",
                "referer": "https://www.firstcarrental.co.za/",
                "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Linux"',
                "sec-fetch-dest": "iframe",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "same-site",
                "upgrade-insecure-requests": "1",
                "user-agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
                ),
            }

            try:
                proxies = self.get_proxy()
                try:
                    response = self.load(source_url, headers, params, proxies)
                    print("Status:", response.status_code)
                except Exception as exc:
                    print(
                        "Proxy failed:",
                        proxies.get("https", ""),
                        "error:",
                        exc,
                    )
                    response = self.load(source_url, headers, params, {})
                    print("Status:", response.status_code, "without proxy")

                if response.status_code == 200:
                    rows = []
                    seen_location_codes = set()
                    self.extraction(
                        response.text,
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
                    self.update(2, refid)
            except Exception:
                self.eHandling()
                self.update(2, refid)

    def extraction(
        self, html, refid, country, websitecode, source_name, rows, seen_location_codes
    ):
        if not html:
            return

        soup = BeautifulSoup(html, "html.parser")
        for item in soup.select("#PU_infocontainer div.s"):
            code_tag = item.select_one("span.cityid")
            if not code_tag:
                continue

            location_code = re.sub(
                r"\s+", " ", code_tag.get_text(" ", strip=True)
            ).strip()
            location_name = re.sub(
                r"\s+",
                " ",
                item.get_text(" ", strip=True).replace(location_code, ""),
            ).strip()
            if (
                not location_code
                or not location_name
                or location_code in seen_location_codes
            ):
                continue

            is_airport = True if "Airport" in location_name else False
            location_type = (
                "Airport"
                if is_airport
                else (
                    "Downtown"
                    if "Downtown" in location_name
                    else "Depot" if "Depot" in location_name else "City"
                )
            )
            name = location_name.split(" : ", 1)[-1]
            city = re.sub(r"\s+", " ", name.rsplit("-", 1)[0]).strip()
            location_term = f"{location_name} {location_code}".strip()
            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            seen_location_codes.add(location_code)
            row = {
                "id": refid,
                "source_name": source_name,
                "website_code": websitecode,
                "pickup_location": location_name,
                "location_country": country or "South Africa",
                "location_code": location_code,
                "is_airport": is_airport,
                "created_date": created_date,
                "location_type": location_type,
                "city": city,
                "region": "Africa",
                "priority_level": "",
                "location_term": location_term,
                "location_name": location_name,
            }
            rows.append(row)


if __name__ == "__main__":

    SC = None
    try:
        # SC = first_loc(0, 163, 163, "input_locations", "locations", False, "20")

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
        SC = first_loc(
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
