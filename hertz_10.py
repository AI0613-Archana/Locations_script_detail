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


class hertz_10:
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
        self.websitecode = 37
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

    def load(self, url, headers, proxies):
        return requests.get(
            url,
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
            source_url = result["source_url"] or "https://www.hertz.ae/en/locations/"
            headers = {
                "accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,image/apng,*/*;q=0.8,"
                    "application/signed-exchange;v=b3;q=0.7"
                ),
                "accept-language": "en-US,en;q=0.9",
                "priority": "u=0, i",
                "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Linux"',
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
                ),
            }

            try:
                proxies = self.get_proxy()
                try:
                    response = self.load(source_url, headers, proxies)
                    print("Status:", response.status_code)
                except Exception as exc:
                    print(
                        "Proxy failed:",
                        proxies.get("https", ""),
                        "error:",
                        exc,
                    )
                    response = self.load(source_url, headers, {})
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

        location_code_mapping = {
            "Abu Dhabi Airport": "64",
            "Al Reef, Abu Dhabi": "122",
            "Car Rental in Dubai Motor City": "56",
            "Dubai Airport Terminal 1": "50",
            "Dubai Airport Terminal 2": "51",
            "Dubai Airport Terminal 3": "53",
            "Dubai Festival City": "59",
            "Dubai Festival City Mall": "43",
            "Dubai Head Office, Al Rashidiya": "60",
            "Dubai Marina": "57",
            "RAK Al Hamra Village Residence": "77",
            "Abu Dhabi Mall": "69",
            "Dubai VOCO Hotel by IHG Trade Centre": "45",
            "Sharjah Airport": "82",
            "Toyota Service Center - Al Badia Dubai": "44",
            "Toyota Service Center – Al Badia Dubai": "44",
            "Dubai Festival Plaza Mall": "31",
        }

        soup = BeautifulSoup(html, "html.parser")
        all_location_links = [
            link
            for link in soup.find_all("a")
            if str(link.get("href") or "").startswith("/en/locations/")
        ]
        location_links = [link for link in all_location_links if link.find("h2")]

        for link in location_links:
            h2_tag = link.find("h2")
            if not h2_tag:
                continue

            pickup_location = re.sub(
                r"\s+", " ", h2_tag.get_text(" ", strip=True)
            ).strip()
            if not pickup_location:
                continue

            location_code = re.sub(
                r"\s+", " ", str(location_code_mapping.get(pickup_location, ""))
            ).strip()
            if not location_code:
                continue
            if location_code in seen_location_codes:
                continue

            href = str(link.get("href") or "")
            p_tags = link.find_all("p", class_="text-md-regular")
            address = (
                re.sub(r"\s+", " ", p_tags[0].get_text(" ", strip=True)).strip()
                if p_tags
                else ""
            )
            city = ""
            if "Abu Dhabi" in pickup_location or "Abu Dhabi" in address:
                city = "Abu Dhabi"
            elif "Dubai" in pickup_location or "Dubai" in address:
                city = "Dubai"
            elif "Sharjah" in pickup_location or "Sharjah" in address:
                city = "Sharjah"
            elif "Ras Al Khaimah" in address or "RAK" in pickup_location:
                city = "Ras Al Khaimah"

            is_airport = True if "airport" in href.lower() or "airport" in pickup_location.lower() else False
            location_type = "Airport" if is_airport else "City"
            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            seen_location_codes.add(location_code)
            row = {
                "id": refid,
                "source_name": source_name,
                "website_code": websitecode,
                "pickup_location": pickup_location,
                "location_country":"AE",
                "location_code": location_code,
                "is_airport": is_airport,
                "created_date": created_date,
                "location_type": location_type,
                "city": city,
                "region": "",
                "priority_level": "",
                "location_term": pickup_location,
                "location_name": pickup_location,
            }
            rows.append(row)


if __name__ == "__main__":

    SC = None
    try:
        # SC = hertz_10(0, 94, 94, "input_locations", "locations", False, "20")

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
        SC = hertz_10(
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
