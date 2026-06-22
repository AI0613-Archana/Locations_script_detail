# -*- coding: utf-8 -*-
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import quote

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


class hertz_4:
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
            source_url = result["source_url"] or "https://api.hertz.com/rest/geography/country"
            headers = {
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "referer": "https://www.hertz.bh/",
                "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Linux"',
                "sec-fetch-dest": "script",
                "sec-fetch-mode": "no-cors",
                "sec-fetch-site": "cross-site",
                "sec-fetch-storage-access": "active",
                "user-agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
                ),
            }
            params = {
                "dialect": "enGB",
                "callback": "parse",
            }

            try:
                rows = []
                seen_location_codes = set()
                proxies = self.get_proxy()
                try:
                    country_response = self.load(source_url, headers, params, proxies)
                    print("Country status:", country_response.status_code)
                except Exception as exc:
                    print(
                        "Country proxy failed:",
                        proxies.get("https", ""),
                        "error:",
                        exc,
                    )
                    country_response = self.load(source_url, headers, params, {})
                    print("Country status:", country_response.status_code, "without proxy")

                if country_response.status_code != 200:
                    self.update(2, refid)
                    continue

                country_text = country_response.text.strip()
                country_match = re.search(r"^[^(]+\(\s*(\{.*\})\s*\)\s*;?$", country_text, re.S)
                country_payload = json.loads(country_match.group(1) if country_match else country_text)
                countries = country_payload.get("data", {}).get("model", [])
                if not isinstance(countries, list):
                    countries = []

                for country_row in countries:
                    if not isinstance(country_row, dict):
                        continue
                    country_code = re.sub(r"\s+", " ", str(country_row.get("value") or "")).strip()
                    country_name = re.sub(r"\s+", " ", str(country_row.get("name") or "")).strip()
                    if not country_code:
                        continue

                    city_url = f"https://api.hertz.com/rest/geography/city/country/{country_code}"
                    try:
                        city_response = self.load(city_url, headers, params, proxies)
                        print(country_code, "city status:", city_response.status_code)
                    except Exception as exc:
                        print(country_code, "city proxy failed:", exc)
                        try:
                            city_response = self.load(city_url, headers, params, {})
                            print(country_code, "city status:", city_response.status_code, "without proxy")
                        except Exception as exc:
                            print(country_code, "city direct failed:", exc)
                            continue

                    if city_response.status_code != 200:
                        continue

                    city_text = city_response.text.strip()
                    city_match = re.search(r"^[^(]+\(\s*(\{.*\})\s*\)\s*;?$", city_text, re.S)
                    city_payload = json.loads(city_match.group(1) if city_match else city_text)
                    cities = city_payload.get("data", {}).get("model", [])
                    if not isinstance(cities, list):
                        continue

                    for city_row in cities:
                        if not isinstance(city_row, dict):
                            continue
                        city_name = re.sub(r"\s+", " ", str(city_row.get("name") or "")).strip()
                        if not city_name:
                            continue

                        location_url = (
                            "https://api.hertz.com/rest/location/country/"
                            f"{country_code}/city/{quote(city_name, safe='')}"
                        )
                        try:
                            location_response = self.load(location_url, headers, params, proxies)
                            print(country_code, city_name, "location status:", location_response.status_code)
                        except Exception as exc:
                            print(country_code, city_name, "location proxy failed:", exc)
                            try:
                                location_response = self.load(location_url, headers, params, {})
                                print(
                                    country_code,
                                    city_name,
                                    "location status:",
                                    location_response.status_code,
                                    "without proxy",
                                )
                            except Exception as exc:
                                print(country_code, city_name, "location direct failed:", exc)
                                continue

                        if location_response.status_code != 200:
                            continue

                        self.extraction(
                            location_response.text,
                            refid,
                            country,
                            websitecode,
                            source_name,
                            rows,
                            seen_location_codes,
                            country_code,
                            country_name,
                        )
                        time.sleep(0.1)

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
        self,
        html,
        refid,
        country,
        websitecode,
        source_name,
        rows,
        seen_location_codes,
        country_code="",
        country_name="",
    ):
        if not html:
            return

        text = html.strip()
        match = re.search(r"^[^(]+\(\s*(\{.*\})\s*\)\s*;?$", text, re.S)
        response_data = json.loads(match.group(1) if match else text)
        data = response_data.get("data", {}) if isinstance(response_data, dict) else {}
        if not isinstance(data, dict):
            return
        locations = data.get("locations", [])
        if not isinstance(locations, list):
            return

        for loc in locations:
            if not isinstance(loc, dict):
                continue

            location_code = re.sub(
                r"\s+", " ", str(loc.get("extendedOAGCode") or "")
            ).strip()
            location_term = re.sub(
                r"\s+", " ", str(loc.get("locationName") or "")
            ).strip()
            if (
                not location_code
                or not location_term
                or location_code in seen_location_codes
            ):
                continue

            city = re.sub(r"\s+", " ", str(loc.get("city") or "")).strip()
            oag_code = re.sub(r"\s+", " ", str(loc.get("OAGCode") or "")).strip()
            is_airport = True if str(loc.get("airport") or "").upper() == "Y" else False
            location_type = "Airport" if is_airport else "City"
            pickup_location = oag_code if is_airport and oag_code else location_term
            location_name = pickup_location
            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            seen_location_codes.add(location_code)
            row = {
                "id": refid,
                "source_name": source_name,
                "website_code": websitecode,
                "pickup_location": pickup_location,
                "location_country": country_code or country or country_name,
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
        # SC = hertz_4(0, 111, 111, "input_locations", "locations", False, "20")

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
        SC = hertz_4(
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
