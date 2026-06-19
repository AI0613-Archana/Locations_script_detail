# -*- coding: utf-8 -*-
import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import psycopg2
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


class hertz_9:
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
        proxy_str = re.sub(r"(rentalcars-res-)[A-Z]{2}", r"\1IL", proxy_str)
        proxy_url = proxy_str if "://" in proxy_str else f"http://{proxy_str}"
        return {"http": proxy_url, "https": proxy_url}

    def normalize_url(self, url):
        url = (url or "").strip()
        if url.startswith("ttps://"):
            url = "h" + url
        if not url:
            return "https://www.hertz.co.il//Handlers/FillAutoComplete.ashx"
        if not re.match(r"^https?://", url, flags=re.I):
            url = "https://" + url.lstrip("/")
        return url

    def load(self, url, headers, params, proxies):
        url = self.normalize_url(url)
        return requests.get(
            url,
            params=params,
            timeout=90,
            headers=headers,
            proxies=proxies,
            impersonate="chrome",
        )

    def fetch_station_response(self, source_url, headers, country_info, proxies):
        station_params = {
            "action": "station",
            "filter": "",
            "source": "dFromStation",
            "country": country_info["country"],
            "state": country_info["state"],
        }
        try:
            station_response = self.load(
                source_url, headers, station_params, proxies
            )
            print(country_info["fcid"], "station status:", station_response.status_code)
            return country_info, station_response.status_code, station_response.text
        except Exception as exc:
            print(country_info["fcid"], "station error:", exc)
            try:
                station_response = self.load(
                    source_url, headers, station_params, {}
                )
                print(
                    country_info["fcid"],
                    "station status:",
                    station_response.status_code,
                    "without proxy",
                )
                return country_info, station_response.status_code, station_response.text
            except Exception as exc:
                print(country_info["fcid"], "direct station error:", exc)
                return country_info, 0, ""

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
            source_url = self.normalize_url(
                result["source_url"]
                or "https://www.hertz.co.il//Handlers/FillAutoComplete.ashx"
            )

            headers = {
                "accept": "application/json, text/javascript, */*; q=0.01",
                "accept-language": "en-US,en;q=0.9",
                "priority": "u=1, i",
                "referer": "https://www.hertz.co.il/",
                "sec-ch-ua": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Linux"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
                ),
                "x-requested-with": "XMLHttpRequest",
            }

            try:
                rows = []
                seen_location_codes = set()
                proxies = self.get_proxy()
                country_params = {
                    "action": "country",
                    "filter": "",
                    "source": "dFromCountry",
                }
                try:
                    country_response = self.load(
                        source_url, headers, country_params, proxies
                    )
                    print("Country status:", country_response.status_code)
                except Exception as exc:
                    print(
                        "Proxy failed:",
                        proxies.get("https", ""),
                        "error:",
                        exc,
                    )
                    country_response = self.load(source_url, headers, country_params, {})
                    print("Country status:", country_response.status_code, "without proxy")

                if country_response.status_code != 200:
                    self.update(2, refid)
                    continue

                country_payload = json.loads(country_response.text or "[]")
                country_data = country_payload.get("data", []) if isinstance(country_payload, dict) else country_payload
                if not isinstance(country_data, list):
                    country_data = []

                countries = []
                for country_row in country_data:
                    raw_id = ""
                    country_name = ""
                    if isinstance(country_row, dict):
                        for key in ("Item1", "Value", "value", "id"):
                            raw_id = re.sub(r"\s+", " ", str(country_row.get(key) or "").replace("\xa0", " ")).strip()
                            if raw_id:
                                break
                        for key in ("Item2", "Text", "text", "label", "name"):
                            country_name = re.sub(r"\s+", " ", str(country_row.get(key) or "").replace("\xa0", " ")).strip()
                            if country_name:
                                break
                    elif isinstance(country_row, (list, tuple)):
                        if len(country_row) > 0:
                            raw_id = re.sub(r"\s+", " ", str(country_row[0] or "").replace("\xa0", " ")).strip()
                        if len(country_row) > 1:
                            country_name = re.sub(r"\s+", " ", str(country_row[1] or "").replace("\xa0", " ")).strip()

                    if not raw_id:
                        continue
                    if ";" in raw_id:
                        country_code, state_code = raw_id.split(";", 1)
                        country_code = re.sub(r"\s+", " ", country_code).strip()
                        state_code = re.sub(r"\s+", " ", state_code).strip()
                    else:
                        country_code = raw_id
                        state_code = ""
                    fcid = f"{country_code};{state_code}" if state_code else country_code
                    country_label = country_name.split(",", 1)[0].strip()
                    countries.append(
                        {
                            "country": country_code,
                            "state": state_code,
                            "fcid": fcid,
                            "fcin": country_label,
                        }
                    )

                with ThreadPoolExecutor(max_workers=50) as executor:
                    futures = [
                        executor.submit(
                            self.fetch_station_response,
                            source_url,
                            headers,
                            country_info,
                            proxies,
                        )
                        for country_info in countries
                    ]
                    for future in as_completed(futures):
                        country_info, status_code, response_text = future.result()
                        if status_code != 200:
                            continue
                        self.extraction(
                            response_text,
                            refid,
                            country,
                            websitecode,
                            source_name,
                            rows,
                            seen_location_codes,
                            country_info,
                        )

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
        country_info=None,
    ):
        if not html:
            return

        response_data = json.loads(html)
        locations = response_data.get("data", []) if isinstance(response_data, dict) else response_data
        if not isinstance(locations, list):
            return

        country_info = country_info or {}
        fcid = country_info.get("fcid", "")
        country_name = country_info.get("fcin", "") or country or ""
        for location in locations:
            if not isinstance(location, (dict, list, tuple)):
                continue

            station_id = ""
            station_name = ""
            if isinstance(location, dict):
                for key in ("Item1", "Value", "value", "id", "Code", "code"):
                    station_id = re.sub(r"\s+", " ", str(location.get(key) or "").replace("\xa0", " ")).strip()
                    if station_id:
                        break
                for key in ("Item2", "Text", "text", "label", "name", "Name"):
                    station_name = re.sub(r"\s+", " ", str(location.get(key) or "").replace("\xa0", " ")).strip()
                    if station_name:
                        break
            else:
                if len(location) > 0:
                    station_id = re.sub(r"\s+", " ", str(location[0] or "").replace("\xa0", " ")).strip()
                if len(location) > 1:
                    station_name = re.sub(r"\s+", " ", str(location[1] or "").replace("\xa0", " ")).strip()

            station_name = re.sub(
                r"^(?:red|green|blue|yellow|orange|black|white)_",
                "",
                station_name,
                flags=re.I,
            ).strip()
            if not station_id or not station_name or not fcid:
                continue

            location_code = f"{station_id}|{fcid}|{country_name}"
            if location_code in seen_location_codes:
                continue
            if 'Airport' in station_name:
                is_airport=True
            else:
                is_airport=False
            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            seen_location_codes.add(location_code)
            row = {
                "id": refid,
                "source_name": source_name,
                "website_code": websitecode,
                "pickup_location": station_name,
                "location_country": "IL",
                "location_code": location_code,
                "is_airport": is_airport,
                "created_date": created_date,
                "location_type": "Airport",
                "city": "",
                "region": country_info.get("state", ""),
                "priority_level": "",
                "location_term": station_name,
                "location_name": station_name,
            }
            rows.append(row)


if __name__ == "__main__":

    SC = None
    try:
        # SC = hertz_9(0, 111, 111, "input_locations", "locations", False, "20")

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
        SC = hertz_9(
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
