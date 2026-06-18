# -*- coding: utf-8 -*-
import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import airportsdata
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


class sunnycars:
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
        self.websitecode = 56
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

    def load(self, url, headers, params, proxies, iata):
        response = None
        for attempt in range(8):
            try:
                time.sleep(random.uniform(0.5, 1.5))
                from tls_chameleon import TLSSession
                session = TLSSession(proxies=proxies)
                response = session.get(
                    url,
                    params=params,
                    timeout=30,
                    headers=headers,
                    proxies=proxies,
                    impersonate="chrome",
                )
                if response.status_code == 200:
                    print(iata,response.status_code)
                    return response
                if response.status_code in (429,) or response.status_code >= 500:
                    wait = min(2**attempt, 120)
                    print(iata, "HTTP", response.status_code, "retrying in", wait)
                    time.sleep(wait)
                    continue
                print(iata, "HTTP", response.status_code)
                return response
            except Exception as exc:
                wait = min(2**attempt, 120)
                print(iata, exc, "retrying in", wait)
                time.sleep(wait)
        return response

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
        airports = airportsdata.load("IATA")
        for result in resultset:
            print(result)
            refid = result["id"]
            websitecode = result["websitecode"]
            source_name = result["source_name"]
            country = result["country"]
            source_url = result["source_url"] or "https://www.sunnycars.de/api/v1/regions"
            headers = {
                "sec-ch-ua-platform": '"Linux"',
                "Referer": "https://www.sunnycars.de/",
                "Accept-Language": "en-US",
                "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
                "x-site-domain": "de",
                "sec-ch-ua-mobile": "?0",
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
            }

            try:
                rows = []
                failed_iatas = []
                seen_location_codes = set()
                futures = {}
                with ThreadPoolExecutor(max_workers=100) as executor:
                    for iata in airports:
                        params = {
                            "affiliatekey": "55",
                            "source": "DE",
                            "search": iata,
                        }
                        futures[
                            executor.submit(
                                self.load,
                                source_url,
                                headers,
                                params,
                                self.get_proxy(),
                                iata,
                            )
                        ] = iata

                    for future in as_completed(futures):
                        iata = futures[future]
                        try:
                            response = future.result()
                        except Exception as exc:
                            print(iata, exc)
                            failed_iatas.append(iata)
                            continue

                        if not response or response.status_code != 200:
                            failed_iatas.append(iata)
                            continue

                        self.extraction(
                            response.text,
                            refid,
                            country,
                            websitecode,
                            source_name,
                            rows,
                            seen_location_codes,
                            iata,
                        )

                print("Extracted:", len(rows))
                print("Failed IATAs:", len(failed_iatas))
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
        iata,
    ):
        if not html:
            return

        response_data = json.loads(html)
        locations = response_data.get("data", [])
        if not isinstance(locations, list):
            return

        domain_data = response_data.get("meta", {}).get("query", {}).get("source", [""])
        domain = domain_data[0] if isinstance(domain_data, list) and domain_data else ""
        for location in locations:
            if not isinstance(location, dict):
                continue

            location_code = re.sub(
                r"\s+", " ", str(location.get("id") or "")
            ).strip()
            location_name = re.sub(
                r"\s+", " ", str(location.get("name") or "")
            ).strip()
            if (
                not location_code
                or not location_name
                or location_code in seen_location_codes
            ):
                continue

            location_type_source = re.sub(
                r"\s+", " ", str(location.get("type") or "")
            ).strip()
            country_data = location.get("country") if isinstance(location.get("country"), dict) else {}
            country_name = re.sub(
                r"\s+", " ", str(country_data.get("name") or country or "")
            ).strip()
            is_airport = True if location_type_source.lower() == "airport" else False
            pickup_location = iata if is_airport else location_name
            location_type = "Airport" if is_airport else location_type_source or "City"
            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            seen_location_codes.add(location_code)
            row = {
                "id": refid,
                "source_name": source_name,
                "website_code": websitecode,
                "pickup_location": pickup_location,
                "location_country": country_name,
                "location_code": location_code,
                "is_airport": is_airport,
                "created_date": created_date,
                "location_type": location_type,
                "city": "",
                "region": "",
                "priority_level": "",
                "location_term": location_name,
                "location_name": location_name,
            }
            rows.append(row)


if __name__ == "__main__":

    SC = None
    try:
        # SC = sunnycars(0, 166, 166, "input_locations", "locations", False, "20")

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
        SC = sunnycars(
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
