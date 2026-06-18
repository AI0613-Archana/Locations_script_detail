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

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}


class tuicars:
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
        self.websitecode = 61
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
        from tls_chameleon import TLSSession
        session=TLSSession()
        return session.get(
            url,
            params=params,
            timeout=30,
            headers=headers,
            proxies=proxies,
            impersonate="chrome",
        )

    def load_iata_with_retry(self, url, headers, iata):
        last_response = None
        last_error = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self.load(
                    url,
                    headers,
                    {"search": iata},
                    self.get_proxy(),
                )
                print(iata, response.status_code, "attempt", attempt)
                if response.status_code == 200:
                    return response

                last_response = response
                last_error = f"HTTP {response.status_code}"
            except Exception as exc:
                last_error = exc
                print(iata, "attempt", attempt, "failed:", exc)

            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)

        if last_response is not None:
            print(iata, "retry exhausted:", last_error)
            return last_response

        raise RuntimeError(f"{iata} retry exhausted: {last_error}") from (
            last_error if isinstance(last_error, Exception) else None
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
        airports = airportsdata.load("IATA")
        for result in resultset:
            print(result)
            refid = result["id"]
            websitecode = result["websitecode"]
            source_name = result["source_name"]
            country = result["country"]
            source_url = (
                result["source_url"]
                or "https://www.tuicars.com/ibe/api/stations/search"
            )
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "Referer": "https://www.tuicars.com/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
                ),
                "age": "31-64",
                "agk": "tuicars",
                "clear_cache": "false",
                "dropOffDate": "21.07.2026",
                "dropOffTime": "10:00",
                "lang": "de_DE",
                "mdtest": "false",
                "pickUpDate": "14.07.2026",
                "pickUpTime": "10:00",
                "promoCode": "",
                "rebuild_cache": "false",
                "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Linux"',
            }

            try:
                rows = []
                failed_iatas = []
                seen_location_codes = set()
                futures = {}
                with ThreadPoolExecutor(max_workers=100) as executor:
                    for iata in airports:
                        futures[
                            executor.submit(
                                self.load_iata_with_retry,
                                source_url,
                                headers,
                                iata,
                            )
                        ] = iata

                    for future in as_completed(futures):
                        iata = futures[future]
                        try:
                            response = future.result()
                        except Exception as exc:
                            print(iata, "failed:", exc)
                            failed_iatas.append(iata)
                            continue

                        if response.status_code != 200:
                            print(iata, "failed after retries:", response.status_code)
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
        self, html, refid, country, websitecode, source_name, rows, seen_location_codes
    ):
        if not html:
            return

        response_data = json.loads(html)
        response_list = response_data.get("response", [])
        if not isinstance(response_list, list):
            return

        for result in response_list:
            if not isinstance(result, dict) or result.get("type") != "station":
                continue

            entries = result.get("entries", [])
            if not isinstance(entries, list):
                continue

            for entry in entries:
                if not isinstance(entry, dict) or entry.get("t") != "ap":
                    continue

                location_code = re.sub(
                    r"\s+", " ", str(entry.get("i") or "")
                ).strip()
                location_name = re.sub(
                    r"\s+", " ", str(entry.get("n") or "")
                ).strip()
                if (
                    not location_code
                    or not location_name
                    or location_code in seen_location_codes
                ):
                    continue

                created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                seen_location_codes.add(location_code)
                row = {
                    "id": refid,
                    "source_name": source_name,
                    "website_code": websitecode,
                    "pickup_location": location_name,
                    "location_country": country or "",
                    "location_code": location_code,
                    "is_airport": True,
                    "created_date": created_date,
                    "location_type": "Airport",
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
        # SC = tuicars(0, 167, 167, "input_locations", "locations", False, "20")

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
        SC = tuicars(
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
