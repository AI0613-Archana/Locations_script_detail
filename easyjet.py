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


class easyjet:
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
        self.websitecode = 23
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

    def get_proxy(self, used_proxies=None):
        if not self.proxyset:
            return {}
        used_proxies = used_proxies or set()
        available_proxies = []
        for row in self.proxyset:
            proxy_str = (row.get("proxy") or "").strip()
            if not proxy_str:
                continue
            proxy_url = proxy_str if "://" in proxy_str else f"http://{proxy_str}"
            if proxy_url not in used_proxies:
                available_proxies.append(proxy_url)

        if not available_proxies:
            return {}

        proxy_url = random.choice(available_proxies)
        return {"http": proxy_url, "https": proxy_url}

    def load(self, iata, headers, proxies):
        payload = {
            "@Target": "Production",
            "@PrimaryLangID": "en",
            "POS": {
                "Source": [
                    {
                        "@ERSP_UserID": "MP",
                        "@ISOCurrency": "INR",
                        "@ISOCountry": "IN",
                        "RequestorID": {
                            "@Type": "16",
                            "@ID": "512111",
                            "@ID_Context": "CARTRAWLER",
                        },
                    },
                    {
                        "RequestorID": {
                            "@Type": "16",
                            "@ID": "1171777887395696",
                            "@ID_Context": "CUSTOMERID",
                        }
                    },
                    {
                        "RequestorID": {
                            "@Type": "16",
                            "@ID": "1371781583760013",
                            "@ID_Context": "ENGINELOADID",
                        }
                    },
                    {
                        "RequestorID": {
                            "@Type": "16",
                            "@ID": "CTABE_V5:5.424.1",
                            "@Instance": "SabTIwlktlflR3TAEZgyp4JYFrQ=",
                            "@ID_Context": "VERSION",
                        }
                    },
                    {
                        "RequestorID": {
                            "@Type": "16",
                            "@ID": "3",
                            "@ID_Context": "BROWSERTYPE",
                        }
                    },
                ]
            },
            "@xmlns": "http://www.cartrawler.com/",
            "@Version": "1.000",
            "VehLocSearchCriterion": {
                "@ExactMatch": "true",
                "@ImportanceType": "Mandatory",
                "@ExcludeTypes": "airport",
                "PartialText": {
                    "@Sort": "1",
                    "@Size": 15,
                    "@POITypes": "1,8",
                    "@MaxPerPOIType": 7,
                    "#text": iata,
                },
            },
            "Window": {
                "@name": "easyJet.com Car Rentals",
                "@engine": "CTABE-V5.0",
                "@svn": "5.424.1-51",
                "@product": "CarWeb",
                "@region": "en",
                "@device": "DESKTOPWEB",
                "@CTMVTScenario": "502122",
                "@CTMVTBucket": "ABE.A",
                "@CTMVTVersion": "3",
                "@CTMVTRequestParams": "",
                "UserAgent": headers["user-agent"],
                "BrowserName": "chrome",
                "BrowserVersion": "147",
                "URL": "https://cars.easyjet.com/",
            },
            "TPA_Extensions": {
                "Tracking": {
                    "SessionID": "1171781527912536",
                    "CustomerID": "1171777887395696",
                    "EngineLoadID": "1371781583760013",
                }
            },
        }
        params = {
            "msg": json.dumps(payload, separators=(",", ":")),
            "type": "CT_VehLocSearchRQ",
        }
        session = TLSSession(
            profile="chrome_120",
            proxies=proxies,
            on_block="none",
            max_retries=0,
        )
        # session=TLSSession()
        return session.get(
            "https://otageo.cartrawler.com/cartrawlerota/json",
            params=params,
            headers=headers,
            timeout=30,
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
            rows = []
            seen_location_codes = set()
            headers = {
                "accept": "application/json, text/plain, */*",
                "origin": "https://cars.easyjet.com",
                "referer": "https://cars.easyjet.com/",
                "user-agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
                ),
            }

            try:

                def fetch_iata(iata):
                    attempted_proxies = set()
                    for attempt in range(1, 2):
                        proxies = self.get_proxy(attempted_proxies)
                        if not proxies:
                            break
                        attempted_proxies.add(proxies.get("https", ""))
                        try:
                            response = self.load(iata, headers, proxies)
                            print("IATA:", iata, "status:", response.status_code)
                            if response.status_code == 200:
                                return iata, response.status_code, response.text
                            return iata, response.status_code, ""
                        except Exception as exc:
                            print(
                                "IATA:",
                                iata,
                                "proxy failed:",
                                proxies.get("https", ""),
                                "attempt:",
                                attempt,
                                "error:",
                                exc,
                            )

                    try:
                        response = self.load(iata, headers, {})
                        print(
                            "IATA:",
                            iata,
                            "status:",
                            response.status_code,
                            "without proxy",
                        )
                        if response.status_code == 200:
                            return iata, response.status_code, response.text
                        return iata, response.status_code, ""
                    except Exception as exc:
                        print("IATA:", iata, "without proxy failed:", exc)

                    return iata, None, ""

                iata_codes = list(airportsdata.load("IATA").keys())
                # for sample input for debuging
                # iata_codes=['ATL','HNL']
                with ThreadPoolExecutor(max_workers=100) as executor:
                    futures = [executor.submit(fetch_iata, iata) for iata in iata_codes]
                    for future in as_completed(futures):
                        iata, status_code, response_text = future.result()
                        if status_code != 200 or not response_text:
                            continue
                        self.extraction(
                            response_text,
                            refid,
                            country,
                            websitecode,
                            source_name,
                            rows,
                            seen_location_codes,
                        )

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

        data = json.loads(html)
        location_details = data.get("VehMatchedLocs", {}).get("LocationDetail", [])
        if isinstance(location_details, dict):
            location_details = [location_details]
        if not isinstance(location_details, list):
            return

        for location in location_details:
            if not isinstance(location, dict):
                continue

            locationname = re.sub(r"\s+", " ", str(location.get("@Name") or "")).strip()
            locationid = re.sub(r"\s+", " ", str(location.get("@Code") or "")).strip()
            externalid = re.sub(
                r"\s+", " ", str(location.get("@ExternalLocId") or "")
            ).strip()
            if locationid == "0":
                locationid = externalid or locationid
            if not locationname or not locationid or locationid in seen_location_codes:
                continue

            seen_location_codes.add(locationid)
            airport_code = re.sub(
                r"\s+", " ", str(location.get("@AirportCode") or "")
            ).strip()
            airport = str(location.get("@Type") or "") == "1"
            if airport and airport_code:
                pickup_location = airport_code
            else:
                pickup_location = locationname

            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            city = re.split(r"\s+-\s+|,", locationname, maxsplit=1)[0].strip()
            region = re.sub(r"\s+", " ", str(location.get("@StateCode") or "")).strip()
            locationcountry = re.sub(
                r"\s+", " ", str(location.get("@CountryCode") or country or "")
            ).strip()
            location_type = "airport" if airport else "city"

            row = {
                "id": refid,
                "source_name": source_name,
                "website_code": websitecode,
                "pickup_location": pickup_location,
                "location_country": locationcountry,
                "location_code": locationid,
                "is_airport": airport,
                "created_date": created_date,
                "location_type": location_type,
                "city": city,
                "region": region,
                "priority_level": "",
                "location_term": "",
                "location_name": locationname,
            }
            # print(row)
            rows.append(row)


if __name__ == "__main__":
    SC = None
    try:
        # SC = easyjet(0, 137, 137, "input_locations", "locations", False, "20")

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
        SC = easyjet(
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
