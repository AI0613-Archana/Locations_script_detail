# -*- coding: utf-8 -*-
import json
import os
import random
import re
import sys
import time
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


class webjet:
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
        self.websitecode = 62
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
        proxy_str = (
            self.proxyset[random.randrange(0, len(self.proxyset))].get("proxy") or ""
        ).strip()
        if not proxy_str:
            return {}
        proxy_url = proxy_str if "://" in proxy_str else f"http://{proxy_str}"
        return {"http": proxy_url, "https": proxy_url}

    def load(self, url, headers, params, proxies):
        cookies = {
            "xid": "17774539383a639nLKD3a1c817de39e46f334c7d3d18266a6a6",
            "_vwo_uuid_v2": "D05641ADE05634911E12F31398A3E1D07|cf48367831d36af75b6b15661c8a0978",
            "wjab_supersonic": "B",
            "FPID": "FPID2.3.J8Mb2f5JyWd1U%2F8RZeIm6D9A4Y%2Bij%2FQvuzDUUq9dsYk%3D.1777453942",
            "orrid": "69f1cba6bac88",
            "bfa": "eyJpdiI6InpFWjNQMUZIUlI3UzAxa1djMkgyd2c9PSIsInZhbHVlIjoiaHpUa0N4NmtJc2EvSU9OS0tabEM0SEVZVDNrOCt5OUoxMEQxdE8xazI1WT0iLCJtYWMiOiJjOGE2ZWZhMjUxYjUzZDhjYjViOGZmYjAzNDQzZDYwNmQ5ZjI5Yzc2ZGM3MWMwNTU3ZjcyYmIxZDQ5NDA0NTQ0IiwidGFnIjoiIn0%3D",
            "_pin_unauth": "dWlkPU9USXdPREF3TURjdE9HWXpaUzAwTURObUxUZzFaRFV0TldVeFl6TTBNR1JsTkRWaQ",
            "fpcid": "7662571018753961144_FP",
            "_gcl_au": "1.1.409114895.1778835062",
            "_fbp": "fb.2.1778835062483.54138032741611583",
            "FPAU": "1.1.409114895.1778835062",
            "_hjSessionUser_6402544": "eyJpZCI6Ijk4NzU1NmQ4LWQwN2QtNWUwZC1iOTAxLTZiZDBlM2I2OGE5NCIsImNyZWF0ZWQiOjE3Nzc0NTM5OTY1NzQsImV4aXN0aW5nIjp0cnVlfQ==",
            "wjab_flights_csm": "B",
            "_pin_unauth": "dWlkPU9USXdPREF3TURjdE9HWXpaUzAwTURObUxUZzFaRFV0TldVeFl6TTBNR1JsTkRWaQ",
            "_cfuvid": "ya8DSNln_ylfUQuFJffNJIXqGsEadiyJ8WcslWG3CKI-1781612829.9866807-1.0.1.1-DUr.dTbHZ03H4FPftjmOsBGrKcRNBALxI2eku0Fdlfo",
            "ab.storage.deviceId.eec5d378-46b3-4df8-9f1d-faa3d1e3c5f1": "g%3A98241faf-7a6d-6357-4996-a30f507b56fe%7Ce%3Aundefined%7Cc%3A1777453942158%7Cl%3A1781612834400",
            "_gid": "GA1.3.1736180333.1781612835",
            "__gads": "ID=14c6654a53d07db8:T=1781513321:RT=1781612835:S=ALNI_Mb7FjNvYa6f7BHdW3JdIBtG-UGC_w",
            "__gpi": "UID=0000146c2fb16ed2:T=1781513321:RT=1781612835:S=ALNI_MZ45xzQHx9jt1q_CuWYSVMgkLrwVQ",
            "__eoi": "ID=25abe496b87251f7:T=1781513321:RT=1781612835:S=AA-AfjaYT7zrXXEW4CaKqy8S4Zwm",
            "_ts": "referral:www.webjet.com.au/|direct|referral:www.google.com/|direct",
            "__utmz": "utmccn=(not set)",
            "__utmzzses": "1",
            "FPLC": "fkmdOwUyWvIBu9OXTtj6%2BAaCIc3%2BrNNsCchUmXUNiu5x%2BvCMETBGuwMYaIn%2Fa4ocHsH8mrWkeoSVeKkVitHkpf8aWtHApup0W83T89Zg0C%2BRtL68069AKHdqxacC7Q%3D%3D",
            "FPGSID": "1.1781612840.1781612840.G-NRSDQT2WKC.TbC_wgskZTVEfJFOXM7oMg",
            "_hjSession_6539845": "eyJpZCI6IjFjOWUxNjNlLWU5MzYtNDAwMC1hYmRkLWY2OGRlMWExYWUwNSIsImMiOjE3ODE2MTI4NzM4NzcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=",
            "tabOffset": "1",
            "cf_clearance": "nU1Y1EBx5P7RFP7ICgIBG5s9TTs4KEkP4_FF28zXaVA-1781612876-1.2.1.1-pNZPQi8Cu4QjyQBH3fZsDIjZFHtA.BgxdxXrL1q6QKq.WL5xuNIRXM64XFi_9QKMsuU6uEIzeSDga1qjm5mZind7fEmSjSnf6dRZirc5Jg4p1niHPrweK9589Vqv_nghHzkpwjDYJk_Ld2tfapRHR_jVXzK0ekZD_XDC8GEqlqEL7BZKFgBqWFvMiWJDoSPaqCyIlu3Tq8Zm_7gJlrG.WlT26kwqb9lFKfa95_c3aFZViS9jxP16TvtXXfb5mm2_1v3ZHkFs6Wr0nGgjZaU6mA.2UVW0N6qHQTkt1uP_2t3GzX_vsUBqCyiOI5MdxqfLZWqB482zMnRhCCwkAZsoDA",
            "__cf_bm": "l8SDSL4U7yvksiOFJNZ6XEfeKRlblJgbNKuk5LkGaAY-1781612876.7771058-1.0.1.1-PzgngOEn8jMRWbjs9YiedmVIGZVnKoVwY.eRHVqmXo75b5T7UAJwalX5wP9x225bl1ReZlAFOLp4UPyJe2hSipGGAPZAFJvK2qg7PlYWTPgaWUukRHfBwTqmvEV.Zp4HtaFMLgqKlzzp9cXj14gf3g",
            "_hjSessionUser_6539845": "eyJpZCI6IjQ4ZDI5ZWEzLTA4ZDgtNTJjMi04M2ZlLThkYjhlM2RkZTExMCIsImNyZWF0ZWQiOjE3ODE2MTI4NzM4NzYsImV4aXN0aW5nIjp0cnVlfQ==",
            "XSRF-TOKEN": "eyJpdiI6IjY3ZGRzQ0xEVitZK1FMUTBOWEZtSWc9PSIsInZhbHVlIjoiTEo4RDFzNTVIVHdBT2FOTWFmYmlvNWZ1eXZEVVQwYkpWU01kMDltNVdTZ09UaXErUTJzd1k2bWNKTWNjdG42RENZbFQ5UzhPcjlqMlF6YTlhSXZFdW1LdllwWGVoREdxVXZFWU54OXRFQTV1T0xySlVSTlVzdXhaZm1GandtOGQiLCJtYWMiOiJjNjdmYjAzNjU1YTcyNmUyODkxMjYzZDQ4YmU4N2QyMmNkYThkYTM1NzFlMzQ0YTU0YmJkOTlkMThlMjQ1N2IyIiwidGFnIjoiIn0%3D",
            "zsession": "eyJpdiI6Im1LZjJRaW8yRGNaNmxtOEJ1aTdSN3c9PSIsInZhbHVlIjoiWjRRcTRqVi9iWU52MEsveEVuK2poRm9yQWFJRU5Tc1ZDQ2JCcDBjRThvQkcvaGJsTEFMelFrMC9aRWlpS3JKMGJPei8yMHp0Qi83cG0wVWdFUWsxZ3RKbzVCU0Y0UkUvaitpTGhLZnR1TXhaQStBRjh1V0JNUTdzT3MzekhuQ0EiLCJtYWMiOiIzMjJhNGJmOGQ0NTJlZTg4Y2VlN2YxYTMyMmIxZjc4Y2FlMTc4YmVmZTNkNmEwOTcwY2M5ZGUyZGQ4OGNhMWMyIiwidGFnIjoiIn0%3D",
            "ab.storage.sessionId.eec5d378-46b3-4df8-9f1d-faa3d1e3c5f1": "g%3Ab079ef38-7373-4f98-8c99-f524254af0ce%7Ce%3A1781614685414%7Cc%3A1781612834398%7Cl%3A1781612885414",
            "_ga_NRSDQT2WKC": "GS2.1.s1781612840$o6$g1$t1781612885$j15$l0$h1140907752",
            "_ga_QDX9S6HCEH": "GS2.1.s1781612840$o4$g1$t1781612886$j14$l0$h0",
            "_ga": "GA1.1.341770466.1777453942",
        }
        return requests.get(
            url,
            params=params,
            headers=headers,
            cookies=cookies,
            proxies=proxies,
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
            source_url = (
                result["source_url"] or "https://cars.webjet.com.au/ajax/suggest"
            )
            rows = []
            seen_location_codes = set()
            headers = {
                "accept": "application/json, text/javascript, */*; q=0.01",
                "accept-language": "en-US,en;q=0.9",
                "referer": "https://cars.webjet.com.au/australia-car-rental",
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
            params = {"query": ""}

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
                    self.extraction(
                        response.text,
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

        locations = json.loads(html)
        if isinstance(locations, dict):
            locations = locations.get("data") or locations.get("locations") or []
        if not isinstance(locations, list):
            return

        for location in locations:
            if not isinstance(location, dict):
                continue

            location_code = re.sub(r"\s+", " ", str(location.get("id") or "")).strip()
            location_name = re.sub(
                r"\s+",
                " ",
                str(
                    location.get("text")
                    or location.get("title")
                    or location.get("name")
                    or ""
                ),
            ).strip()
            if (
                not location_code
                or not location_name
                or location_code in seen_location_codes
            ):
                continue

            location_type_source = (
                re.sub(r"\s+", " ", str(location.get("type") or "")).strip().lower()
            )
            is_airport = True if "airport" in location_type_source else False
            iata = (
                re.sub(
                    r"\s+",
                    " ",
                    str(
                        location.get("airport_code")
                        or location.get("iata")
                        or location.get("bm_code")
                        or ""
                    ),
                )
                .strip()
                .upper()
            )
            if not iata and is_airport:
                iata_match = re.search(r"\(([A-Z0-9]{3,4})\)", location_name)
                if iata_match:
                    iata = iata_match.group(1)
            pickup_location = iata if is_airport and iata else location_name
            name_parts = [
                re.sub(r"\s+", " ", part).strip()
                for part in location_name.split(",")
                if re.sub(r"\s+", " ", part).strip()
            ]
            city = re.sub(
                r"\s+",
                " ",
                str(location.get("city_name") or location.get("city") or ""),
            ).strip()
            if not city and name_parts:
                city = name_parts[0]
                city = re.sub(r"\s*\([A-Z0-9]{3,4}\)\s*", " ", city).strip()
                city = re.sub(r"\s+Airport$", "", city, flags=re.I).strip()
            region = re.sub(
                r"\s+",
                " ",
                str(location.get("state_name") or location.get("region") or ""),
            ).strip()
            if not region and len(name_parts) > 2:
                region = name_parts[-2]
            location_country = re.sub(
                r"\s+",
                " ",
                str(
                    location.get("country_code")
                    or location.get("country")
                    or country
                    or ""
                ),
            ).strip()
            location_type = "Airport" if is_airport else "City"
            if "station" in location_type_source:
                location_type = "Railway Station"
            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            seen_location_codes.add(location_code)
            row = {
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
                "location_term": "",
                "location_name": location_name,
            }
            rows.append(row)


if __name__ == "__main__":
    SC = None
    try:
        # SC = webjet(2, 151, 151, "input_locations", "locations", False, "20")

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
        SC = webjet(
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
