# -*- coding: utf-8 -*-
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import airportsdata
import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor, execute_values

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}

SOURCE_NAME = "fti"
WEBSITE_CODE = 32
SUGGESTER_URL = "https://cars.ypsilon.net/proxy/suggester"
THREAD_COUNT = 20

session = requests.Session()

cookies = {
    "AMCVS_00CE231A52DFED3F0A490D44%40AdobeOrg": "1",
    "AMCV_00CE231A52DFED3F0A490D44%40AdobeOrg": "179643557%7CMCIDTS%7C20623%7CMCMID%7C44074603059851422293212255630604763388%7CMCAAMLH-1782363116%7C12%7CMCAAMB-1782363116%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1781765516s%7CNONE%7CMCSYNCSOP%7C411-20630%7CvVersion%7C5.5.0",
    "s_ppn": "mietwagen%3Asuche",
    "s_cc": "true",
    "consentSettingsDTO": "{%22cmp%22:1%2C%22tms%22:1%2C%22necessary%22:1%2C%22analytics%22:1%2C%22hintsOffers%22:1%2C%22abTesting%22:1%2C%22extendedAnalytics%22:1%2C%22crossDomainDTO%22:1%2C%22advancedProfiling%22:0%2C%22thirdPartyUserDetection%22:0%2C%22googleMaps%22:1%2C%22youTube%22:1%2C%22survey%22:1%2C%22analyticsOptimization%22:1%2C%22dataAffiliate%22:1%2C%22comfortCookies%22:1%2C%22embeddedContent%22:1%2C%22analyticsVisits%22:1%2C%22dataCoop%22:1%2C%22liveChat%22:1%2C%22offersCoop%22:1%2C%22remarketingSocial%22:1%2C%22remarketingThird%22:1%2C%22voyager%22:1%2C%22monitoringErrors%22:1%2C%22feederAnalytics%22:1%2C%22streaming%22:0%2C%22app%22:1%2C%22appPerformance%22:1%2C%22thinglink%22:0%2C%22instagram%22:0%2C%22opinionStage%22:0%2C%22yumpu%22:0%2C%22offersThirdParty%22:0%2C%22vimeo%22:0%2C%22spotify%22:0%2C%22reason%22:1}",
    "_gid": "GA1.2.1303790333.1781758327",
    "_gat_gtag_UA_45809059_6": "1",
    "s_adform": "dtodertourprod%2Cdtoglobalmstprod",
    "s_ppvl": "mietwagen%253Asuche%2C71%2C71%2C568%2C1854%2C568%2C1920%2C1200%2C1%2CP",
    "_ga_B53FNTDYSD": "GS2.1.s1781758326$o1$g1$t1781758342$j44$l0$h0",
    "_ga": "GA1.2.614232716.1781758327",
    "s_ppv": "mietwagen%253Asuche%2C100%2C71%2C805%2C1854%2C568%2C1920%2C1200%2C1%2CP",
    "s_sq": "dtodertourprod%252Cdtoglobalmstprod%3D%2526pid%253Dmietwagen%25253Asuche%2526pidt%253D1%2526oid%253Dfunctiontb%252528%252529%25257B%25257D%2526oidt%253D2%2526ot%253DTEXT",
}

headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "origin": "https://cars.ypsilon.net",
    "priority": "u=1, i",
    "referer": "https://cars.ypsilon.net/de_DE?aid=dertourmietwagen&sid=71VcZ8hCnmlvoQYyc8pL",
    "sec-ch-ua": '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    ),
}


class fti:

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

    def load(self, search_value, source_url):
        return session.post(
            source_url,
            cookies=cookies,
            headers=headers,
            json={
                "locale": "de_DE",
                "phrase": str(search_value).lower(),
            },
            timeout=30,
        )

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
            websitecode = result["websitecode"]
            source_name = result.get("source_name", SOURCE_NAME)
            country = result.get("country", "") or result.get("location_country", "")
            source_url = result.get("source_url") or SUGGESTER_URL
            print("refid", refid, "source_url", source_url)
            try:
                rows = []
                seen_keys = set()
                airport_map = airportsdata.load("IATA")
                airport_codes = [
                    code
                    for code, airport_data in airport_map.items()
                    if airport_data.get("country") == country
                ]
                if not airport_codes:
                    airport_codes = list(airport_map.keys())
                print("airport seed count", len(airport_codes), "for country", country)
                with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
                    futures = {
                        executor.submit(self.load, airport_code, source_url): airport_code
                        for airport_code in airport_codes
                    }
                    for future in as_completed(futures):
                        airport_code = futures[future]
                        try:
                            response = future.result()
                            print(
                                "search_value",
                                airport_code,
                                "status",
                                response.status_code,
                            )
                            if response.status_code != 200:
                                continue
                            extracted_rows = self.extraction(
                                response.json(),
                                refid,
                                source_name,
                                websitecode,
                            )
                            for row in extracted_rows:
                                unique_key = (
                                    row["location_code"],
                                    row["location_term"],
                                    row["location_name"],
                                )
                                if unique_key in seen_keys:
                                    continue
                                seen_keys.add(unique_key)
                                rows.append(row)
                        except Exception:
                            continue
                if rows:
                    self.insert(rows)
                    self.update(1, refid)
                else:
                    self.update(2, refid)
            except Exception:
                self.update(2, refid)

    def extraction(self, response_data, refid, source_name, websitecode):
        rows = []
        created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        for item in response_data.get("data", []):
            location_name = item.get("location", "").strip()
            location_code = item.get("locationKey", "").strip()
            raw_location_type = item.get("locationType", "").strip()
            value = str(raw_location_type or "").upper()
            if value == "AIRP":
                location_type = "airport"
            elif value in {"PPLA", "PPL", "ADM1"}:
                location_type = "city"
            else:
                location_type = value.lower()
            is_airport = value == "AIRP"
            location_country = item.get("countryCode", "").strip()
            city = item.get("city", "").strip()
            region = item.get("region", "").strip()
            location_term = item.get("iata", "").strip()

            if not location_name or not location_code:
                continue

            row = {
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
            rows.append(row)
        return rows


if __name__ == "__main__":
    RETRY = 1
    while RETRY < 20:
        SC = None
        try:
            SC = fti(0, 160, 160, "input_locations", "locations", False, "20")
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
            # SC = fti(
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
