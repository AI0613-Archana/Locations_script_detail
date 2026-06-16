# -*- coding: utf-8 -*-
import json
import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
from curl_cffi import requests
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

ses = requests.Session()

SOURCE_NAME = "rentalcar_reunion"
WEBSITE_CODE = 52
DOMAIN = "https://en.rentacar-reunion.fr/"

cookies = {
    "_pk_ref.17.d75a": "%5B%22%22%2C%22%22%2C1780660785%2C%22https%3A%2F%2Fwww.google.com%2F%22%5D",
    "_pk_id.17.d75a": "1ebd83a6e0b62b2f.1780660785.",
    "axeptio_authorized_vendors": "%2C%2C",
    "_gcl_au": "1.1.1503746488.1780660816",
    "_ga": "GA1.1.1123312177.1780660816",
    "axeptio_cookies": "{%22$$completed%22:true%2C%22$$token%22:%22ZBj1jARrXMu65przcWgtgXnC6m%22%2C%22$$date%22:%222026-06-05T12:00:15.804Z%22%2C%22$$cookiesVersion%22:{%22name%22:%2219g6dmjd0adv9%22%2C%22identifier%22:%2269456adb5ac6ebe9701db174%22}%2C%22facebook_pixel%22:false%2C%22tiktok%22:false%2C%22linkedin%22:false%2C%22bing%22:false%2C%22TikTok%22:false%2C%22Linkedin%22:false%2C%22Bing%22:false%2C%22$$googleConsentMode%22:{%22analytics_storage%22:%22granted%22%2C%22ad_storage%22:%22granted%22%2C%22ad_user_data%22:%22granted%22%2C%22ad_personalization%22:%22granted%22%2C%22version%22:2}%2C%22$$scope%22:%22persistent%22%2C%22$$duration%22:180}",
    "axeptio_all_vendors": "%2Cfacebook_pixel%2Ctiktok%2Clinkedin%2Cbing%2CTikTok%2CLinkedin%2CBing%2C",
    "FPID": "FPID2.2.hrCqKqkaX%2Fbp36XrcyaunPAszLrgNwoFnW1JXLkllQ4%3D.1780660816",
    "FPAU": "1.1.1503746488.1780660816",
    "_pk_id": "c50341823a88805b",
    "_ga_BZESNZN6Z5": "GS2.1.s1780663258$o2$g0$t1780663258$j60$l0$h2026417498",
}

headers = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=1, i",
    "purpose": "prefetch",
    "referer": DOMAIN,
    "sec-ch-ua": '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    "x-middleware-prefetch": "1",
    "x-nextjs-data": "1",
}


class rentalcar_reunion:
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

    def load(self, source_url):
        params = {
            "slug": "rental-car-reunion",
        }
        response = ses.get(
            source_url,
            params=params,
            cookies=cookies,
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

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
            websitecode = result.get("websitecode") or self.websitecode
            source_name = result.get("source_name", SOURCE_NAME)
            source_url = (
                result.get("source_url")
                or "https://en.rentacar-reunion.fr/_next/data/_7_xI2TMTlgro5DsKZJBH/en/rental-car-reunion.json"
            )
            print("refid", refid, "source_url", source_url)
            try:
                response_data = self.load(source_url)
                rows = self.extraction(response_data, refid, websitecode, source_name)
                if rows:
                    self.insert(rows)
                    self.update(1, refid)
                else:
                    self.update(2, refid)
            except Exception:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                self.update(2, refid)

    def extraction(self, response_data, refid, websitecode, source_name):
        rows = []
        agencies = response_data.get("pageProps", {}).get("agenciesTopics", [])

        for agency in agencies:
            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            agency_erp_code = str(agency.get("agencyErpCode", "")).strip()
            agency_id = str(agency.get("agencyId", "")).strip()
            location_code = (
                f"{agency_erp_code}|{agency_id}" if agency_erp_code or agency_id else ""
            )
            agency_type_code = agency.get("agencyType", {}).get("agencyTypeCode", "")
            is_airport = agency_type_code == "ARP"
            city = agency.get("city", "")
            location_type = "airport" if is_airport else "city"
            pickup_location = agency.get("agencyTranslation", {}).get(
                "agencyTranslationName", ""
            )

            rows.append(
                {
                    "id": refid,
                    "source_name": source_name,
                    "website_code": websitecode,
                    "pickup_location": pickup_location,
                    "location_country": "",
                    "location_code": location_code,
                    "is_airport": is_airport,
                    "created_date": created_date,
                    "location_type": location_type,
                    "city": city,
                    "region": "",
                    "priority_level": "",
                    "location_term": "",
                    "location_name": pickup_location,
                }
            )

        return rows


if __name__ == "__main__":
    RETRY = 1
    while RETRY < 20:
        SC = None
        try:
            # SC = rentalcar_reunion(1, 152, 152, "input_locations", "locations", False, "20")
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
            SC = rentalcar_reunion(
                status,
                startid,
                endid,
                inputtable,
                outputtable,
                offline,
                proxyid,
            )
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
