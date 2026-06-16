# -*- coding: utf-8 -*-
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import airportsdata
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv
from requests import Session
from datetime import datetime, timezone

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}

ses = Session()
THREAD_COUNT = 20

cookies = {
    "mwcheck24": "j40a638lh4hag6qi6bg0365dburc5amnek1h54bdr23vni3q7ba0ejq4osoco8r5",
    "wpset": "ch24_pss_mw",
    "stick_to_core": "true",
    "devicetype": "desktop",
    "deviceoutput": "desktop",
    "market": "in",
    "ppset": "mietwagen",
    "mwspt": "acpr67%3Dc%26apd9%3Dd%26bbw5%3Db%26bot10%3Db%26coa8%3DB%26dhl3%3Db%26erb0%3Db%26ffd1%3Da%26fsf4%3Db%26ins2%3Da%26ncs%3Db%26nfh6%3Db%26pct4%3Dc%26pre5%3Db%26rdp6%3Db%26rmm3%3Db%26sme6%3Db%26sms1%3Db",
    "app-language": "de",
    "premium_level": "level_none",
    "core_session_id": "019ecedd-bc22-7eca-989b-8677d42d3ec3",
    "c24consent": "f",
    "mwc_id.1.acae": "b551191ffab44478.1781587119.",
    "mwc_ses.1.acae": "1",
    "c24-dt": "eyJhbGciOiJBMjU2S1ciLCJlbmMiOiJBMjU2R0NNIiwia2lkIjoiMThmNDJlMTgtNTcyOC00ODM1LTkwNzItNTkxNzBmNTU2MmJlIn0._dd4T0iB-HrGVfmPRP7cmEtbfJNNR_my_8q8E_Z6UWwTaTWnWJ_iCg.NPUSsATt4_WSKmx6.g5t4uqkd4A7R1OU-XQAmpZtq6A7sZ9EJgZ_WsirZphZUyz7Es8i0ZSHAMo3EOvbRJWnWGjrK3V0cE3sJXVZBai996jiEqEC3vKBNSKzQlp9s52zmBnjS2VDMBBPhQc7FzwTDM4y86rk8bba6Z24UwNh6lAxPxEUgLW5q-pk0V1UX2QMouczluJ1r_TXbX8eBOvgEbVXpdwvcObJiQsADi3cYhv8_rh4Z_Gxy0GwVIA3kMSbl79rtNjRi9GNS-iekla6hX4NcPAooCmKDds9sfavIG-oZdDpgdUnsJ7CzPHtN2CzoQb2RFt4eEty4xGLj05_ZlEqrbxMwd0t_SfqSG_owO5hIeDCOcvPZ941isv0eQcA2.j3Alhghr42Mdie5KkAoi_A",
    "gcampaignid": "undefined",
    "referer": "unknown",
    "v": "3",
    "hash": "a6467830f99d69b358a0c65eeaeb84cb",
    "timestamp_creation": "1781587125",
    "c24search": "v3_%7C1781588212%7C2%7C2a33ef007d30e2b5b9c31f3f9fb8df72.91ec2896b12a4df631ee7383f57fde62d74e5b0ac1618b886739a00a41816da7",
    "c24-lvp": "rentalcar%3A1781588240",
    "timestamp_access": "1781588246",
}

headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://mietwagenvergleich.check24.de",
    "priority": "u=1, i",
    "referer": "https://mietwagenvergleich.check24.de/",
    "sec-ch-ua": '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
}


class check24:

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
        self.websitecode = 20
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
        params = {
            "search_value": str(search_value).lower(),
        }
        return ses.get(
            source_url,
            params=params,
            cookies=cookies,
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
            source_name = result.get("source_name", "check24")
            country = result.get("country", "") or result.get("location_country", "")
            source_url = result.get("source_url") or "https://api-public.mietwagenvergleich.check24.de/journey/suggest/query"
            print("refid", refid, "source_url", source_url)
            try:
                rows = []
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
                            print("search_value", airport_code, "status", response.status_code)
                            if response.status_code != 200:
                                continue
                            rows.extend(
                                self.extraction(
                                    response.json(),
                                    refid,
                                    country,
                                    websitecode,
                                    source_name,
                                )
                            )
                        except Exception:
                            continue
                if rows:
                    self.insert(rows)
                    self.update(1, refid)
                else:
                    self.update(2, refid)
            except Exception:
                self.update(2, refid)

    def extraction(self, response_data, refid, country, websitecode, source_name):
        destinations = response_data.get("data", {}).get("searchable_destinations", [])
        rows = []
        for destination in destinations:
            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            airport = destination.get("destination_type") == "airport"
            location_code = destination.get("destination_id")
            location_term = destination.get("destination_name", "")
            pickup_location = destination.get("iata_code") or location_term
            location_country = destination.get("country_code", "") or country
            region = destination.get("region_name", "")
            if airport:
                location_type = "airport"
                is_airport = True
            else:
                location_type = "city"
                is_airport = False
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
                "city": "",
                "region": region,
                "priority_level": "",
                "location_term": location_term,
                "location_name": location_term,
            }
            rows.append(row)
        return rows


if __name__ == "__main__":
    RETRY = 1
    while RETRY < 20:
        SC = None
        try:
            # SC = check24(1, 144, 144, "input_locations", "locations", False, "20")
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
            SC = check24(
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
