# -*- coding: utf-8 -*-
import os
import sys
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import airportsdata
import cloudscraper
import psycopg2
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

THREAD_COUNT = 20

DOMAINS = [
    "www.autoeurope.at",
    "www.autoeurope.be",
    "www.autoeurope.ca",
    "www.autoeurope.eu",
    "www.autoeurope.de",
    "www.autoeurope.dk",
    "www.autoeurope.es",
    "www.autoeurope.fi",
    "www.autoeurope.fr",
    "www.autoeurope.co.uk",
    "www.autoeurope.it",
    "es.autoeurope.com",
    "www.autoeurope.nl",
    "www.autoeurope.pt",
    "www.autoeurope.se",
    "www.autoeurope.com",
]


class autoeurope:
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
        self.websitecode = 9
        self.is_dc_input = False
        self.scraper = cloudscraper.create_scraper()
        self.cursor.execute(
            f"""
            SELECT * FROM {self.inputtable}
            WHERE websitecode = %s::text AND status = %s AND id BETWEEN %s AND %s
        """,
            (str(self.websitecode), status, startid, endid),
        )
        resultset = self.cursor.fetchall()
        self.main(resultset)

    def load(self, base_url, inputs, cookies, headers):
        params = {
            "method": "textsearch",
            "criteria": inputs,
            "affiliateName": "AUTOEUROPE",
            "operator_id_list": "",
        }
        return self.scraper.get(
            f"{base_url}/plugins/ae3/lib/searchController.cfc",
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
        try:
            with self.conn.cursor() as cursor:
                execute_values(cursor, sql, values, page_size=500)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
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
        cookies = {
            "MXP_TRACKINGID": "6BC7B417-9040-099D-2498FEC15E99B407",
            "CFID": "5284746",
            "CFTOKEN": "f698fa9217ad5347-6BC7B3CC-AA28-4B9C-D5826C11433D1F23",
            "SESSID": "6BC7B4E9B3C433E6ED83DD85F8D223C6",
            "SITEFLAGCODE": "FRA",
            "rxVisitor": "1758540439297ICE1A0CVHPNF2RJKV2SHK7U8T7LT43P8",
            "osano_consentmanager_uuid": "5b41c4c3-9e84-4749-9b0c-a62cdc86ad2f",
            "osano_consentmanager": "ovG_xNyeXjokKaF_zMtGgv1T61Z2P2AOBEtE15WYgIBwowHUzWGsX9KxdkvZH84zL3dPthshxRXGm51TKX9oc9ZzNVuMmE6RMayJ-k0-EIdrrhCuIhaDxywm9ajmbVuxb9XG5aZnvjc8ANcepBp9DoA5diHVK8zWb9fMBubdjT-_4nX5AaGQjMUGWh4VX15CYIEX_IY1lSML36JNVNXdc1g4jGcG6N15OreLXgmqldQ71_jyzPbdS1L1VB5VjdlOgN1UZa_Q6vf4bLxj8cjeEidNODozECSqZERespTEcsRf30RUtsIfvaI7nTTBSEHuQLJqtb97Fww=",
            "_gcl_au": "1.1.1890456612.1758540440",
            "_gid": "GA1.2.460794994.1758540441",
            "_dc_gtm_UA-22828275-1": "1",
            "_gat_UA-66348978-1": "1",
            "pxcts": "1a7466d0-97a7-11f0-9214-9046bcecd2bb",
            "_pxvid": "1a745f37-97a7-11f0-9214-e183455a19d2",
            "SessionStarted": "true",
            "user_id": "undefined",
            "NumberVisits": "1",
            "_gat_UA-22828275-1": "1",
            "MyAccount-Pop-up": "true",
            "dtCookie": "v_4_srv_2_sn_A4225C42B4FA500D4310CD6F290AA84C_app-3A22d06ed7b87dbe2b_1_ol_0_perc_100000_mul_1",
            "_ga_HJLRJEPQFG": "GS2.2.s1758540441$o1$g1$t1758540459$j42$l0$h0",
            "dtSa": "-",
            "_ga_44LQ5MQKXX": "GS2.1.s1758540440$o1$g1$t1758540462$j38$l0$h0",
            "_ga": "GA1.2.1582317574.1758540441",
            "JSESSIONID": "0804E0B7CDC8BA6CE61ABBABB5AF9E28.cfusion",
            "_uetsid": "1ac0128097a711f0b7f7074fdf421730|n4boej|2|fzj|0|2091",
            "_uetvid": "1ac0c82097a711f0aec3918f24382098|15jbw83|1758540459845|2|1|bat.bing.com/p/insights/c/n",
            "_px3": "f24fb07d86ffe512c60450a476dd2af4b576c1feade2f385ff29f649f9a7a0a5:7SXWI7vu/+6oaQhJT+ec03hblDqxVGXyt7xpRBt/bc+8ZZgllvrjPAKwyMKxtOgnc+fbB4SJyO5hgcaWGG6o3Q==:1000:9mGX3yzYzMnKyodjBosODommAOiHkl0h2vycPrdfuinTGAlrqbVfzOve8LcF0z7RGd1a6RKK/ot6xQyN2hqLYeA7tTtI6to8Ege8y8OvY93ba1/dyXi4Yi85WcOLbI3KIR2g0alNdlcvewQBDGOWrIysZY5gF/w+VTHClh00/eXjIKHuUxMzLqop28wvfmW0xLsoUBrihdr6iZ+uF/Xfv7nJ2G38ncvAVX0tJdlB038=",
            "rxvt": "1758542263438|1758540439298",
            "dtPC": "2$540460783_519h3vQOKQRCOURIHKFPGRQJFGOMPSVWMGFRBR-0e0",
        }

        for result in resultset:
            print(result)
            refid = result["id"]
            websitecode = result.get("websitecode") or self.websitecode
            source_name = result.get("source_name", "autoeurope")
            country = result.get("country", "") or result.get("location_country", "")
            source_url = result.get("source_url", "")
            print("refid", refid, "source_url", source_url)
            rows = []
            seen = set()

            try:
                airport_map = airportsdata.load("IATA")
                airport_codes = [
                    code
                    for code, airport_data in airport_map.items()
                    if code
                    and len(code) == 3
                    and code.isalpha()
                    and (not country or airport_data.get("country") == country)
                ]
                if not airport_codes:
                    airport_codes = [
                        code
                        for code in airport_map.keys()
                        if code and len(code) == 3 and code.isalpha()
                    ]
                print("airport seed count", len(airport_codes), "for country", country)

                domains = DOMAINS
                if source_url:
                    parsed_domain = urlparse(source_url).netloc or source_url.replace(
                        "https://", ""
                    ).replace("http://", "").strip("/")
                    if parsed_domain:
                        domains = [parsed_domain]

                for domain in domains:
                    base_url = f"https://{domain}"
                    headers = {
                        "accept": "application/json, text/javascript, */*; q=0.01",
                        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
                        "cache-control": "no-cache",
                        "pragma": "no-cache",
                        "priority": "u=1, i",
                        "referer": f"{base_url}/results/",
                        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
                        "sec-ch-ua-mobile": "?0",
                        "sec-ch-ua-platform": '"Linux"',
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors",
                        "sec-fetch-site": "same-origin",
                        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
                        "x-dtpc": "2$540460783_519h3vQOKQRCOURIHKFPGRQJFGOMPSVWMGFRBR-0e0",
                        "x-requested-with": "XMLHttpRequest",
                    }

                    print(f"Checking domain: {domain}", flush=True)

                    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
                        futures = {
                            executor.submit(
                                self.load, base_url, inputs, cookies, headers
                            ): inputs
                            for inputs in airport_codes
                        }
                        for future in as_completed(futures):
                            inputs = futures[future]
                            try:
                                response = future.result()
                                response.raise_for_status()
                                payload = response.json()
                            except Exception as exc:
                                print(
                                    f"{domain} | {inputs} | request failed: {exc}",
                                    flush=True,
                                )
                                continue

                            locations = payload.get("locations", {})
                            extracted_locations = []
                            if isinstance(locations, dict):
                                first_bucket = locations.get("0")
                                if isinstance(first_bucket, dict):
                                    extracted_locations = (
                                        first_bucket.get("locations", []) or []
                                    )
                                else:
                                    for value in locations.values():
                                        if (
                                            isinstance(value, dict)
                                            and "locations" in value
                                        ):
                                            extracted_locations = (
                                                value.get("locations", []) or []
                                            )
                                            break
                            elif isinstance(locations, list):
                                extracted_locations = locations

                            for location in extracted_locations:
                                if location.get("iata") != inputs:
                                    continue

                                unique_key = (domain, inputs, location.get("chaos_hub_id"))
                                if unique_key in seen:
                                    continue

                                seen.add(unique_key)
                                created_date = datetime.now(timezone.utc).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                                location_term = (location.get("display_label") or "").strip()
                                location_country = location.get("chaos_country_code", "")
                                location_type = location.get("category", "")
                                city = location.get("chaos_city_name", "")
                                region = location.get("region_name", "")
                                is_airport = str(location_type).lower() == "airport"
                                location_code_parts = [
                                    str(location.get("chaos_hub_id") or "").strip(),
                                    str(location.get("desk_list") or "").strip(),
                                ]
                                location_code = "|".join(
                                    part for part in location_code_parts if part
                                )

                                row = {
                                    "id": refid,
                                    "source_name": source_name,
                                    "website_code": websitecode,
                                    "pickup_location": inputs,
                                    "location_country": location_country,
                                    "location_code": location_code,
                                    "is_airport": is_airport,
                                    "created_date": created_date,
                                    "location_type": location_type,
                                    "city": city,
                                    "region": region,
                                    "priority_level": "",
                                    "location_term": location_term,
                                    "location_name": location_term,
                                }
                                rows.append(row)
                                print(row, flush=True)

                if rows:
                    self.insert(rows)
                    self.update(1, refid)
                else:
                    self.update(2, refid)
            except Exception:
                self.update(2, refid)


if __name__ == "__main__":
    RETRY = 1
    while RETRY < 20:
        SC = None
        try:
            SC = autoeurope(0, 181, 181, "input_locations", "locations", False, "20")
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
            # SC = autoeurope(
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
