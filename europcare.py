# -*- coding: utf-8 -*-
import os
import re
import sys
import json
import random
import time
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv
from curl_cffi import requests
from datetime import datetime, timezone

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}


class europcar:

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
        self.websitecode = 28
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
        proxy_str = self.proxyset[random.randrange(0, len(self.proxyset))]["proxy"]
        return {"https": f"http://{proxy_str}"}

    def load(self, url, headers, proxies):
        return requests.get(url, timeout=15, headers=headers)

    def insert(self, chunks):
        if not chunks:
            print("No rows supplied for insert.")
            return
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

    def build_headers(self):
        def rand_chrome_version():
            major = random.randint(120, 135)
            build = random.randint(0, 9999)
            patch = random.randint(0, 150)
            return f"{major}.0.{build}.{patch}"

        def rand_platform():
            return random.choice(
                [
                    "Linux x86_64",
                    "Windows NT 10.0; Win64; x64",
                ]
            )

        chrome_version = rand_chrome_version()
        platform = rand_platform()
        user_agent = (
            f"Mozilla/5.0 ({platform}) AppleWebKit/537.36 "
            f"(KHTML, like Gecko) Chrome/{chrome_version} Safari/537.36"
        )
        sec_ch_ua_platform = f'"{platform.split(";")[0].split()[0]}"'

        return {
            "accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
            "cache-control": "max-age=0",
            "sec-ch-ua": f'"Google Chrome";v="{chrome_version.split(".")[0]}", "Chromium";v="{chrome_version.split(".")[0]}", "Not/A)Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": sec_ch_ua_platform,
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": user_agent,
        }

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
        except Exception as e:
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
            domainname = result["domainname"]
            country = result["country"]
            website_url = result["website_url"]
            source_url = result["source_url"]
            print("refid", source_url)
            try:
                proxies = self.get_proxy()
                headers = {
                    "Connection": "keep-alive",
                    "Cache-Control": "max-age=0",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
                }
                response = self.load(source_url, headers, proxies)
                # open('europcar.html', 'w').write(str(response.content.decode('utf-8')))
                html = response.content.decode("utf-8")
                print(response.status_code)
                if response.status_code != 200:
                    continue
                else:
                    self.extraction(html, refid, country, websitecode, source_name)
            except Exception as e:

                # self.log(refid, f"failed: {e}")
                self.update(2, refid)

    def jsonMatch(self, key, data):
        """Safely get value from dict or first match in list."""
        if isinstance(data, dict):
            return data.get(key, "")
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and key in item:
                    return item[key]
        return ""

    def extraction(self, html, refid, country, websitecode, source_name):
        print("inside extraction")
        data = html
        if html:
            html_ = re.sub(r"^jQuery.*?\(|\);$", "", html)
            html = json.loads(html_)
            rows = []
            for data1 in self.jsonMatch("allCoutriesStations", html):
                for data in self.jsonMatch(data1, html["allCoutriesStations"]):
                    created_date = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    locationid = self.jsonMatch("code", data)
                    locationterm = self.jsonMatch("label", data)
                    locationname = self.jsonMatch("address", data["details"])
                    address1 = self.jsonMatch("street", data["details"])
                    city = self.jsonMatch("city", data["details"])
                    country = self.jsonMatch("country", data)
                    postalcode = self.jsonMatch("postcode", data["details"])
                    phonenumber = self.jsonMatch("phone", data["details"])
                    latitude = self.jsonMatch("latitude", data["details"])
                    longitude1 = self.jsonMatch("longitude", data["details"])
                    airport = self.jsonMatch("airports", data)
                    region = self.jsonMatch("countryTr", data)
                    if airport == True:
                        location_type = "airport"
                    else:
                        location_type = "city"
                    if "latitude" in str(longitude1):
                        longitude = re.sub('"postcode":.*', "", str(longitude1))
                    else:
                        longitude = longitude1
                    locationname = re.sub(r"'", "''", locationname)
                    address1 = re.sub(r"'", "''", address1)
                    city = re.sub(r"'", "''", city)
                    locationterm = re.sub("'", "''", locationterm)
                    # comp_code = 'EP'
                    print("postalcode", postalcode)
                    print("phonenumber", phonenumber)
                    print("latitude", latitude)
                    print("longitude", longitude)
                    print("country:", country)
                    print("locationid:", locationid)
                    print("_" * 30)
                    row = {
                        "id": refid,
                        "source_name": source_name,
                        "website_code": websitecode,
                        "pickup_location": locationname,
                        "location_country": country,
                        "location_code": locationid,
                        "is_airport": True,
                        "created_date": created_date,
                        "location_type": location_type,
                        "city": city,
                        "region": region,
                        "priority_level": "",
                        "location_term": locationterm,
                        "location_name": locationname,
                    }
                    rows.append(row)
            self.insert(rows)
            self.update(1, refid)
        else:
            self.update(2, refid)


if __name__ == "__main__":
    RETRY = 1
    while RETRY < 20:
        SC = None
        try:
            SC = europcar(1, 5, 5, "input_locations", "locations", False, "1,2,3")
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
            # SC = europcar(
            #     status,
            #     startid,
            #     endid,
            #     inputtable,
            #     outputtable,
            #     offline,
            #     proxyid,
            # )
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
        RETRY += 1
