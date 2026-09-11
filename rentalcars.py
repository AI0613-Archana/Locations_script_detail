# -*- coding: utf-8 -*-
import os
import re
import sys
import json
import random
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from curl_cffi import requests
from datetime import datetime, timezone

ses = requests.Session(impersonate='chrome')

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}


class rentalcars:

    def __init__(
        self, status, startid, endid, inputtable, outputtable, offline, proxyid,
        max_workers=10,
    ):
        self.inputtable = inputtable
        self.outputtable = outputtable
        self.startid = startid
        self.endid = endid
        self.proxyid = proxyid
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        self.websitecode = 63
        self.is_dc_input = False
        self.max_workers = max_workers

        # thread-safety primitives (mirrors sixt.py structure)
        self.seen_lock = threading.Lock()
        self.rows_lock = threading.Lock()
        self.db_lock = threading.Lock()

        self.cursor.execute(
            f"SELECT proxy FROM proxy_list WHERE status IN ({self.proxyid})"
        )
        self.proxyset = self.cursor.fetchall()

        self.length_limits = self._get_length_limits(self.cursor)

        self.cursor.execute(
            f"""
            SELECT * FROM {self.inputtable}
            WHERE websitecode = %s AND status = %s AND id BETWEEN %s AND %s
        """,
            (str(self.websitecode), status, startid, endid),
        )
        resultset = self.cursor.fetchall()
        self.main(resultset)

    # -- PROXY --------------------------------------------------------------
    def get_proxy(self):
        if not self.proxyset:
            return {}
        proxy_str = self.proxyset[random.randrange(0, len(self.proxyset))]["proxy"]
        return {"https": f"http://{proxy_str}"}

    # -- HTTP -----------------------------------------------------------------
    def load(self, url, headers, proxies):
        return ses.get(url, timeout=15, headers=headers)

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

    # -- shared cookies / per-endpoint headers (same literal values as before,
    # just pulled out into methods so every thread can build its own copy) --
    def _cookies(self):
        return {
            'pcm_consent': 'analytical%3Dtrue%26countryCode%3DIN%26consentId%3De9d3f90c-9701-45cd-97d1-e4421a34110f%26consentedAt%3D2026-09-11T06%3A39%3A37.523Z%26expiresAt%3D2027-03-10T06%3A39%3A37.523Z%26implicit%3Dtrue%26marketing%3Dtrue%26regionCode%3DTN%26regulation%3Dnone%26legacyRegulation%3Dnone',
            'cors_js': '1',
            'BJS': '-',
            'bkng_sso_ses': 'e30',
            'bkng_sso_session': 'e30',
            '_gid': 'GA1.2.2126024718.1789108780',
            '_gcl_au': '1.1.20316885.1789108781',
            'FPID': 'FPID2.2.ZrAOZBaOdmzs97QNt%2FrNC11Y4NyKq%2B5mfdluocg6fS0%3D.1789108780',
            'FPAU': '1.1.20316885.1789108781',
            'FPLC': 'WImksRMXT%2B5r6czidbneDl51KPw0nYIQgSUmbo0wQpZl%2B4WNO69mVg12IjIMxpuTmdLYiEBG%2FHJ9FbQfBKqyg53BqAwh3oG%2BQhdBXZ0l1QxDm9IFT%2BZy%2B8ytkqx1oA%3D%3D',
            '_yjsu_yjad': '1789108782.89d6e4f9-000e-4375-afcf-d28c60cbdaa6',
            'persisted_lang': 'en',
            'persisted_currency': 'INR',
            'OptanonConsent': 'isGpcEnabled=0&datestamp=Fri+Sep+11+2026+12%3A31%3A01+GMT%2B0530+(India+Standard+Time)&version=202501.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=4549a107-9045-41f5-8d54-c43a5940e74f&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&implicitConsentCountry=nonGDPR&implicitConsentDate=1789108780340&groups=C0001%3A1%2CC0002%3A1%2CC0004%3A1&AwaitingReconsent=false',
            '_ga': 'GA1.1.1820026905.1789108780',
            '_ga_A12345': 'GS2.1.s1789108781$o1$g1$t1789110062$j60$l0$h82012277',
            'bkng': '11UmFuZG9tSVYkc2RlIyh9Yaa29%2F3xUOLbwcLxQQ4VaCrVZg3LUReM0B17KTp2uAas3vNx8lYLSRL6XqRH74KSAYxWdycpftvdL1pMY1%2BAh8rrgOwAItYC5wO5hIoBU5OUUv3KY3KgxS1w3OiLyOqbWIn6%2FG%2BeFh1OsWw12CMEwAarBfEtkoLOpY018K%2Bu2%2BFPvqGveS3mN2k%3D',
            '_uetsid': '91f5f600adab11f183371bd53957fcdd',
            '_uetvid': '91f60fa0adab11f1911963f088f7b931',
            'bkng_sso_auth': 'CAIQ6bn9wwIaZj9pmZihvhgHkheR3IJmPUfZi/PmxR0EOupmMbSQEJQ/jaWvAezSCmXxYtVADhOSsWMkrlSUUN2Ngg31pIHFblPLxOwVF8egJ+2sILeniOidOX5Yit5lrSIU9XGthFm3FbzgV4tZVA==',
            'aws-waf-token': '02510481-432a-4f97-97a9-54583bdac481:HgoAdgcxSgdQAAAA:AArPkFSbQ7twQVaeSs6WSul7eYJsrDNFcdfBlL1cQSyuzPHL2Jdc0rykK6G2jJrnxhECFCPgtEwQotKO2tCN7GHIo53EgLx5GAyD2y0s7S6ceJ+iPG5Prp+dhITvGY6m4PGQYt7SsbpPJ43kJrB23iIq/FTWHnmHu0/WnalAvlhM+risXbBZh1X1mafUeD63N4UsDw+MPzHrfKDVzWnbVK7SiTeeRNErutmxx2UeKKfT+EeC7l3MN+HxWaZ79C6EUfBrrtaQ1sTQfFp8ZgoIjy581G3UspxJA9E+iHGcUJKIt2xZIjWb9e+HkAtTXmaabq0bY8PDYsnMD8xk3qjMukWFFQY=',
        }

    def _sitemap_headers(self):
        # headers for GET https://www.booking.com/cars/sitemap.en-gb.html
        return {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-GB,en;q=0.9',
            'ect': '4g',
            'priority': 'u=0, i',
            'referer': 'https://www.booking.com/cars/index.en-gb.html?label=gen173nr-10CAEoggI46AdIM1gEaGyIAQGYATO4ARnIAQzYAQPoAQH4AQGIAgGoAgG4Asq2w9EGwAIB0gIkNTk4MTYwYzctZDkwMi00N2E5LTlkYzctYTMwZmJmYzNmMjRk2AIB4AIB&sid=96399a8f22c4f2fa18f24c3b6571317d&aid=304142',
            'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        }

    def _country_headers(self):
        # headers for the per-country page ("2nd hit")
        return {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-GB,en;q=0.9',
            'ect': '4g',
            'priority': 'u=0, i',
            'referer': 'https://www.booking.com/cars/sitemap.en-gb.html?label=gen173nr-10CAEoggI46AdIM1gEaGyIAQGYATO4ARnIAQzYAQPoAQH4AQGIAgGoAgG4Asq2w9EGwAIB0gIkNTk4MTYwYzctZDkwMi00N2E5LTlkYzctYTMwZmJmYzNmMjRk2AIB4AIB&sid=96399a8f22c4f2fa18f24c3b6571317d&aid=304142',
            'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        }

    def _airport_headers(self):
        # headers for GET https://cars.booking.com/api/location-suggestions
        return {
            'accept': '*/*',
            'accept-language': 'en-GB,en;q=0.9',
            'origin': 'https://www.booking.com',
            'priority': 'u=1, i',
            'referer': 'https://www.booking.com/',
            'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        }

    @staticmethod
    def _sitemap_params():
        return {
            'label': 'gen173nr-10CAEoggI46AdIM1gEaGyIAQGYATO4ARnIAQzYAQPoAQH4AQGIAgGoAgG4Asq2w9EGwAIB0gIkNTk4MTYwYzctZDkwMi00N2E5LTlkYzctYTMwZmJmYzNmMjRk2AIB4AIB',
            'sid': '96399a8f22c4f2fa18f24c3b6571317d',
            'aid': '304142',
        }

    # -- DB ---------------------------------------------------------------------
    def _get_length_limits(self, cursor):
        cursor.execute(
            """
            SELECT column_name, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = %s
              AND character_maximum_length IS NOT NULL
            """,
            (self.outputtable.split(".")[-1],),
        )
        return {
            row["column_name"]: row["character_maximum_length"]
            for row in cursor.fetchall()
        }

    def insert_one(self, row):
        """Insert a single row into outputtable immediately (thread-safe)."""
        columns = [c for c in row.keys() if c != "id"]
        colnames = ",".join(columns)
        placeholders = ",".join(["%s"] * len(columns))
        sql = f"INSERT INTO {self.outputtable} ({colnames}) VALUES ({placeholders})"

        value_row = []
        for col in columns:
            value = row.get(col)
            max_len = self.length_limits.get(col)
            if isinstance(value, str) and max_len and len(value) > max_len:
                print(
                    "Truncated",
                    col,
                    "from",
                    len(value),
                    "to",
                    max_len,
                    "for location_name",
                    row.get("location_name"),
                )
                value = value[:max_len]
            value_row.append(value)

        with self.db_lock:
            try:
                self.cursor.execute(sql, tuple(value_row))
                self.conn.commit()
                print(
                    "INSERTED |",
                    "pickup:",
                    row.get("pickup_location"),
                    "| location_country:",
                    row.get("location_country"),
                    "| type:",
                    row.get("location_type"),
                )
                return True
            except Exception:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                print("INSERT FAILED for", row.get("location_name"))
                self.eHandling()
                return False

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

    def jsonMatch(self, key, data):
        """Safely get value from dict or first match in list."""
        if isinstance(data, dict):
            return data.get(key, "")
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and key in item:
                    return item[key]
        return ""

    # -- ROW BUILD ------------------------------------------------------------
    def _build_row(
        self,
        refid,
        source_name,
        websitecode,
        name,
        location_country,
        city,
        location_type,
        locationterm,
        latitude,
        longitude,
        booking_country,
        created_date,
    ):
        return {
            "id": refid,
            "source_name": source_name,
            "website_code": websitecode,
            "pickup_location": name,
            "location_country": location_country,
            "location_code": "",
            "is_airport": True,
            "created_date": created_date,
            "location_type": location_type,
            "city": city,
            "priority_level": "",
            "location_term": locationterm,
            "location_name": name,
            "name": name,
            "latitude": latitude,
            "longitude": longitude,
            "booking_country": booking_country,
        }

    # -- MAIN -------------------------------------------------------------------
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
                self.extraction(refid, country, websitecode, source_name)
            except Exception:
                self.eHandling()
                self.update(2, refid)

    # -- EXTRACTION ---------------------------------------------------------
    def extraction(self, refid, country, websitecode, source_name):
        print("inside extraction")

        cookies = self._cookies()
        headers = self._sitemap_headers()
        params = self._sitemap_params()

        response = ses.get(
            'https://www.booking.com/cars/sitemap.en-gb.html',
            params=params,
            cookies=cookies,
            headers=headers,
        )

        locations_grp = re.search(
            r'(?s)All car hire locations(.*?)</html>', response.text
        ).group(1)

        countries = re.findall(r'(?s)<a href=".*?html\?', locations_grp)

        print("length of countries:", len(countries))

        country_urls = []
        for block in countries:
            if 'sitemap' in block or 'https' in block or '<span>' not in block:
                country_url = re.search(r'(?s)<a href="(.*?html)\?', block).group(1)
                country_urls.append(country_url)

        seen_airport_codes = set()
        any_inserted = False

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(
                    self._process_country,
                    country_url,
                    refid,
                    country,
                    websitecode,
                    source_name,
                    seen_airport_codes,
                )
                for country_url in country_urls
            ]
            for future in as_completed(futures):
                try:
                    if future.result():
                        any_inserted = True
                except Exception:
                    self.eHandling()

        if any_inserted:
            self.update(1, refid)
        else:
            self.update(2, refid)

    def _process_country(
        self, country_url, refid, country, websitecode, source_name, seen_airport_codes
    ):
        print(country_url)

        headers = self._country_headers()
        params = self._sitemap_params()

        response = ses.get(
            country_url,
            params=params,
            cookies=self._cookies(),
            headers=headers,
        )
        print(response.status_code)

        inserted_any = False

        if re.search(r'(?s)Airports in (.*?)Regions in', response.text):

            airport_locations_grp = re.search(
                r'(?s)Airports in (.*?)Regions in', response.text
            ).group(1)

            airport_locations = re.findall(
                r'(?s)<a href=".*?html\?', airport_locations_grp
            )

            print("length of airports:", len(airport_locations))

            for airport in airport_locations:
                airport_code_match = re.search(
                    r'(?s)<a.*?airport/.*?/(.*?)\.', airport
                )
                if not airport_code_match:
                    continue
                airport_code = airport_code_match.group(1)

                with self.seen_lock:
                    if airport_code.upper() in seen_airport_codes:
                        continue
                    seen_airport_codes.add(airport_code.upper())

                if self._process_airport(
                    airport_code, refid, country, websitecode, source_name
                ):
                    inserted_any = True

        return inserted_any

    def _process_airport(self, airport_code, refid, country, websitecode, source_name):
        headers = self._airport_headers()
        params = {
            'language': 'en',
            'cor': 'us',
            'aid': '304142',
            'term': airport_code.upper(),
        }

        response = ses.get(
            'https://cars.booking.com/api/location-suggestions',
            params=params,
            cookies=self._cookies(),
            headers=headers,
        )
        print("r_location_suggestions", response.status_code)
        print("airport name:", airport_code.upper())

        inserted_any = False

        try:
            suggestions = json.loads(response.text)
        except Exception:
            self.eHandling()
            return False

        for aa in suggestions:
            if aa.get('placeType') != 'A':
                continue

            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            name = aa['name']
            latitude = aa['lat']
            longitude = aa['lng']
            locationterm = f'{name}'
            city = aa.get('city', '')
            region = aa.get('region', '') or ''
            location_country = aa.get('countryIso', '')

            print("locationname :", airport_code.upper())
            print("locationterm :", locationterm)
            print("location_country:", location_country)
            print("booking_country:", country)
            print("-" * 10)

            location_type = "airport"

            row = self._build_row(
                refid,
                source_name,
                websitecode,
                name,
                location_country,
                city,
                location_type,
                locationterm,
                latitude,
                longitude,
                country,
                created_date,
            )

            if self.insert_one(row):
                with self.rows_lock:
                    inserted_any = True

        return inserted_any


if __name__ == "__main__":
    RETRY = 1
    MAX_WORKERS = 20
    while RETRY < 20:
        SC = None
        try:
            SC = rentalcars(
                0, 305, 305, "input_locations", "locations", False, "60",
                max_workers=MAX_WORKERS,
            )
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
            # SC = rentalcars(
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