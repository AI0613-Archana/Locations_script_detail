# -*- coding: utf-8 -*-
import html as html_lib
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timezone

import psycopg2
from bs4 import BeautifulSoup
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


class hertz_6:
    def __init__(
        self, status, startid, endid, inputtable, outputtable, offline, proxyid
    ):
        self.inputtable = inputtable
        self.outputtable = outputtable
        self.startid = startid
        self.endid = endid
        self.offline = offline
        self.proxyid = proxyid
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        self.websitecode = 37
        self.is_dc_input = False

        if self.proxyid:
            self.cursor.execute(
                f"SELECT proxy FROM proxy_list WHERE status IN ({self.proxyid})"
            )
            self.proxyset = self.cursor.fetchall()
        else:
            self.proxyset = []

        if str(status).strip().lower() == "any":
            self.cursor.execute(
                f"""
                SELECT * FROM {self.inputtable}
                WHERE websitecode = %s AND id BETWEEN %s AND %s
                ORDER BY id
            """,
                (self.websitecode, startid, endid),
            )
        else:
            self.cursor.execute(
                f"""
                SELECT * FROM {self.inputtable}
                WHERE websitecode = %s AND status = %s AND id BETWEEN %s AND %s
                ORDER BY id
            """,
                (self.websitecode, status, startid, endid),
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

    def load(self, url, headers, proxies):
        return requests.get(
            url,
            timeout=30,
            headers=headers,
            proxies=proxies,
            impersonate="chrome",
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
            domainname = result.get("domainname") or "www.hertz.tn"
            country = result.get("country") or "TN"
            website_url = result.get("website_url") or "https://www.hertz.tn/"
            source_url = result.get("source_url") or website_url
            target_url = (
                website_url if "hertz.tn" in website_url else "https://www.hertz.tn/"
            )
            print("refid", refid, "target_url", target_url)

            try:
                html_content = ""
                if self.offline and os.path.exists("response.html"):
                    print("Reading offline response.html")
                    with open("response.html", "r", encoding="utf-8") as f:
                        html_content = f.read()
                else:
                    proxies = self.get_proxy()
                    headers = self.build_headers()
                    try:
                        response = self.load(target_url, headers, proxies)
                        if response.status_code == 200:
                            html_content = response.text
                    except Exception as exc:
                        print("Proxy load failed:", exc, "trying direct request")
                        try:
                            response = self.load(target_url, headers, {})
                            if response.status_code == 200:
                                html_content = response.text
                        except Exception as exc2:
                            print("Direct load failed:", exc2)

                    if not html_content and os.path.exists("response.html"):
                        print("Falling back to local response.html")
                        with open("response.html", "r", encoding="utf-8") as f:
                            html_content = f.read()

                if html_content:
                    rows = self.extraction(
                        html_content, refid, country, websitecode, source_name
                    )
                    print("Extracted rows count:", len(rows))
                    if rows:
                        self.insert(rows)
                        self.update(1, refid)
                    else:
                        self.update(2, refid)
                else:
                    print("No HTML content retrieved.")
                    self.update(2, refid)
            except Exception:
                self.eHandling()
                self.update(2, refid)

    def extraction(self, html_content, refid, country, websitecode, source_name):
        print("inside extraction")
        if not html_content:
            return []

        rows = []
        seen_codes = set()
        soup = BeautifulSoup(html_content, "html.parser")

        select = soup.find("select", {"id": "lieu_depart"}) or soup.find(
            "select", {"name": "lieu_depart"}
        )
        options = select.find_all("option") if select else soup.find_all("option")

        extracted_options = []
        if options:
            for opt in options:
                val = opt.get("value", "").strip()
                name = html_lib.unescape(opt.get_text()).strip()
                if val and val != "0":
                    extracted_options.append((val, name))

        if not extracted_options:
            select_match = re.search(
                r'<select[^>]*id=["\']lieu_depart["\'][^>]*>(.*?)</select>',
                html_content,
                re.I | re.S,
            )
            target_html = select_match.group(1) if select_match else html_content
            matches = re.findall(
                r'<option\s+[^>]*value=["\']([^"\']+)["\'][^>]*>(.*?)</option>',
                target_html,
                re.I | re.S,
            )
            for code, name in matches:
                code = code.strip()
                name = html_lib.unescape(re.sub(r"<[^>]+>", "", name)).strip()
                if code and code != "0":
                    extracted_options.append((code, name))

        known_cities = [
            "Tunis",
            "Monastir",
            "Sfax",
            "Djerba",
            "Zarzis",
            "Enfidha",
            "Tozeur",
            "Tabarka",
            "Hammamet",
            "Sousse",
            "Bizerte",
            "Nabeul",
            "Ben Arous",
        ]

        for location_code, location_name in extracted_options:
            if not location_code or location_code in seen_codes:
                continue

            location_name = re.sub(r"\s+", " ", location_name).strip()
            if not location_name or location_name.lower().startswith("lieu de"):
                continue

            seen_codes.add(location_code)

            is_airport = bool(re.search(r"a[ée]roport|airport", location_name, re.I))
            location_type = "Airport" if is_airport else "City"

            city = ""
            for c in known_cities:
                if re.search(rf"\b{re.escape(c)}\b", location_name, re.I):
                    city = c
                    break

            if not city:
                clean_loc = re.sub(
                    r"^(Agence Centrale|Agence|Aéroport International de|Aéroport international de|Aéroport International|Aéroport)\s*",
                    "",
                    location_name,
                    flags=re.I,
                ).strip()
                city = clean_loc.split("–")[0].split("-")[0].strip()

            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            row = {
                "id": refid,
                "source_name": source_name,
                "website_code": websitecode,
                "pickup_location": location_name,
                "location_country": country,
                "location_code": location_code,
                "is_airport": is_airport,
                "created_date": created_date,
                "location_type": location_type,
                "city": city,
                "region": "",
                "priority_level": "",
                "location_term": location_name,
                "location_name": location_name,
                "booking_country": country,
            }
            rows.append(row)

        return rows


if __name__ == "__main__":
    STATUS = 0
    STARTID = 132
    ENDID = 132
    INPUTTABLE = "input_locations"
    OUTPUTTABLE = "locations"
    OFFLINE = False
    PROXYID = "1,2,3"

    SC = None
    try:
        if len(sys.argv) >= 8:
            (
                script,
                STATUS,
                STARTID,
                ENDID,
                INPUTTABLE,
                OUTPUTTABLE,
                OFFLINE,
                PROXYID,
                *extra_args,
            ) = sys.argv
            if str(OFFLINE).lower() in ("true", "1", "yes"):
                OFFLINE = True
            elif str(OFFLINE).lower() in ("false", "0", "no"):
                OFFLINE = False

        SC = hertz_6(
            STATUS,
            STARTID,
            ENDID,
            INPUTTABLE,
            OUTPUTTABLE,
            OFFLINE,
            PROXYID,
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