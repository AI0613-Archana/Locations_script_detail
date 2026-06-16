# -*- coding: utf-8 -*-
import os
import re
import sys
import json
import random
import time
import string
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

DIALECT_MAP = {
    "AE": "enGB",
    "BE": "frFR",
    "BG": "enGB",
    "BH": "enGB",
    "CH": "deCH",
    "CN": "zhCN",
    "CY": "enGB",
    "DE": "deDE",
    "DK": "daDK",
    "EE": "enGB",
    "ES": "esES",
    "FI": "fiFI",
    "FR": "frFR",
    "GB": "enGB",
    "GR": "enGB",
    "HR": "enGB",
    "IE": "enGB",
    "IL": "enGB",
    "IN": "enGB",
    "IT": "itIT",
    "JO": "enGB",
    "LB": "enGB",
    "LV": "enGB",
    "MA": "frFR",
    "MT": "enGB",
    "MU": "enGB",
    "NL": "nlNL",
    "NO": "noNO",
    "QA": "enGB",
    "RO": "enGB",
    "RS": "enGB",
    "RU": "ruRU",
    "SA": "enGB",
    "SE": "svSE",
    "SG": "enGB",
    "SI": "enGB",
    "CZ": "csCZ",
    "TH": "enGB",
    "TN": "frFR",
    "TR": "trTR",
    "UA": "enGB",
    "US": "enUS",
    "ZA": "enGB",
}


class hertz:

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
        self.websitecode = 37

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

    def load(self, url, params, headers):
        return requests.get(url, params=params, headers=headers, timeout=30)

    def insert(self, chunks):
        if not chunks:
            print("No rows supplied for insert.")
            return
        columns = [c for c in chunks[0].keys() if c != "id"]
        colnames = ",".join(columns)
        values = [tuple(row.get(col) for col in columns) for row in chunks]
        sql = f"INSERT INTO {self.outputtable} ({colnames}) VALUES %s ON CONFLICT DO NOTHING"
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

    def resolve_title(self, loc):
        """
        Get full location name from locationTitle.
        Falls back to displayText if locationTitle is just an IATA code
        or matches the first segment of displayText.
        displayText format: 'DXB, Dubai International Airport, Dubai, AE'
        """
        title = loc.get("locationTitle", "").strip()
        display = loc.get("displayText", "").strip()
        parts = [p.strip() for p in display.split(",")]

        # parts[0] = IATA code, parts[1] = full name
        iata_code = parts[0] if parts else ""

        # Use displayText parts[1] if:
        # - title is empty
        # - title is just the IATA code (e.g. "DXB")
        # - title is a short all-caps code (e.g. "AAL", "CDG")
        if not title or title == iata_code or (len(title) <= 4 and title.isupper()):
            title = parts[1] if len(parts) >= 2 else display

        return title

    def fetch_wordwheel(self, search_text, dialect, referer):
        print(referer)
        """Call Hertz WordWheel API and return locationList."""
        url = "https://loc.hertz.com/locations/WordWheel"
        params = {
            "callback": "parse",
            "dialect": dialect,
            "systemId": "IRAC",
            "subSystemId": "IRAC",
            "searchText": search_text,
        }
        headers = {
            "accept": "*/*",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
            "referer": referer,
            "sec-ch-ua": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
            "sec-fetch-dest": "script",
            "sec-fetch-mode": "no-cors",
            "sec-fetch-site": "cross-site",
            "sec-fetch-storage-access": "active",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            # 'cookie': 'visid_incap_1269859=VNR32TZjTjeFyazcu6o9kEbsMGoAAAAAQUIPAAAAAADAWKnZ0wY854hJzSrAEHxw; nlbi_1269859=AQa9PmP28D8aiAihy32dPAAAAABZ75w+vKnZD+RkHUshzIy2; nlbi_1269859_1267917=b3XZDMR9g0brIn/6y32dPAAAAABo/NxxC89ZZl+jOHQVO8/H; visid_incap_1269861=wiG/VoSRSR6vP6SPQWjcaUjsMGoAAAAAQUIPAAAAAACahAozpf9pfZmsDUEU2xL5; nlbi_1269859_3091200=FkwYb7e2xX0mnGEmy32dPAAAAAAHq6xXvG3ukVFw7Cb3h9bH; visid_incap_1269862=CQSl5bvjQKmZY7JIvbDeZ0zsMGoAAAAAQUIPAAAAAACtk3KmDaJyJ486cdMMytEi; visid_incap_1269867=ACuXavcGRMenevpuCoESJUvsMGoAAAAAQUIPAAAAAADIoriQjobQc7/gaIZAskKO; nlbi_1269861=Cj5MJGLFmRETPGMmd6pEcgAAAACsOxXdJyybFan8zdFfLiHj; __privaci_cookie_consent_uuid=ebed7c29-93dc-4f5d-a938-f5aa35fa4fde:20; __privaci_cookie_consent_generated=ebed7c29-93dc-4f5d-a938-f5aa35fa4fde:20; nlbi_1269867=g9Z5OJZ+6Ew0z/firkucOwAAAADKzCZcowo3i/AkZjFJmf6G; __privaci_cookie_no_action={"status":"no-action-consent"}; nlbi_1269859_3201900=ZFy4DKyESAZcBUG6y32dPAAAAAApxd9aACdtXxOjVAhgP73Z; nlbi_1269859_3091787=mtU/Ys/qmzQUEA0Zy32dPAAAAAB4iiEb4qst88b0GODip2Kq; incap_ses_736_1269859=LtjGLE3atCA+xGNRT8w2CnT1MGoAAAAAlx7dYYTN68kvtaqA8QlsmA==; nlbi_1269859_2147483392=sTBwCx4IwRQYqjVMy32dPAAAAACwYMXi4w1ZcwY3skYCRkkJ; incap_ses_736_1269861=QRi/KXOXay2U3npRT8w2CuUMMWoAAAAA29zzRCrUEk4cC3E2ITVjWw==; incap_ses_736_1269862=QjbqfSrqOER04npRT8w2CuoMMWoAAAAAsaZwJnHzAi4npnijBJGzFQ==; nlbi_1269861_1267930=+9K7Stb0IQtzAK45d6pEcgAAAAAvIGqhJIOtW47W/FP+qHdM; incap_ses_736_1269867=eD1UOL9gQWq44HpRT8w2CusMMWoAAAAAOLp5yFQZ5pll/uvJoBpIrQ==',
        }

        resp = self.load(url, params, headers)
        print(resp)
        resp.raise_for_status()
        match = re.search(r"parse\((\{.*\})\)", resp.text, re.DOTALL)
        if not match:
            return []
        return json.loads(match.group(1)).get("locationList", [])

    def extraction(self, refid, country, websitecode, source_name, dialect, referer):
        print("inside extraction")
        print("referer", referer)
        search_chars = list(string.ascii_uppercase)
        if dialect in ("zhCN", "ruRU"):
            search_chars += [str(i) for i in range(10)]

        seen_oags = set()
        rows = []

        for char in search_chars:
            try:
                locations = self.fetch_wordwheel(char, dialect, referer)
                time.sleep(0.3)

                # Fallback to enGB if dialect returns nothing
                if not locations and dialect not in ("enGB", "enUS"):
                    locations = self.fetch_wordwheel(char, "enGB", referer)
                    time.sleep(0.3)

                for loc in locations:
                    oag = loc.get("preferredOag", "").strip()
                    if not oag or oag in seen_oags:
                        continue
                    seen_oags.add(oag)

                    category = loc.get("categoryCode", 0)
                    is_airport = category == 1
                    location_type = "airport" if is_airport else "city"
                    title = self.resolve_title(loc)
                    created_date = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )

                    print(
                        f"  oag: {oag} | title: {title} | "
                        f"type: {location_type} | country: {country}"
                    )
                    display = loc.get("displayText", "").strip()
                    parts = [p.strip() for p in display.split(",")]
                    iata_code = (
                        parts[0] if parts else oag
                    )  # fallback to oag if no displayText

                    title = self.resolve_title(loc)
                    created_date = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )

                    row = {
                        "id": refid,
                        "source_name": source_name,
                        "website_code": websitecode,
                        "pickup_location": iata_code,  # ← "DXB"
                        "location_country": country,
                        "location_code": oag,
                        "is_airport": is_airport,
                        "created_date": created_date,
                        "location_type": location_type,
                        "city": loc.get("city", "").strip(),
                        "region": loc.get("stateName", "").strip(),
                        "priority_level": None,
                        "location_term": title,  # ← "Dubai International Airport"
                        "location_name": title.upper(),  # ← "DUBAI INTERNATIONAL AIRPORT"
                    }
                    # row = {
                    #     "id":               refid,
                    #     "source_name":      source_name,
                    #     "website_code":     websitecode,
                    #     "pickup_location":  title.upper(),
                    #     "location_country": country,
                    #     "location_code":    oag,
                    #     "is_airport":       is_airport,
                    #     "created_date":     created_date,
                    #     "location_type":    location_type,
                    #     "city":             loc.get("city", "").strip(),
                    #     "region":           loc.get("stateName", "").strip(),
                    #     "priority_level":   None,
                    #     "location_term":    title,
                    #     "location_name":    title.upper(),
                    # }
                    rows.append(row)

            except Exception:
                self.eHandling()
                time.sleep(2)

        if rows:
            self.insert(rows)
            print(f"  => {len(rows)} locations inserted for country {country}")
        else:
            print(f"  => No locations found for country {country}")

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
            print("refid", refid, "| source_url", source_url)
            print("website_url", website_url)
            print("domainname", domainname)
            dialect = DIALECT_MAP.get(country, "enGB")
            referer = f"https://{domainname}/"
            if "https://www.hertz.ch/" in website_url:
                dialect = "deDE"
            try:
                self.extraction(
                    refid, country, websitecode, source_name, dialect, referer
                )
                self.update(1, refid)
            except Exception:
                self.eHandling()
                self.update(2, refid)


if __name__ == "__main__":
    RETRY = 1
    while RETRY < 20:
        SC = None
        try:
            SC = hertz(0, 1, 1000000, "input_locations", "locations", False, "1,2,3")
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


# CH,DK
