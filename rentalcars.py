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

# from base_scraper import BaseScraper
from datetime import datetime, timezone

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
        except Exception as e:
            try:
                self.conn.rollback()
            except Exception:
                pass
            # params may contain refid as the last element; include for logging when available
            # self.log(None, f"_execute_commit failed: {e} -- query: {query} params: {params}")
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
                # proxies = self.get_proxy()
                # headers = {
                #     "Connection": "keep-alive",
                #     "Cache-Control": "max-age=0",
                #     "Upgrade-Insecure-Requests": "1",
                #     "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
                #     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                #     "Accept-Encoding": "gzip, deflate, br",
                #     "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
                # }
                # response = self.load(source_url, headers, proxies)
                # # open('europcar.html', 'w').write(str(response.content.decode('utf-8')))
                # html = response.content.decode("utf-8")
                # print(response.status_code)
                # if response.status_code != 200:
                #     continue
                # else:
                self.extraction(refid, country, websitecode, source_name)
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

    def extraction(self,refid, country, websitecode, source_name):
        print("inside extraction")

        cookies = {
            'pcm_consent': 'analytical%3Dtrue%26countryCode%3DIN%26consentId%3D601acb98-d107-4ca7-a28c-1a48ff5165db%26consentedAt%3D2026-06-16T05%3A12%3A40.117Z%26expiresAt%3D2026-12-13T05%3A12%3A40.117Z%26implicit%3Dtrue%26marketing%3Dtrue%26regionCode%3DTN%26regulation%3Dnone%26legacyRegulation%3Dnone',
            'bkng_sso_ses': 'e30',
            'bkng_sso_session': 'e30',
            '_gcl_au': '1.1.1889586477.1781586767',
            'bkng_prue': '1',
            '_yjsu_yjad': '1781586767.34baa05c-ca26-435c-b5af-ff2f9c58d030',
            'FPID': 'FPID2.2.1w7nYZLBgyBwIrDTfKK%2Faakw8yyKAocGkeryIMrAswg%3D.1781586767',
            'FPAU': '1.1.1889586477.1781586767',
            'FPLC': 'UWIYpN4%2BzG%2BluzoJUwkx%2FxDQjX9Cq7xAtHxEur%2BCZ%2F%2FOEtLw737xA7qblxhec1olLo%2BfED0yO92GNOVDNOIdNy8P3TP2%2BJ%2Fmrby54IqM1NZU6p1xU%2FRA5bdoXXdzrg%3D%3D',
            'cors_js': '1',
            'BJS': '-',
            '_gid': 'GA1.2.1711172287.1781586774',
            'pcm_personalization_disabled': '0',
            'cto_bundle': 'jZAmWF9sUDVrNzRLayUyRnAzSldvbkd1cWxHSDZuTmklMkZaYTF1TnpNWmhzRXVJMHQyVG9NM2U1NFklMkYlMkZ4dTg1QTRtYnlmcHNhJTJGSyUyRm1CZmdDSE9ac3VzSGlBQVVQVDg3dlRNZ2VzakxIQUZTZ2ttJTJCbEN4UUFQemw0R3VobUUlMkJhRXFlQUp5JTJGSQ',
            '_rdt_uuid': '1781586767152.e8b560e5-77b9-4543-8a4a-9d71011c5e56',
            '__gads': 'ID=992c061e6617eac7:T=1781586766:RT=1781587200:S=ALNI_Mbjlr9VtLq1wnwI9zPO99kPBBfX4Q',
            '__gpi': 'UID=0000146e434266ce:T=1781586766:RT=1781587200:S=ALNI_MbZJViEzC7nHpb_5dqbLiUaHVmONw',
            '__eoi': 'ID=1cc2749210d2d1db:T=1781586766:RT=1781587200:S=AA-Afjac5jzpaLP91Yz0dx55TKf2',
            'g_state': '{"i_l":0,"i_ll":1781587284625,"i_b":"kKGtDIhILlZ1mqYCX0ZJO+NsHXpeOWTNA0f4QFq5fCw","i_e":{"enable_itp_optimization":0},"i_et":1781587195920}',
            'bk_nav_search': '%7B%22u%22%3A%22https%3A%2F%2Fwww.booking.com%2Fcars%2Findex.en-gb.html%3Flabel%3Dgen173nr-10CAEoggI46AdIM1gEaGyIAQGYATO4ARnIAQzYAQPoAQH4AQGIAgGoAgG4Asq2w9EGwAIB0gIkNTk4MTYwYzctZDkwMi00N2E5LTlkYzctYTMwZmJmYzNmMjRk2AIB4AIB%26sid%3D96399a8f22c4f2fa18f24c3b6571317d%26aid%3D304142%22%2C%22t%22%3A1781587629604%2C%22p%22%3A%22index%22%7D',
            'OptanonConsent': 'implicitConsentCountry=nonGDPR&implicitConsentDate=1781586764475&isGpcEnabled=0&datestamp=Tue+Jun+16+2026+10%3A57%3A10+GMT%2B0530+(India+Standard+Time)&version=202501.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=74c9d612-9d7f-4b18-926f-3d256c1d8147&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0004%3A1&AwaitingReconsent=false',
            '_ga': 'GA1.1.965424957.1781586767',
            '_ga_A12345': 'GS2.1.s1781586767$o1$g1$t1781587630$j34$l0$h941685780',
            '_uetsid': '03f1d0d0694211f1abc7737fcf2f9680',
            '_uetvid': '03f209c0694211f19759df56ce1124f9',
            'bkng': '11UmFuZG9tSVYkc2RlIyh9Yaa29%2F3xUOLbca8KLfxLPecyWBQgff1Xz9xpu%2Fvg3EeoBpTCJv6lHnUpabvz4MYixMx%2Bj%2FohcHNyIvh2pgZ86AJZ6okrT%2F5WG4noXYtB%2FYnopW1q94EOkQASVXkqpLBJgy6vdup9mVK2u3DfIASDB%2FxXqNYeljNj%2B9f%2Bbj3bnww7JifkM3CDCUs%3D',
            'bkng_sso_auth': 'CAIQi4nT0gIaZqdC/c1tXnV8WdyyL4+nPwU+Z8rbRTuPP4RVZ7f3mmUSvIWulqhcTiSGBGuHGcc+kri8kAh4k7VmiGFhb2JHApxg8TuyNuP/H/HeD22O4cglI+hTGtLLTxkuQWiyig/w9o9HvJxVEg==',
            'aws-waf-token': '2862bf10-180c-4518-bd72-0c7f2807a9b3:HgoAu/slHEMQAAAA:BzTnSw4hsUUD5Ie+0j+vGsSf8Xw3m43l14ce4uhQFc3TbQtzuTzud3yr4afH+HLQbsier06kqKAmcWi64VXZN2X57OKxDdCKFdtCHu5E14cSigsbzJ2fB1Wgc6ukvH6/j4eTOU/JHWpuTor+7pCFoCiR5p0WR54WbEVjHCUYIOp1Cpvsjz914Qh/JcIZmKuH+EP1k1GtPzdVg+MpznlhOnUkhyG0MM3DY87KnUMbAT2RuDidAso+57krOSnaROaYQEE=',
        }

        headers = {
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
            # 'cookie': 'pcm_consent=analytical%3Dtrue%26countryCode%3DIN%26consentId%3D601acb98-d107-4ca7-a28c-1a48ff5165db%26consentedAt%3D2026-06-16T05%3A12%3A40.117Z%26expiresAt%3D2026-12-13T05%3A12%3A40.117Z%26implicit%3Dtrue%26marketing%3Dtrue%26regionCode%3DTN%26regulation%3Dnone%26legacyRegulation%3Dnone; bkng_sso_ses=e30; bkng_sso_session=e30; _gcl_au=1.1.1889586477.1781586767; bkng_prue=1; _yjsu_yjad=1781586767.34baa05c-ca26-435c-b5af-ff2f9c58d030; FPID=FPID2.2.1w7nYZLBgyBwIrDTfKK%2Faakw8yyKAocGkeryIMrAswg%3D.1781586767; FPAU=1.1.1889586477.1781586767; FPLC=UWIYpN4%2BzG%2BluzoJUwkx%2FxDQjX9Cq7xAtHxEur%2BCZ%2F%2FOEtLw737xA7qblxhec1olLo%2BfED0yO92GNOVDNOIdNy8P3TP2%2BJ%2Fmrby54IqM1NZU6p1xU%2FRA5bdoXXdzrg%3D%3D; cors_js=1; BJS=-; _gid=GA1.2.1711172287.1781586774; pcm_personalization_disabled=0; cto_bundle=jZAmWF9sUDVrNzRLayUyRnAzSldvbkd1cWxHSDZuTmklMkZaYTF1TnpNWmhzRXVJMHQyVG9NM2U1NFklMkYlMkZ4dTg1QTRtYnlmcHNhJTJGSyUyRm1CZmdDSE9ac3VzSGlBQVVQVDg3dlRNZ2VzakxIQUZTZ2ttJTJCbEN4UUFQemw0R3VobUUlMkJhRXFlQUp5JTJGSQ; _rdt_uuid=1781586767152.e8b560e5-77b9-4543-8a4a-9d71011c5e56; __gads=ID=992c061e6617eac7:T=1781586766:RT=1781587200:S=ALNI_Mbjlr9VtLq1wnwI9zPO99kPBBfX4Q; __gpi=UID=0000146e434266ce:T=1781586766:RT=1781587200:S=ALNI_MbZJViEzC7nHpb_5dqbLiUaHVmONw; __eoi=ID=1cc2749210d2d1db:T=1781586766:RT=1781587200:S=AA-Afjac5jzpaLP91Yz0dx55TKf2; g_state={"i_l":0,"i_ll":1781587284625,"i_b":"kKGtDIhILlZ1mqYCX0ZJO+NsHXpeOWTNA0f4QFq5fCw","i_e":{"enable_itp_optimization":0},"i_et":1781587195920}; bk_nav_search=%7B%22u%22%3A%22https%3A%2F%2Fwww.booking.com%2Fcars%2Findex.en-gb.html%3Flabel%3Dgen173nr-10CAEoggI46AdIM1gEaGyIAQGYATO4ARnIAQzYAQPoAQH4AQGIAgGoAgG4Asq2w9EGwAIB0gIkNTk4MTYwYzctZDkwMi00N2E5LTlkYzctYTMwZmJmYzNmMjRk2AIB4AIB%26sid%3D96399a8f22c4f2fa18f24c3b6571317d%26aid%3D304142%22%2C%22t%22%3A1781587629604%2C%22p%22%3A%22index%22%7D; OptanonConsent=implicitConsentCountry=nonGDPR&implicitConsentDate=1781586764475&isGpcEnabled=0&datestamp=Tue+Jun+16+2026+10%3A57%3A10+GMT%2B0530+(India+Standard+Time)&version=202501.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=74c9d612-9d7f-4b18-926f-3d256c1d8147&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0004%3A1&AwaitingReconsent=false; _ga=GA1.1.965424957.1781586767; _ga_A12345=GS2.1.s1781586767$o1$g1$t1781587630$j34$l0$h941685780; _uetsid=03f1d0d0694211f1abc7737fcf2f9680; _uetvid=03f209c0694211f19759df56ce1124f9; bkng=11UmFuZG9tSVYkc2RlIyh9Yaa29%2F3xUOLbca8KLfxLPecyWBQgff1Xz9xpu%2Fvg3EeoBpTCJv6lHnUpabvz4MYixMx%2Bj%2FohcHNyIvh2pgZ86AJZ6okrT%2F5WG4noXYtB%2FYnopW1q94EOkQASVXkqpLBJgy6vdup9mVK2u3DfIASDB%2FxXqNYeljNj%2B9f%2Bbj3bnww7JifkM3CDCUs%3D; bkng_sso_auth=CAIQi4nT0gIaZqdC/c1tXnV8WdyyL4+nPwU+Z8rbRTuPP4RVZ7f3mmUSvIWulqhcTiSGBGuHGcc+kri8kAh4k7VmiGFhb2JHApxg8TuyNuP/H/HeD22O4cglI+hTGtLLTxkuQWiyig/w9o9HvJxVEg==; aws-waf-token=2862bf10-180c-4518-bd72-0c7f2807a9b3:HgoAu/slHEMQAAAA:BzTnSw4hsUUD5Ie+0j+vGsSf8Xw3m43l14ce4uhQFc3TbQtzuTzud3yr4afH+HLQbsier06kqKAmcWi64VXZN2X57OKxDdCKFdtCHu5E14cSigsbzJ2fB1Wgc6ukvH6/j4eTOU/JHWpuTor+7pCFoCiR5p0WR54WbEVjHCUYIOp1Cpvsjz914Qh/JcIZmKuH+EP1k1GtPzdVg+MpznlhOnUkhyG0MM3DY87KnUMbAT2RuDidAso+57krOSnaROaYQEE=',
        }

        params = {
            'label': 'gen173nr-10CAEoggI46AdIM1gEaGyIAQGYATO4ARnIAQzYAQPoAQH4AQGIAgGoAgG4Asq2w9EGwAIB0gIkNTk4MTYwYzctZDkwMi00N2E5LTlkYzctYTMwZmJmYzNmMjRk2AIB4AIB',
            'sid': '96399a8f22c4f2fa18f24c3b6571317d',
            'aid': '304142',
        }

        response = requests.get('https://www.booking.com/cars/sitemap.en-gb.html', params=params, cookies=cookies, headers=headers)

        # open("booking_1sthit.html","w").write(response.text)

        # print(response.status_code)

        locations_grp = re.search(r'(?s)All car hire locations(.*?)</html>',response.text).group(1)

        countries = re.findall(r'(?s)<a href=".*?html\?',locations_grp)

        print("length of countries:", len(countries))

        rows = []

        for country in countries:

            if 'sitemap' in country and 'https' in country and '<span>' not in country:
                country_url = re.search(r'(?s)<a href="(.*?html)\?',country).group(1)
                print(country_url)

                headers = {
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
                    # 'cookie': 'pcm_consent=analytical%3Dtrue%26countryCode%3DIN%26consentId%3D601acb98-d107-4ca7-a28c-1a48ff5165db%26consentedAt%3D2026-06-16T05%3A12%3A40.117Z%26expiresAt%3D2026-12-13T05%3A12%3A40.117Z%26implicit%3Dtrue%26marketing%3Dtrue%26regionCode%3DTN%26regulation%3Dnone%26legacyRegulation%3Dnone; bkng_sso_ses=e30; bkng_sso_session=e30; _gcl_au=1.1.1889586477.1781586767; bkng_prue=1; _yjsu_yjad=1781586767.34baa05c-ca26-435c-b5af-ff2f9c58d030; FPID=FPID2.2.1w7nYZLBgyBwIrDTfKK%2Faakw8yyKAocGkeryIMrAswg%3D.1781586767; FPAU=1.1.1889586477.1781586767; FPLC=UWIYpN4%2BzG%2BluzoJUwkx%2FxDQjX9Cq7xAtHxEur%2BCZ%2F%2FOEtLw737xA7qblxhec1olLo%2BfED0yO92GNOVDNOIdNy8P3TP2%2BJ%2Fmrby54IqM1NZU6p1xU%2FRA5bdoXXdzrg%3D%3D; cors_js=1; BJS=-; _gid=GA1.2.1711172287.1781586774; pcm_personalization_disabled=0; cto_bundle=jZAmWF9sUDVrNzRLayUyRnAzSldvbkd1cWxHSDZuTmklMkZaYTF1TnpNWmhzRXVJMHQyVG9NM2U1NFklMkYlMkZ4dTg1QTRtYnlmcHNhJTJGSyUyRm1CZmdDSE9ac3VzSGlBQVVQVDg3dlRNZ2VzakxIQUZTZ2ttJTJCbEN4UUFQemw0R3VobUUlMkJhRXFlQUp5JTJGSQ; _rdt_uuid=1781586767152.e8b560e5-77b9-4543-8a4a-9d71011c5e56; __gads=ID=992c061e6617eac7:T=1781586766:RT=1781587200:S=ALNI_Mbjlr9VtLq1wnwI9zPO99kPBBfX4Q; __gpi=UID=0000146e434266ce:T=1781586766:RT=1781587200:S=ALNI_MbZJViEzC7nHpb_5dqbLiUaHVmONw; __eoi=ID=1cc2749210d2d1db:T=1781586766:RT=1781587200:S=AA-Afjac5jzpaLP91Yz0dx55TKf2; g_state={"i_l":0,"i_ll":1781587284625,"i_b":"kKGtDIhILlZ1mqYCX0ZJO+NsHXpeOWTNA0f4QFq5fCw","i_e":{"enable_itp_optimization":0},"i_et":1781587195920}; bk_nav_search=%7B%22u%22%3A%22https%3A%2F%2Fwww.booking.com%2Fcars%2Findex.en-gb.html%3Flabel%3Dgen173nr-10CAEoggI46AdIM1gEaGyIAQGYATO4ARnIAQzYAQPoAQH4AQGIAgGoAgG4Asq2w9EGwAIB0gIkNTk4MTYwYzctZDkwMi00N2E5LTlkYzctYTMwZmJmYzNmMjRk2AIB4AIB%26sid%3D96399a8f22c4f2fa18f24c3b6571317d%26aid%3D304142%22%2C%22t%22%3A1781587629604%2C%22p%22%3A%22index%22%7D; OptanonConsent=implicitConsentCountry=nonGDPR&implicitConsentDate=1781586764475&isGpcEnabled=0&datestamp=Tue+Jun+16+2026+10%3A59%3A12+GMT%2B0530+(India+Standard+Time)&version=202501.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=74c9d612-9d7f-4b18-926f-3d256c1d8147&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0004%3A1&AwaitingReconsent=false; bkng=11UmFuZG9tSVYkc2RlIyh9Yaa29%2F3xUOLbKE7bjkbYWzkrvktr9Tb8GgGsDij4qItc98I3Nq9tVUyXYfKURi%2FUL8%2FB37jsI%2F30HI7Uv3%2BNNGWFXkzvdaeEf7WDZ%2BzNZOa%2Fb2r3sD3sqtUBGnSOMFLPzUdImrjpV3GhcNdCI%2B7Bv8Pul8DQFLR6o29m%2F2%2B2cUPyYrjF%2Br%2Fp5l8%3D; _ga=GA1.1.965424957.1781586767; _ga_A12345=GS2.1.s1781586767$o1$g1$t1781587757$j60$l0$h941685780; _uetsid=03f1d0d0694211f1abc7737fcf2f9680; _uetvid=03f209c0694211f19759df56ce1124f9; aws-waf-token=2862bf10-180c-4518-bd72-0c7f2807a9b3:HgoAusUmoG0TAAAA:La/X0mdG5GQpGwaaclf7sMfsrnF7oyE4MyVrp8rrCo3yidbOD+DuDazdyq1jkysRQIIPoaQBgI8exJJMeB8pO8t5/eHpWjL1MDiFTPBarSqLn9zDJX+cJsvn28FTIzZueaJUOCkJ3qEgpsXthPeOs4rBckMsYD4Im01SX5aiV0Q5dlqElqIbWVCTf76mttLri3SIz6IeiI71D3l+qrnkg537pjR9v7XXCumSv86Xvx8C8VSwBp0VSmC8Y5FdOrd8cew=; bkng_sso_auth=CAIQi4nT0gIaZngt8n/VKurp9pjo/EuwQRsGyacmh5F9ewoKw9nJCzFuchvPsk6KuRuQgKhNMNqZeuLByHqPLR5iHBF7fJQnUCtrFjfsopi/UrWUpSi0CKiNJu3pusrovUWrngl4jrsCeGzhDA2IIg==',
                }

                params = {
                    'label': 'gen173nr-10CAEoggI46AdIM1gEaGyIAQGYATO4ARnIAQzYAQPoAQH4AQGIAgGoAgG4Asq2w9EGwAIB0gIkNTk4MTYwYzctZDkwMi00N2E5LTlkYzctYTMwZmJmYzNmMjRk2AIB4AIB',
                    'sid': '96399a8f22c4f2fa18f24c3b6571317d',
                    'aid': '304142',
                }

                response = requests.get(
                    country_url,
                    params=params,
                    cookies=cookies,
                    headers=headers,
                )
                # print(response.status_code)
                # open("booking_2ndhit.html","w").write(response.text)

                if re.search(r'(?s)Airports in (.*?)Regions in',response.text):

                    airport_locations_grp = re.search(r'(?s)Airports in (.*?)Regions in',response.text).group(1)

                    airport_locations = re.findall(r'(?s)<a href=".*?html\?',airport_locations_grp)

                    print("length of airports:", len(airport_locations))

                    for airport in airport_locations:
                        airport_code = re.search(r'(?s)<a.*?airport/.*?/(.*?)\.',airport).group(1)

                        headers = {
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
                            # 'cookie': 'pcm_consent=analytical%3Dtrue%26countryCode%3DIN%26consentId%3D601acb98-d107-4ca7-a28c-1a48ff5165db%26consentedAt%3D2026-06-16T05%3A12%3A40.117Z%26expiresAt%3D2026-12-13T05%3A12%3A40.117Z%26implicit%3Dtrue%26marketing%3Dtrue%26regionCode%3DTN%26regulation%3Dnone%26legacyRegulation%3Dnone; bkng_sso_ses=e30; bkng_sso_session=e30; _gcl_au=1.1.1889586477.1781586767; bkng_prue=1; _yjsu_yjad=1781586767.34baa05c-ca26-435c-b5af-ff2f9c58d030; FPID=FPID2.2.1w7nYZLBgyBwIrDTfKK%2Faakw8yyKAocGkeryIMrAswg%3D.1781586767; FPAU=1.1.1889586477.1781586767; FPLC=UWIYpN4%2BzG%2BluzoJUwkx%2FxDQjX9Cq7xAtHxEur%2BCZ%2F%2FOEtLw737xA7qblxhec1olLo%2BfED0yO92GNOVDNOIdNy8P3TP2%2BJ%2Fmrby54IqM1NZU6p1xU%2FRA5bdoXXdzrg%3D%3D; cors_js=1; BJS=-; _gid=GA1.2.1711172287.1781586774; tj_seed=00754cafc0db23810ca9f3d70cfd000000; essentials_visitor=%7B%22correlationId%22%3A%22847daa6c-e7f7-4a5b-9b69-5d2524343007%22%7D; attribution=%7B%22adplat%22%3A%22cross_product_bar%22%2C%22affiliateCode%22%3A%22booking-cars%22%2C%22aid%22%3A%22304142%22%2C%22label%22%3A%22gen173nr-10CAEoggI46AdIM1gEaGyIAQGYATO4ARnIAQzYAQPoAQH4AQGIAgGoAgG4Asq2w9EGwAIB0gIkNTk4MTYwYzctZDkwMi00N2E5LTlkYzctYTMwZmJmYzNmMjRk2AIB4AIB%22%7D; tj_conf="tj_pref_currency:INR|tj_pref_lang:en|tjcor:in|"; pcm_personalization_disabled=0; cto_bundle=jZAmWF9sUDVrNzRLayUyRnAzSldvbkd1cWxHSDZuTmklMkZaYTF1TnpNWmhzRXVJMHQyVG9NM2U1NFklMkYlMkZ4dTg1QTRtYnlmcHNhJTJGSyUyRm1CZmdDSE9ac3VzSGlBQVVQVDg3dlRNZ2VzakxIQUZTZ2ttJTJCbEN4UUFQemw0R3VobUUlMkJhRXFlQUp5JTJGSQ; _rdt_uuid=1781586767152.e8b560e5-77b9-4543-8a4a-9d71011c5e56; __gads=ID=992c061e6617eac7:T=1781586766:RT=1781587200:S=ALNI_Mbjlr9VtLq1wnwI9zPO99kPBBfX4Q; __gpi=UID=0000146e434266ce:T=1781586766:RT=1781587200:S=ALNI_MbZJViEzC7nHpb_5dqbLiUaHVmONw; __eoi=ID=1cc2749210d2d1db:T=1781586766:RT=1781587200:S=AA-Afjac5jzpaLP91Yz0dx55TKf2; g_state={"i_l":0,"i_ll":1781587284625,"i_b":"kKGtDIhILlZ1mqYCX0ZJO+NsHXpeOWTNA0f4QFq5fCw","i_e":{"enable_itp_optimization":0},"i_et":1781587195920}; bkng_sso_auth=CAIQi4nT0gIaZr9ic9ELFX2o/KZ56rrU1imeEBmPSgLnbJR6non7unvOAMF7ln04qji4KeDfNksUE/VHu6OBTWaVJh/jXxW1cMp5MYegtLdbjirBSOANKuAsYxDNgwH1If+/7juTgxFrYD4KAxcWkg==; _ga=GA1.1.965424957.1781586767; _ga_A12345=GS2.1.s1781586767$o1$g1$t1781588742$j57$l0$h941685780; _uetsid=03f1d0d0694211f1abc7737fcf2f9680; _uetvid=03f209c0694211f19759df56ce1124f9; bkng=11UmFuZG9tSVYkc2RlIyh9Yaa29%2F3xUOLbiKbS0JOgDBK6LEtX6%2Blc%2BZDK4s2cKmmo5Q3UHoV%2BqrX5%2BbbJQcEsRcD8QcuTxodaijolKa02%2FCEcZCDGYQVe%2Fsa9t4aZlmZP7OJ5cqKIOf%2FI7MJ5OWUrwuzuOqZFyGHj8Pz%2FJ9CKRV1Ftxhd2cyMsRX5hj0ubN6fkQ%2F0dnnd6oU%3D; aws-waf-token=2862bf10-180c-4518-bd72-0c7f2807a9b3:HgoAj1gpB7oJAAAA:ZXL2Wvh6k5JKOhK2FVBneMZ4tUQos7P85pMe1J9aDST5MVe2SgdYfNzuwtvKDWof4B18l1mj3gmPidrcElO2dIt481psCk1lQeRgBciMFAv2Gxuir9dVHa3GCCTjQkF3MbHLe7ljiO2mpvXoJBuBHZouBI+fHP0uv+Oo9x/kH7u1CCXl8VSgOknZO0LowYyfMyg1AQ8ej1c2zmJqlHdDYRnWwRQtrAG97C90BSWB2HnD4CpdZxru',
                        }

                        params = {
                            'language': 'en',
                            'cor': 'in',
                            'aid': '304142',
                            'term': airport_code.upper(),
                        }

                        response = requests.get('https://cars.booking.com/api/location-suggestions', params=params, cookies=cookies, headers=headers)

                        print("airport name:", airport_code.upper())

                        for aa in json.loads(response.text):
                            created_date = datetime.now(timezone.utc).strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            if aa['placeType'] == 'A':
                                name=aa['name']
                                ter1=aa['lat']
                                ter2=aa['lng']
                                locationterm=f'{name}|{ter1}|{ter2}'
                                city = aa['city']
                                region = aa['region']
                                print("locationname :",airport_code.upper())
                                print("locationterm :",locationterm)
                                print("-"*10)

                                airport = True
        
                                if airport == True:
                                    location_type = "airport"
                                else:
                                    location_type = "city"
                                # if "latitude" in str(longitude1):
                                #     longitude = re.sub('"postcode":.*', "", str(longitude1))
                                # else:
                                #     longitude = longitude1
                                # locationname = re.sub(r"'", "''", locationname)
                                # address1 = re.sub(r"'", "''", address1)
                                # city = re.sub(r"'", "''", city)
                                # locationterm = re.sub("'", "''", locationterm)
                                # comp_code = 'EP'
                                row = {
                                    "id": refid,
                                    "source_name": source_name,
                                    "website_code": websitecode,
                                    "pickup_location": name,
                                    "location_country": country,
                                    "location_code": "",
                                    "is_airport": True,
                                    "created_date": created_date,
                                    "location_type": location_type,
                                    "city": city,
                                    "region": region,
                                    "priority_level": "",
                                    "location_term": locationterm,
                                    "location_name": name,
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
            SC = rentalcars(1, 5, 5, "input_locations", "locations", False, "1,2,3")
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



