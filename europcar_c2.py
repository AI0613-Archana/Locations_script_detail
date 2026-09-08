# -*- coding: utf-8 -*-
import os,re
import sys
import time
import json
import random
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


class europcar_c2:
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
        self.websitecode = 30
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

    def get_proxy(self, country=""):
        if not self.proxyset:
            return {}
        proxy_str = self.proxyset[random.randrange(0, len(self.proxyset))]["proxy"]
        if country:
            proxy_str = re.sub(r"(rentalcars-res-)[a-zA-Z]{2}", rf"\1{country.upper()}", proxy_str)
        return {"https": f"http://{proxy_str}", "http": f"http://{proxy_str}"}

    def load(self, url, params, headers, cookies, proxies):
        return requests.get(
            url,
            params=params,
            cookies=cookies,
            headers=headers,
            proxies=proxies,
            impersonate="chrome",
            timeout=15,
        )

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
        cookies = {
            "CookieConsent": "{stamp:%27wF/EVh1REYLh0saXFAyuoRFrRASpTVn2JljafT57EdYug7wRR67cxA==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27explicit%27%2Cver:1%2Cutc:1788852203729%2Cregion:%27in%27}",
            "_gcl_au": "1.1.449267266.1788852204",
            "_ga": "GA1.1.1565244333.1788852196",
            "FPID": "FPID2.2.oPVh2QJpGJdkWtdAwGe9RLVW2stlbCAVA4Yk016O8qU%3D.1788852196",
            "FPLC": "zVYf04KSv1tp3KzSFOxCQmnE1QBlGtU%2FS6k5qWR3tRVHTWG67MMXU58KmA5ycpAZRUwnxj2QecMAvvkPC%2FGAXIh7nq%2Fo7o8h5b%2FKWVJsXFBh2DH0CKcvM4VC3XVGqA%3D%3D",
            "__kla_id": "eyJjaWQiOiJaakF6TVRnMU1ETXRaVGRrTUMwME5tUmpMV0ZsTW1RdFpEZGtaalU0WmpWbE9HTmkifQ==",
            "_clck": "hh7o03%5E2%5Eg9a%5E0%5E2442",
            "cuvid": "cf1773f845fb490fa737c067f4998663",
            "_fbp": "fb.1.1788852207159.729531470482119794.AQYAAQIB",
            "bm_sz": "CEE84DEBC088C680890CEB66701739A3~YAAQd6LfrcWgCUegAQAAq2ZggAH9DMd8XQz8oLB1DN0bMYLKGTx8sv73iIIw/g7Owrt4jUZtSdMAO2u8aSV8Cu0N0lWgv/LsrE9Jrt/Qamn/oofTw+VW1NUUL6lOceOLAtT58gFcHOpRL4eKdc6x9tx++qh2SHnRMdxjPxhO8NxgFTBhaY225g7nrlbLS71j/LmXeqeJ6eeyXyhlFTv8frVSg+69BVeupo3hiNioNKrL6iUdrl6cvgQmun9rlnF4ItVFdf5cE11XA8nFG9ZsIxK/bzTGQz7lRcWXqizC4YnFmjYnz1qsJFN0qiZClodh25BJ7B0wNkY5vEI5LghDyp48JpNyRynI11PTg7/XHEEim831wljw/d63yIf89mkAzPcsK3/c9OaKAwfQE3WGia7YhP2bER0twuKUxOW1H276IO032Yg=~4404033~4473906",
            "_abck": "C14D3DCC691B5DAAA796A589ED998E5C~0~YAAQd6LfrXWiCUegAQAADmtggBBhdsBwFvhmZPtlV7II0Lp3p7FY0BZYAcn2LFq7fV/r7vVcp3TdgoSDOWPwEa9MSJfBjllRRQ+/SAwhVq91uXRl3K0hJr5O6jA7OvstvxkfEM28szoLqm4qo0dPvcE4Wj8OVFiaD0vo/u9dn4MgurHPLFMrGIQwwP+s7NqthZSo1Zf0F41SZ7zJem1EWwyaeVuxAhXu14uzWmBmk7MsQ5wG//HGwG+c3PkXqmbdspf3dneQ+PxDZkWPCf+sLFSEFMqm2zcp1vz/WIP3mqDRsdik51uNStdQBvw9ZrYR+3SMqzYt8ypZSwIRbXVQHRyosy8Fsh/4NLKEb2Ew2VymFzkY/tkH9tw8swjwXUu7QHG/DfgTSb7oMwdfwDqaUzucFlNnY8YhJU4fMVj2vRUIlI+pGmbrOJUEEsnkxKY2RwpjrcswqGzXVr7Rg9nywQGD73NRzyRgXsOAkpSidAX96xF1/gK52h5hTnLOfQEf24edAXkhJLq07iHw8XFS7pnR6tuHWiDfzsS0jta5Jm8p0+wEsOjAyP6K6g507sIkwbj8zPTGNnMLrStKEbWT3Bj4qNodMeuMwpuAGttrpkk0GHqPjfQSAPD8qgILQqxLw7m0FIPWWH64jayxvlEyzlUO6WkBuCRT3GDehhU/A8TjwWL+MEAsMr+f~-1~-1~-1~AAQAAAAG%2f%2f%2f%2f%2fyUyO85WiRBbNPx2%2fE4MjsYHIq+pu0nf5KomvZTxZCYLZs6Sky6tctBqEbmXvVC%2fGBZTrZVWt21agohzlSyqchEEeEqAvkGYMfgHqQTuX3gmChsW3EEkaw52VRJmm3w0oLHjjtI%3d~-1",
            "viewport": "2xl",
            "cusid": "1788860199221",
            "cuvon": "1788860199237",
            "ak_bmsc": "B44614FA4722469850F5F713A6B51F29~000000000000000000000000000000~YAAQd6LfrXykCUegAQAANXFggAELDXXLm8KLoy8RMQPWe5ChNlt4w3oAfWx+AW6bM9kCzmwj1eSPnpea0QvGbmeZMfsbg8wmo0ekYVP2zDajKB+QWZX+wV/OpMf5G+Buwa0wnpnvT+xlToNKLC1hBBcZ1fGsRWddYnSq7gOT3pVxVFzzuc5CgQHdbFCiq8oWvfrIIAh7e3Edko/n+jEjOD7P0SyNeuG2RC2LnPvzHu1GIV3pkWGrngRwnNnGBA9BsEe3vIAHtSaEmp1Mjc7OHUIedxf08kZLVz2dnxTyRr9YzjsX4YmVYlwO3lQPoYe8wFW9ZnNo8wZn4IIKTERBCpNuScRYw0PEfE4waNvLYPzfkrQTCbo2tdc0vGxwUmriTL2b78rbujBVYrgtwTG4NJo8vIETwMAEmO6ldf0JhAOUoLIpxg/Ayt1nG4tE7g+dWtwgJ/MabnhVHL9H65W4TmiAWotaoPI=",
            "_ga_L6CSQBC4D3": "GS2.1.s1788860199$o3$g0$t1788860199$j60$l0$h1821639521",
            "FPGSID": "1.1788860200.1788860200.G-L6CSQBC4D3.JjA5eRixQ7DivQ11f9jPGA",
            "_clsk": "zvcey2%5E1788860202082%5E1%5E1%5Eb.clarity.ms%2Fcollect",
        }
        headers = {
            "accept": "application/json",
            "accept-language": "en-US,en;q=0.8",
            "priority": "u=1, i",
            "referer": "https://www.europcar.dk/",
            "sec-ch-ua": '"Not=A?Brand";v="99", "Brave";v="151", "Chromium";v="151"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "sec-gpc": "1",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
        }

        for result in resultset:
            print(result)
            refid = result["id"]
            websitecode = result["websitecode"]
            source_name = result["source_name"]
            country = result.get("country", "")
            
            for input_id in range(1, 101):
                params = {
                    "bookType": "ECBOOK",
                    "id": str(input_id),
                }

                try:
                    proxies = self.get_proxy(country)
                    source_url = "https://www.europcar.dk/api/EC/GetLocationDetails"
                    response = self.load(source_url, params, headers, cookies, proxies)
                    print(response.status_code)
                    if response.status_code == 200 and response.json():
                        try:
                            data = response.json()
                            self.extraction(data, refid, input_id, websitecode, source_name, country)
                        except Exception as e:
                            print(f"Exception during extraction for {input_id}: {e}")
                    else:
                        print(f"data is not present for this input id {input_id}")
                except Exception as e:
                    print(f"Request failed for {input_id}: {e}")
                
                time.sleep(2)

    def extraction(self, data, refid, input_id, websitecode, source_name, country):
        if data:
            id_val = str(data.get("id", ""))
            name = str(data.get("name", ""))
            loc_type = str(data.get("type", ""))

            is_airport = (
                "lufthavn" in name.lower()
                or "airport" in name.lower()
                or loc_type.lower() == "airport"
            )
            created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            extracted_data = {
                "id": refid,
                "source_name": source_name,
                "website_code": websitecode,
                "pickup_location": name,
                "location_country": data.get("country", "") or country or "DK",
                "location_code": id_val,
                "is_airport": is_airport,
                "created_date": created_date,
                "location_type": loc_type,
                "city": data.get("city", ""),
                "region": "",
                "priority_level": "",
                "location_term": name,
                "location_name": name,
                "booking_country": country,
            }
            print(extracted_data)
            self.insert([extracted_data])
            self.update(1, refid)
        else:
            self.update(2, refid)

if __name__ == "__main__":
    RETRY = 1
    while RETRY < 20:
        SC = None
        try:
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
            SC = europcar_c2(
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
        RETRY += 1
