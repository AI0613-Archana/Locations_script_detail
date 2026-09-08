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
        self.websitecode = 28
        self.is_dc_input = False
        self.cursor.execute(
            f"SELECT proxy FROM proxy_list WHERE status IN ({self.proxyid})"
        )
        self.proxyset = self.cursor.fetchall()

        self.cursor.execute(
            f"""
            SELECT * FROM {self.inputtable}
            WHERE websitecode = %s AND status = %s AND id BETWEEN %s AND %s
        """,
            (self.websitecode, status, startid, endid),
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
            'bm_sz': '948BDAED132AEA5D38D681279416F673~YAAQhaLfrWyhYnygAQAAjr+YgAGPXvdhXs8WBX79erxXABoXIS84K+VhBCEkjms+NHMzUpKVBtdlvx6L0xvJTCRWY3yr6WR/WCWn4fVoKUTmIOlsQn6Js+clNItrIOMnD4viVahI7sFWHgQxN3LbiiyV2Xx15/MPEcg23KRqgok9BjzQE2guDM0bupUoIlVDKdW02zJuNOhkXd84vp9ztQ0P+1GIbgcAq5J9vGe9TBJssBkdl+57kBPPoYfeOCFrLV0bVWVZtg8Te6f1QfpuvekKNI91UPGGY2KFhSx68aVgyru05c8cy8BV1sjguIHPcQB5G2VBfnmu3F5xQXws/HVYoE1oFpqhC0/BCZcpyMAQRKEuhnMkmu13a5nVbIjABRrU9NtxQmDY9nM5mvEf4+fPVrh7/oowjU5t~4272438~3225411',
            'bm_sv': 'F2C99350372FD9788270D4B1A926F2AE~YAAQhaLfrUikYnygAQAAeMmYgAGysIUktW5fV5xceYIXdGsUkuVtgoQ7NxtXTKQ7Shbypj1+qGYTiHRjAehnpxQCXxVIqbM3hdecip0u19J1DoUSEhXN/KXg/TSne/FDhn4qJb1pgidVpJcanPCuwtjPzEG01kL25Rn2K2V8lg4E3bGWttBpH3YIrsI8JbRReviol56z4aIYGmx7IvZHp8jL+J1nwOsb/Z1GT5KJu6VlCFpzCCW0PtFHmuGJFgv8xg==~1',
            'ak_bmsc': 'D82DC82B04647E0A5DB679C0226360F4~000000000000000000000000000000~YAAQhaLfrZqkYnygAQAAMMuYgAFFI9zsXASJwcGl+RDOzkDvc2JRyxuIN4t5rvE/K16Nl62RHZoPbiRtN+srbkQ5EOpeijrWZuPlbCFJmNWF5G2Lqh1laoDmfx0MOmDVLW2OocqRpooYoXeopWLebzhq/sPc0EwGaRMCiBSsLOVFJnqA3Ehq5GWmcbzE/Sdlm1JmG+F6y07xAZDCaQ+MEioLv6ZbFnBqywh4dA+GnixXU/2g+X0Dk/0MPOJuVE4NKD6m/5DTQuX+hZmkfdEPgxgWKZK9GpihuLKVrm6n8sGX3fN9DJtV1pMXTYcPk/JWfoQsq93XacNptWT0PKCK6bbRJOz1DcthG8K5o5dehBjHu0rFF4GdGRm7b/143Q4EJSLwlHFz+pWee6ELGY6VaZNgaZKZc1qPOP5aTR9mJyf1FgDWDmftS1XDBwdO7HVmdD55xViyWTeANqlLExxPXF/3XmKgM+A=',
            'CookieConsent': '{stamp:%27U7ftMlg2hVDGDqot+rJcCAgLKDH0WZ4Urm9msrwdVK9dYffubPvOig==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27explicit%27%2Cver:1%2Cutc:1788863892757%2Cregion:%27in%27}',
            '_abck': 'BBEEBAE393FCF6251FBD3A2851489757~0~YAAQhaLfrVKlYnygAQAARc6YgBAbRKfJDPst0VwUtJwevL23KfiJe0Y72Oq9ZPHkuY5J5ue6zq7XXJslYoK71lKMYMYFgPFCW8HIxjSl07VdJXWp64FeL4o9My8DRojiJHfSNUbBdY+bAXfO5R+Ar+aFFxBrCK732Ay59dcRLZpT0tkW2qB+Gekce+YYOu7UL3B4mpDnid9XL1Zo3/JOXzE4lC64n56vEtsjRmlhCWfunZigfP+Iej6YQMmfciDai7yBfUy+gcK2x61w5SB40QsSsVl7h7cB/j/faInQrBgFbvGsQSA3Qmu6Q8vd67Q7nwcnU4URVMaygfdHo/cIAaWq5fs46y6ttkr3AQq+7LrbUku0PTFmEt5OgqZ9/Q53+md7W49fCyqyjCsLQclmigwVHpZELzHEDLrxj+tcm27kAVaRH9cLB+hphPtrMBB2msvR8LKlRXz29UrZby1D1XDFEbF4uvTMzfmSzkqiJMmNGL8o/0bTpJt2PUxnjrUh5vk27ZDxTWi6KlTcs6M8gO4fypL5bVzeAHthqubzhCl1NJvBVgHBL7QpDg0voUyVQCRKtgjCEYP/OG/KWLEg1EyEBJSg2eY6ifpRKCJt7cog+GPFLJ3xWfSpbcGOvYx+dkLyW/7+l/Khjbdh5GP0zw==~-1~-1~-1~AAQAAAAG%2f%2f%2f%2f%2f5nII9vTiXfRd5IabHaRut85LoMqlE51RKayljDdVp19tOOfdTUDJsbhxebmMEvLwattml9fpUfsl41XwSbzRN9P3Vm+CY5LZDo2~-1',
            '_ga_L6CSQBC4D3': 'GS2.1.s1788863893$o1$g0$t1788863893$j60$l0$h902591252',
            '_ga': 'GA1.1.1856958871.1788863893',
            '_gcl_au': '1.1.698558766.1788863893',
            'FPID': 'FPID2.2.XuyZXxpVQnBo46s2V3FoaApndTtZKjkEWHCHv4qZZX8%3D.1788863893',
            'FPLC': '6cmjjDi4BYSR8Ei4%2B%2BcUJ%2BfMYPHiqrSxCVK85q9VDN1n3dUuEVEmKiLecC7TaaV%2Bb4arsIPuF01udtL4dvgHqc5iMTaaoDe1T3xHziNxejjMhwhGRIM8weN3LS1s5w%3D%3D',
            'FPGSID': '1.1788863894.1788863894.G-L6CSQBC4D3.uIMfEdN0zZ0dNJ9VFXRsaA',
            '__kla_id': 'eyJjaWQiOiJPRE5rTlRSbE9HUXRPV1E0WVMwME56RXdMVGsxWXpNdE4yVmhOVFl6WlRobE4yRTEifQ==',
            '_clck': '1mhl381%5E2%5Eg9a%5E0%5E2442',
            'cusid': '1788863895160',
            'cusid': '1788863895160',
            'cuvid': 'c8e4f0f60305414f83c6594efb69c273',
            '_clsk': '73vnve%5E1788863896085%5E1%5E1%5Ep.clarity.ms%2Fcollect',
            '_fbp': 'fb.1.1788863894944.82957882433276904.AQYAAQIB',
            'cuvon': '1788863896601',
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
            
            for input_id in [28, 64]:
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
            SC = europcar_c2(0, 10, 10, "input_locations", "locations", False, "60")

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
            # SC = europcar_c2(
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
