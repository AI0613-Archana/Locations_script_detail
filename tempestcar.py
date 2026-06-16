# -*- coding: utf-8 -*-
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import psycopg2
import requests
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

API_URL = "https://www.tempestcarhire.co.za/api/Locations/FetchInitialList"
DOMAIN = "https://www.tempestcarhire.co.za/"
SOURCE_NAME = "tempest"
WEBSITE_CODE = 57
STATUS = "1"
HEADERS = {
    "sec-ch-ua-platform": '"Linux"',
    "Referer": DOMAIN,
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "sec-ch-ua": '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
}


def _parse_branch_objects(branch_text):
    branches = []
    search_from = 0

    while True:
        start = branch_text.find('{"code":', search_from)
        if start == -1:
            break

        depth = 0
        in_string = False
        escaped = False

        for index in range(start, len(branch_text)):
            char = branch_text[index]

            if escaped:
                escaped = False
                continue

            if char == "\\":
                escaped = True
                continue

            if char == '"':
                in_string = not in_string
                continue

            if in_string:
                continue

            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    branches.append(json.loads(branch_text[start : index + 1]))
                    search_from = index + 1
                    break
        else:
            break

    return branches


def _salvage_truncated_tempest_payload(raw_text):
    groups = []
    group_pattern = re.compile(r'\{"groupName":"([^"]+)","branches":\[')
    matches = list(group_pattern.finditer(raw_text))

    for index, match in enumerate(matches):
        group_name = match.group(1)
        branch_start = match.end()
        branch_end = matches[index + 1].start() if index + 1 < len(matches) else len(raw_text)
        branch_text = raw_text[branch_start:branch_end]
        branches = _parse_branch_objects(branch_text)

        if branches:
            groups.append(
                {
                    "groupName": group_name,
                    "branches": branches,
                }
            )

    if not groups:
        raise ValueError("Tempest API returned malformed JSON and no complete branches could be recovered.")

    return groups


class tempestcar:
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
        response = requests.get(source_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        raw_text = response.text.strip()
        try:
            return json.loads(raw_text)
        except json.JSONDecodeError as exc:
            if exc.pos >= len(raw_text) - 1:
                return _salvage_truncated_tempest_payload(raw_text)
            raise

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
        for result in resultset:
            print(result)
            refid = result["id"]
            websitecode = result.get("websitecode") or self.websitecode
            source_name = result.get("source_name", SOURCE_NAME)
            source_url = result.get("source_url") or API_URL
            print("refid", refid, "source_url", source_url)
            try:
                response_data = self.load(source_url)
                rows = self.extraction(response_data, refid, websitecode, source_name)
                if rows:
                    self.insert(rows)
                    self.update(1, refid)
                else:
                    self.update(2, refid)
            except Exception as e:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                print("Processing error:", repr(e))
                self.update(2, refid)

    def extraction(self, response_data, refid, websitecode, source_name):
        rows = []

        for group in response_data:
            group_name = str(group.get("groupName", "")).strip()
            branches = group.get("branches", [])

            for branch in branches:
                created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                description = branch.get("description", "")
                parts = [part.strip() for part in str(description).split(",")]
                desc_country = parts[0] if len(parts) > 0 else ""
                desc_region = parts[1] if len(parts) > 1 else ""
                desc_city = ", ".join(parts[2:]) if len(parts) > 2 else ""
                is_airport = bool(branch.get("isAirportBranch"))
                country = branch.get("countryName") or desc_country
                region = branch.get("groupingName") or desc_region or group_name
                city = desc_city or branch.get("name", "")
                location_type = "airport" if is_airport else "city"
                location_term = description or branch.get("name", "")

                rows.append(
                    {
                        "id": refid,
                        "source_name": source_name,
                        "website_code": websitecode,
                        "pickup_location": branch.get("name", ""),
                        "location_country": country,
                        "location_code": branch.get("code", ""),
                        "is_airport": is_airport,
                        "created_date": created_date,
                        "location_type": location_type,
                        "city": city,
                        "region": region,
                        "priority_level": "",
                        "location_term": location_term,
                        "location_name": branch.get("name", ""),
                    }
                )

        return rows


if __name__ == "__main__":
    RETRY = 1
    while RETRY < 20:
        SC = None
        try:
            SC = tempestcar(0, 149, 149, "input_locations", "locations", False, "20")
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
