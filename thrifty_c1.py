# -*- coding: utf-8 -*-
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import airportsdata
import psycopg2
from curl_cffi import requests
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}

SOURCE_URL = "https://www.thrifty.com.gr/"
SOURCE_NAME = "thrifty_gr"
WEBSITE_CODE = 58
SEARCH_URL = "https://www.thrifty.com.gr/el/Resources/SearchBranchLocations"
THREAD_COUNT = 20

airport_data = airportsdata.load("IATA")

COOKIES = {
    "cp_total_cart_items": "0",
    "cp_total_cart_value": "0",
    "cpab": "b0921252-eb2c-4c5d-c5db-6adb42bafb7d",
    "ASP.NET_SessionId": "f0uffhlxmgyavhp0tiw2k3nc",
    "__RequestVerificationToken": (
        "l-YGsFAEHfMPS0luw36UU62mrRrN8q5Pps7WdnfmuG0E8brgTMTF6wYqh7vINvkKZ2yOgcc9nv1qHxfAk7iWt7U6NUORwUn8q7yDq-MfZYA1"
    ),
    "cp_sessionTime": "1781853513759",
}

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://www.thrifty.com.gr",
    "Referer": SOURCE_URL,
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
    "sec-ch-ua": '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
}

session = requests.Session()


def extract_locations(response_json: dict, search_value: str) -> list[dict]:
    """Pull the raw Locations payload out of a single search response."""
    return response_json.get("Locations", [])


def format_location_row(item: dict, search_value: str) -> dict | None:
    label = (item.get("Label") or "").strip()
    location_code = item.get("Value", "")
    location_country = item.get("Country", "")

    if not (label and location_code):
        return None

    is_airport = not label.upper().startswith(f"{search_value.upper()},")
    airport_meta = airport_data.get(search_value.upper(), {})

    return {
        "source_name": SOURCE_NAME,
        "website_code": WEBSITE_CODE,
        "pickup_location": search_value.upper(),
        "location_country": location_country,
        "location_code": location_code,
        "is_airport": is_airport,
        "location_type": "airport" if is_airport else "city",
        "city": "",
        "region": airport_meta.get("subd", ""),
        "priority_level": "",
        "location_term": label,
        "location_name": label,
    }


class ThriftyGrIngestor:
    def __init__(
        self,
        status: int,
        start_id: int,
        end_id: int,
        input_table: str = "input_locations",
        output_table: str = "locations",
    ) -> None:
        self.input_table = input_table
        self.output_table = output_table
        self.website_code = WEBSITE_CODE
        self.connection = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        self.length_limits = self._get_length_limits()

        self.cursor.execute(
            f"""
            SELECT * FROM {self.input_table}
            WHERE websitecode = %s AND status = %s
              AND id BETWEEN %s AND %s
            """,
            (str(self.website_code), status, start_id, end_id),
        )
        self.input_rows = self.cursor.fetchall()

    def _get_length_limits(self) -> dict[str, int]:
        self.cursor.execute(
            """
            SELECT column_name, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = %s
              AND character_maximum_length IS NOT NULL
            """,
            (self.output_table.split(".")[-1],),
        )
        return {
            row["column_name"]: row["character_maximum_length"]
            for row in self.cursor.fetchall()
        }

    def search(self, search_value: str, source_url: str):
        return session.post(
            source_url,
            cookies=COOKIES,
            headers=HEADERS,
            data={"term": str(search_value).lower()},
            timeout=30,
        )

    def insert_one(self, row: dict) -> None:
        columns = [column for column in row if column != "id"]
        values = []
        for column in columns:
            value = row[column]
            max_length = self.length_limits.get(column)
            if isinstance(value, str) and max_length:
                value = value[:max_length]
            values.append(value)

        column_names = ",".join(columns)
        placeholders = ",".join(["%s"] * len(columns))
        query = (
            f"INSERT INTO {self.output_table} "
            f"({column_names}) VALUES ({placeholders})"
        )
        self.cursor.execute(query, tuple(values))

    def update_status(self, input_id: int, status: int) -> None:
        self.cursor.execute(
            f"UPDATE {self.input_table} SET status = %s WHERE id = %s",
            (status, input_id),
        )

    def fetch_rows_for_input(self, input_row: dict) -> list[dict]:
        refid = input_row["id"]
        source_name = input_row.get("source_name") or SOURCE_NAME
        source_url = input_row.get("source_url") or SEARCH_URL
        country = input_row.get("country") or input_row.get("location_country") or ""

        airport_codes = [
            code for code, meta in airport_data.items() if meta.get("country") == country
        ]
        if not airport_codes:
            airport_codes = list(airport_data.keys())

        created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        seen_keys = set()

        with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
            futures = {
                executor.submit(self.search, code, source_url): code
                for code in airport_codes
            }
            for future in as_completed(futures):
                search_value = futures[future]
                try:
                    response = future.result()
                    if response.status_code != 200:
                        continue
                    for item in extract_locations(response.json(), search_value):
                        row = format_location_row(item, search_value)
                        if row is None:
                            continue
                        unique_key = (
                            search_value.upper(),
                            row["location_code"],
                            row["location_term"],
                        )
                        if unique_key in seen_keys:
                            continue
                        seen_keys.add(unique_key)
                        row["id"] = refid
                        row["source_name"] = source_name
                        row["created_date"] = created_date
                        rows.append(row)
                except Exception:
                    continue

        return rows

    def ingest(self) -> int:
        inserted_total = 0

        for input_row in self.input_rows:
            refid = input_row["id"]
            try:
                rows = self.fetch_rows_for_input(input_row)

                inserted_for_input = 0
                for row in rows:
                    self.insert_one(row)
                    inserted_for_input += 1

                self.update_status(refid, 1 if inserted_for_input else 2)
                self.connection.commit()
                inserted_total += inserted_for_input
                print(f"id {refid}: inserted {inserted_for_input} rows")
            except Exception:
                self.connection.rollback()
                traceback.print_exc()
                self.update_status(refid, 2)
                self.connection.commit()

        return inserted_total

    def close(self) -> None:
        try:
            self.cursor.close()
            self.connection.close()
        except Exception:
            pass


def main() -> None:
    ingestor = None
    try:
        ingestor = ThriftyGrIngestor(
            status=int(os.getenv("THRIFTY_INPUT_STATUS", "0")),
            start_id=int(os.getenv("THRIFTY_START_ID", "265")),
            end_id=int(os.getenv("THRIFTY_END_ID", "265")),
        )
        inserted = ingestor.ingest()
        print(f"Inserted {inserted} Thrifty GR location rows")
    except Exception:
        if ingestor:
            ingestor.connection.rollback()
        traceback.print_exc()
        raise
    finally:
        if ingestor:
            ingestor.close()


if __name__ == "__main__":
    main()