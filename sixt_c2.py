import json
import os
import re
import traceback
from datetime import datetime, timezone
import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor


load_dotenv()


DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 6438)),
    "dbname": os.getenv("DB_NAME", "abg_staging"),
    "user": os.getenv("DB_USER", "tooluser"),
    "password": os.getenv("DB_PASSWORD"),
}


SOURCE_URL = "https://www.sixt.com.tr/"
SOURCE_NAME = "sixt_tr"
WEBSITE_CODE = 55


COOKIES = {
    "CookieConsent": "{stamp:%27hRFEXnA6Se5amHnKhe52blk6TbKc07eJK13AJ8KTSHSPiaf8AxAQIw==%27%2Cnecessary:true%2Cpreferences:false%2Cstatistics:false%2Cmarketing:false%2Cmethod:%27explicit%27%2Cver:2%2Cutc:1787554418660%2Cregion:%27in%27}",
    "XSRF-TOKEN": "eyJpdiI6ImJSM1FLTXIzZHBaeFRSTlZuWGE4YlE9PSIsInZhbHVlIjoieEIxcGp1eUc1VmF0V0tIclJYQWY4bS9qdC9BK3paLzgxUmF1Ri9lY2FkeFJndysxR2Rab1l6d3RQV3lWa29uK3NCc2k1QkxrTGxscEQ1MHVUYzN3NmVVSEMwVlpqNTlpTnMzcU14RFdqS0pzM1JWWXRuL1V1bGJIZ3Q2M0QvRXQiLCJtYWMiOiI4ZTA0NjhmNWRiMWMzMjE0YmJlMTg5N2U2MTAwNGVhOTMxZmE1MWNlYzY2NWQ5NThjN2Y1YzQ4YmJmZjIyMjdhIiwidGFnIjoiIn0%3D",
    "orange_session": "eyJpdiI6IkNnMlVMZFppV0V5d3hDeUw2bXU3Vmc9PSIsInZhbHVlIjoiaTNGOXVrWUdORFp1QXJUU1lZYWZoc0c3eHdkcER6WW02S2M4ZUVBNnNjQmVReGVKVU91NlZtWE56SWJrRkZqcGVFMUJYN2ZhTG9LRkwzcXllMzJ3bzRPdG9Qb0hrQStFOHk5L0lFRjkzMzRZN1k3L01mYWdCMkV6MDNJNWtEejIiLCJtYWMiOiJkZTQxM2JhZjY4ZWUxNjlmNzViY2QxYzRhMjIyY2FlZmFkYmY2ZjkwMjg5ZTlmNTg0ODhjNGVjMDg1NGE5YWVmIiwidGFnIjoiIn0%3D",
}


HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=4, i",
    "referer": "https://www.sixt.com.tr/",
    "sec-ch-ua": '"Not=A?Brand";v="99", "Google Chrome";v="151", "Chromium";v="151"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "no-cors",
    "sec-fetch-site": "same-origin",
    "sec-purpose": "prefetch",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
}


def extract_station_data(html: str) -> list[dict]:
    match = re.search(r"var trStationData = (\[.*?\]);", html, re.S)
    if not match:
        raise ValueError("Could not find trStationData in the response body")
    return json.loads(match.group(1))


def format_station_row(station_entry: dict) -> str | None:
    station = station_entry.get("station", {})
    location_term = station.get("name") or station.get("display_name") or ""
    location_code = station.get("code") or ""
    station_id = station.get("id") or ""

    if not (location_term and location_code and station_id):
        return None

    return f"{location_term}|{location_code}|{station_id}"


class SixtStationIngestor:
    def __init__(
        self,
        status: str,
        start_id: int,
        end_id: int,
        input_table: str = "input_locations",
        output_table: str = "locations",
    ) -> None:
        self.input_table = input_table
        self.output_table = output_table
        self.connection = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        self.length_limits = self._get_length_limits()

        self.cursor.execute(
            f"""
            SELECT * FROM {self.input_table}
            WHERE websitecode = %s AND status = %s
              AND id BETWEEN %s AND %s
            """,
            (WEBSITE_CODE, status, start_id, end_id),
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

    def ingest(self, station_entries: list[dict]) -> int:
        inserted = 0
        created_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        for input_row in self.input_rows:
            inserted_for_input = 0
            seen_station_ids = set()
            for station_entry in station_entries:
                station = station_entry.get("station", {})
                location_term = (
                    station.get("name") or station.get("display_name") or ""
                ).strip()
                locationcode = str(station.get("code") or "").strip()
                station_id = str(station.get("id") or "").strip()
                if not (location_term and locationcode and station_id):
                    continue
                if station_id in seen_station_ids:
                    continue
                seen_station_ids.add(station_id)

                row = {
                    "id": input_row["id"],
                    "source_name": input_row.get("source_name") or SOURCE_NAME,
                    "website_code": WEBSITE_CODE,
                    "pickup_location":location_term, 
                    "location_country": "TR",
                    "location_code" : f"{station_id}|{locationcode}",
                    "is_airport": "AIRPORT" in location_term.upper(),
                    "created_date": created_date,
                    "location_type": "Airport"
                    if "AIRPORT" in location_term.upper()
                    else "Station",
                    "city": location_term,
                    "region": "",
                    "priority_level": "",
                    "location_term": location_term,
                    "location_name": location_term,
                }
                self.insert_one(row)
                inserted += 1
                inserted_for_input += 1

            self.update_status(input_row["id"], 1 if inserted_for_input else 2)

        self.connection.commit()
        return inserted

    def close(self) -> None:
        try:
            self.cursor.close()
            self.connection.close()
        except Exception:
            pass


def fetch_station_entries() -> list[dict]:
    response = requests.get(SOURCE_URL, cookies=COOKIES, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return extract_station_data(response.text)


def main() -> None:
    station_entries = fetch_station_entries()

    ingestor = None
    try:
        ingestor = SixtStationIngestor(
            status=os.getenv("SIXT_INPUT_STATUS", "1"),
            start_id=int(os.getenv("SIXT_START_ID", "232")),
            end_id=int(os.getenv("SIXT_END_ID", "232")),
        )
        inserted = ingestor.ingest(station_entries)
        print(f"Inserted {inserted} Sixt station rows")
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
