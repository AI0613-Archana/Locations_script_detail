import requests
import urllib.parse
import pandas as pd
import requests
import re


def get_agency_id(lat, lon, radius=50, vehicle_type="truck"):
    cookies = {
        "NEXT_LOCALE": "fr",
        "sp_i": "19cffb9697534b7d6102d66",
        "axeptio_cookies": "{%22$$token%22:%22eTcncXcSJd7xtqsNNj03KrvTaP%22%2C%22$$date%22:%222026-03-18T06:54:33.397Z%22%2C%22$$cookiesVersion%22:{}%2C%22$$completed%22:false}",
        "axeptio_authorized_vendors": "%2C%2C",
        "axeptio_all_vendors": "%2C%2C",
        "accessToken": "%7B%22accessToken%22%3A%22YOUR_TOKEN%22%7D",
        "isUpsell": "false",
    }

    headers = {
        "accept": "text/x-component",
        "content-type": "text/plain;charset=UTF-8",
        "next-action": "7864189d02824d2a4b3a3c95f42bef772b97285a64",
        "origin": "https://www.ada.fr",
        "referer": "https://www.ada.fr/?type=truck",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64)",
    }

    params = {
        "type": vehicle_type,
    }

    # 🔥 Dynamic payload
    data = f'[{lat},{lon},{radius},"{vehicle_type}"]'

    try:
        response = requests.post(
            "https://www.ada.fr/",
            params=params,
            cookies=cookies,
            headers=headers,
            data=data,
            timeout=15,
        )

        if response.status_code != 200:
            print(f"❌ Request failed: {response.status_code}")
            return None

        text = response.text

        # 🔍 Extract agencyID
        match = re.search(r'agencyID":"(.*?)"', text)

        if match:
            return match.group(1)
        else:
            print("⚠️ agencyID not found")
            return None

    except Exception as e:
        print("❌ Error:", str(e))
        return None


headers = {
    "sec-ch-ua-platform": '"Linux"',
    "Referer": "https://www.ada.fr/",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}

pickup_locations = [
    "Agence CHALON SUR SAONE",
    "Agence ANTONY",
    "Agence ARRAS",
    "Agence BESSANCOURT",
    "Agence BEZIERS",
    "Agence CERNAY LES REIMS",
    "Agence ESTREES SAINT DENIS",
    "Agence GARCHES",
    "Agence MAINE DE BOIXE",
    "Agence MAINTENON",
    "Agence MERIGNAC",
    "Agence METZ",
    "Agence NIORT",
    "Agence ORLEANS SAINT VINCENT",
    "Agence PARIS 13 Place d’Italie",
    "Agence RENNES Gare - Centre",
    "Agence SAINT MARTIN D'HERES",
    "Agence SAINT PRIEST EN JAREZ",
    "Agence SAINTE PAZANNE",
    "Agence SAINTE SAVINE",
    "Agence TOULOUSE Av. des Etats Unis",
    "Agency CAPBRETON - ANGRESSE",
]
print(f"🔍 Total locations: {len(pickup_locations)}")

BASE_URL = "https://api.mapbox.com/geocoding/v5/mapbox.places/"

params = {
    "access_token": "pk.eyJ1IjoiYWRhMjAyMyIsImEiOiJjbG93em15ZzQxYjQ5Mm1zMTByNzd0d3k5In0.XxcU8vPZKJ8LEMWqS12Mxg",  # replace this
    "language": "fr",
    "country": "FR",
}
loaction_codes = []
for location in pickup_locations:
    session = requests.Session()
    # Encode location for URL
    encoded_location = urllib.parse.quote(location)

    url = f"{BASE_URL}{encoded_location}.json"

    response = session.get(url, headers=headers, params=params)

    print(f"\n🔍 Searching: {location}")

    if response.status_code == 200:
        data = response.json()

        # print(data)
        # exit()

        # Example: print first result
        if data.get("features"):
            first = data["features"][0]
            print("📍 Found:", first.get("place_name"))
            print("🌐 Coordinates:", first.get("center"))
            code = get_agency_id(first.get("center")[1], first.get("center")[0])
            current_location_data = {
                "location": location,
                "code": code,
                "location_term": first.get("place_name"),
            }
            loaction_codes.append(current_location_data)

        else:
            print("❌ No results")
            current_location_data = {
                "location": location,
                "code": None,
                "location_term": None,
            }
            loaction_codes.append(current_location_data)
    else:
        print("⚠️ Request failed:", response.status_code)

df = pd.DataFrame(loaction_codes)
print(df)
df.to_csv("ada_locations.csv", index=False)
