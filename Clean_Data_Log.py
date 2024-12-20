from flask import Flask, request, jsonify
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import re
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
from datetime import datetime
import pytz
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import os
import time
import pycountry

# Initialize Flask app
app = Flask(__name__)

# Configure seed for consistent results with langdetect
DetectorFactory.seed = 0

# Initialize the geolocator
geolocator = Nominatim(user_agent="location_parser", timeout=5)

# Configure authentication with Google Sheets and Drive
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

google_credentials_json = os.getenv('GOOGLE_CREDENTIALS')
if not google_credentials_json:
    raise ValueError("Environment variable 'GOOGLE_CREDENTIALS' not set or invalid.")
creds_dict = json.loads(google_credentials_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
drive_service = build('drive', 'v3', credentials=creds)

# Variables for Google Sheets and Drive
url_data = os.getenv('Url_Data')
url_data_clean = os.getenv('Url_DataClean')
folder_id = os.getenv('var_FolderID')

if not url_data or not url_data_clean or not folder_id:
    raise ValueError("One or more required environment variables are not set (Url_Data, Url_DataClean, var_FolderID).")

# Function to parse location with retry mechanism
def parse_location(location):
    if pd.isna(location) or location.strip() == "":
        return {"City": "Unknown", "State": "Unknown", "Country": "Unknown", "Postal Code": None}
    try:
        # Dividir la ubicación en partes basadas en comas
        parts = [part.strip() for part in location.split(",")]

        city = "Unknown"
        state = "Unknown"
        country = "Unknown"

        # Si solo hay una parte, comprobar si es un país
        if len(parts) == 1:
            single_part = parts[0]
            # Validar si el texto es un país
            if pycountry.countries.get(name=single_part) or pycountry.countries.get(alpha_2=single_part):
                country = single_part
            else:
                city = single_part
        else:
            # Asignar el primer elemento como City
            if len(parts) >= 1 and parts[0]:
                city = parts[0]

            # Asignar el segundo elemento como State/Region
            if len(parts) >= 2 and parts[1]:
                state_candidate = parts[1]
                if re.fullmatch(r"[A-Z]{2,3}", state_candidate):
                    state = state_candidate
                else:
                    state = state_candidate

            # Asignar el último elemento como Country
            if len(parts) >= 3 and parts[-1]:
                country_candidate = parts[-1]
                if len(country_candidate) >= 3:
                    country = country_candidate

        return {"City": city, "State": state, "Country": country, "Postal Code": None}

    except Exception as e:
        print(f"Error parsing location '{location}': {e}")
        return {"City": "Unknown", "State": "Unknown", "Country": "Unknown", "Postal Code": None}

# Function to process data
def process_data():
    local_tz = pytz.timezone('America/Lima')
    timestamp = datetime.now(local_tz).strftime("%Y%m%d%H%M%S")

    sheet_input = client.open_by_url(url_data)
    worksheet1 = sheet_input.get_worksheet(0)

    sheet_output = client.open_by_url(url_data_clean)
    worksheet2 = sheet_output.get_worksheet(0)

    headers = worksheet1.row_values(1)
    rows = worksheet1.get_all_values()[1:]

    data = pd.DataFrame(rows, columns=headers)

    for col in ["Mail From Dropcontact", "Email", "Professional Email", "Phone", "Phone Number From Drop Contact"]:
        if col not in data.columns:
            data[col] = None

    data.insert(0, 'Nro', range(1, len(data) + 1))
    data['log'] = data['Nro'].apply(lambda x: f"{timestamp}-{x}")

    def is_valid_email(email):
        if email and isinstance(email, str):
            regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            return re.match(regex, email) is not None
        return False

    def get_valid_email(row):
        email_columns = ["Mail From Dropcontact", "Email", "Professional Email"]
        for col in email_columns:
            if col in row and is_valid_email(row[col]):
                return row[col]
        return "invalid@loriginal.org"

    data["Valid Email"] = data.apply(get_valid_email, axis=1)

    def clean_phone(phone):
        if pd.isna(phone) or phone.strip() == "":
            return ""
        cleaned = re.sub(r'[^\d+]', '', phone)
        if len(cleaned) >= 8:
            return cleaned
        return ""

    data["Phone"] = data["Phone"].apply(clean_phone)
    data["Phone Number From Drop Contact"] = data["Phone Number From Drop Contact"].apply(clean_phone)

    def get_combined_phone(row):
        if row["Phone"]:
            return row["Phone"]
        if row["Phone Number From Drop Contact"]:
            return row["Phone Number From Drop Contact"]
        return ""

    data["Combined Phone"] = data.apply(get_combined_phone, axis=1)

    def detect_language(description):
        if description:
            try:
                return detect(description)
            except LangDetectException:
                return "en"
        return "en"

    data['language'] = data['Description'].apply(detect_language)

    # Actualización de columnas con parse_location
    location_components = data["Location"].apply(parse_location)
    data["City"] = location_components.apply(lambda x: x["City"])
    data["State"] = location_components.apply(lambda x: x["State"])
    data["Country"] = location_components.apply(lambda x: x["Country"])
    data["Postal Code"] = location_components.apply(lambda x: x["Postal Code"])

    output_columns = [
        "Nro", "FirstName", "Last Name", "Full Name", "Profile Url",
        "Mail From Dropcontact", "Email", "Professional Email", "Valid Email",
        "Phone", "Phone Number From Drop Contact", "Combined Phone", "Location",
        "City", "State", "Country", "Postal Code", "Company", "Job Title",
        "Description", "log", "language"
    ]

    data = data[output_columns]

    worksheet2.clear()
    worksheet2.update([data.columns.values.tolist()] + data.values.tolist())

    csv_path = f"cleaned_data_{timestamp}.csv"
    data.to_csv(csv_path, index=False)

    file_metadata = {'name': f"cleaned_data_{timestamp}.csv", 'parents': [folder_id]}
    media = MediaFileUpload(csv_path, mimetype='text/csv')
    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return f"File uploaded to Google Drive with ID: {file.get('id')}"

@app.route('/process', methods=['POST'])
def process_route():
    try:
        result = process_data()
        return jsonify({'message': result}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
