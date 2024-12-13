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
from geopy.exc import GeocoderTimedOut
import os

# Initialize Flask app
app = Flask(__name__)

# Configure seed for consistent results with langdetect
DetectorFactory.seed = 0

# Initialize the geolocator
geolocator = Nominatim(user_agent="location_parser")

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

# Function to parse location
def parse_location(location):
    if pd.isna(location) or location.strip() == "":
        return {"City": "Empty", "State": "Empty", "Country": "Empty", "Postal Code": "Empty"}
    try:
        geo_location = geolocator.geocode(location, timeout=10)
        if geo_location and geo_location.raw.get('address'):
            address = geo_location.raw['address']
            return {
                "City": address.get('city', address.get('town', address.get('village', "Empty"))),
                "State": address.get('state', "Empty"),
                "Country": address.get('country', "Empty"),
                "Postal Code": address.get('postcode', "Empty")
            }
    except GeocoderTimedOut:
        print(f"Geocoder timed out for location: {location}")
    except Exception as e:
        print(f"Error parsing location '{location}': {e}")
    return {"City": "Empty", "State": "Empty", "Country": "Empty", "Postal Code": "Empty"}

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

    location_components = data["Location"].apply(parse_location)
    data["City"] = location_components.apply(lambda x: x["City"])
    data["State"] = location_components.apply(lambda x: x["State"])
    data["Country"] = location_components.apply(lambda x: x["Country"])
    data["Postal Code"] = location_components.apply(lambda x: x["Postal Code"])

    output_columns = [
        "Nro", "FirstName", "Last Name", "Full Name", "Profile Url",
        "Mail From Dropcontact", "Email", "Professional Email", "Valid Email",
        "Phone", "Phone Number From Drop Contact", "Combined Phone",
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
