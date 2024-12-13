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
import os

# Initialize Flask app
app = Flask(__name__)

# Configure seed for consistent results with langdetect
DetectorFactory.seed = 0

# Configure authentication with Google Sheets and Drive
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Load credentials from environment variables
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

# Function to process data
def process_data():
    # Configure timezone
    local_tz = pytz.timezone('America/Lima')
    timestamp = datetime.now(local_tz).strftime("%Y%m%d%H%M%S")

    # Load data from the input Google Sheets
    sheet_input = client.open_by_url(url_data)
    worksheet1 = sheet_input.get_worksheet(0)

    # Load the output sheet
    sheet_output = client.open_by_url(url_data_clean)
    worksheet2 = sheet_output.get_worksheet(0)

    # Read the header row and data
    headers = worksheet1.row_values(1)
    rows = worksheet1.get_all_values()[1:]

    # Create a DataFrame
    data = pd.DataFrame(rows, columns=headers)

    # Ensure email columns exist
    for email_col in ["Mail From Dropcontact", "Email", "Professional Email"]:
        if email_col not in data.columns:
            data[email_col] = None

    # Add 'Nro' column with continuous numbering
    data.insert(0, 'Nro', range(1, len(data) + 1))

    # Add 'log' column with unique identifiers
    data['log'] = data['Nro'].apply(lambda x: f"{timestamp}-{x}")

    # Function to validate email format
    def is_valid_email(email):
        if email and isinstance(email, str):
            regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            return re.match(regex, email) is not None
        return False

    # Function to get the best email based on priority
    def get_valid_email(row):
        email_columns = ["Mail From Dropcontact", "Email", "Professional Email"]
        for col in email_columns:
            if col in row and is_valid_email(row[col]):
                return row[col]
        return "invalid@loriginal.org"

    # Apply email validation
    data["Valid Email"] = data.apply(get_valid_email, axis=1)

    # Function to clean phone numbers
    def clean_phone(phone):
        if pd.isna(phone) or phone.strip() == "":
            return ""
        # Remove all non-digit characters except "+"
        cleaned = re.sub(r'[^\d+]', '', phone)
        # Ensure the phone number is at least 8 digits
        if len(cleaned) >= 8:
            return cleaned
        return ""

    # Apply phone number cleaning
    data["Phone Number From Drop Contact"] = data["Phone Number From Drop Contact"].apply(clean_phone)

    # Detect language from the description
    def detect_language(description):
        if description:
            try:
                return detect(description)
            except LangDetectException:
                return "en"
        return "en"

    data['language'] = data['Description'].apply(detect_language)

    # Define output columns
    output_columns = [
        "Nro", "FirstName", "Last Name", "Full Name", "Profile Url",
        "Mail From Dropcontact", "Email", "Professional Email", "Valid Email",
        "Location", "Company", "Job Title", "Description", "Phone Number From Drop Contact", "log", "language"
    ]

    # Reorder and filter columns for the output
    data = data[output_columns]

    # Clear previous data and update the output Google Sheets
    worksheet2.clear()
    worksheet2.update([data.columns.values.tolist()] + data.values.tolist())

    # Save data as a CSV file with the timestamp
    csv_path = f"cleaned_data_{timestamp}.csv"
    data.to_csv(csv_path, index=False)

    # Upload the CSV file to Google Drive
    file_metadata = {'name': f"cleaned_data_{timestamp}.csv", 'parents': [folder_id]}
    media = MediaFileUpload(csv_path, mimetype='text/csv')
    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return f"File uploaded to Google Drive with ID: {file.get('id')}"

# Flask route to trigger the script with a POST request
@app.route('/process', methods=['POST'])
def process_route():
    try:
        result = process_data()
        return jsonify({'message': result}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Start the Flask app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
