from flask import Flask, request, jsonify
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
from datetime import datetime
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

# Load credentials from the JSON file
creds = ServiceAccountCredentials.from_json_keyfile_name(creds_dict, scope)
client = gspread.authorize(creds)
drive_service = build('drive', 'v3', credentials=creds)

# Your original script logic encapsulated in a function
def process_data():
    # Load data from the input Google Sheets
    sheet_input = client.open_by_url("https://docs.google.com/spreadsheets/d/1eZs3-64SL92NrcmViDehMDTn8fIJTdCZqvsshQT1nek/edit?usp=sharing")
    worksheet1 = sheet_input.get_worksheet(0)

    # Load the output sheet
    sheet_output = client.open_by_url("https://docs.google.com/spreadsheets/d/1xRISBywX7X-tK3HSWeDlup-_VcFXc0dGwyarXvfMwCM/edit?usp=sharing")
    worksheet2 = sheet_output.get_worksheet(0)

    # Get existing data from the output sheet
    existing_data = worksheet2.get_all_records()

    # Determine the next 'Nro' number
    if existing_data:
        last_id = max(row['Nro'] for row in existing_data if 'Nro' in row and str(row['Nro']).isdigit())
    else:
        last_id = 0  # If the sheet is empty, start at 0

    # Read the header row from the input Google Sheets
    headers = worksheet1.row_values(1)

    # Function to make headers unique if they are duplicated
    def make_headers_unique(headers):
        seen = {}
        unique_headers = []
        for header in headers:
            if header in seen:
                seen[header] += 1
                unique_headers.append(f"{header}_{seen[header]}")
            else:
                seen[header] = 0
                unique_headers.append(header)
        return unique_headers

    # Make headers unique
    unique_headers = make_headers_unique(headers)

    # Read the data from the sheet excluding the header row
    rows = worksheet1.get_all_values()[1:]

    # Create a DataFrame with unique headers
    data = pd.DataFrame(rows, columns=unique_headers)

    # Select only the necessary columns for processing
    required_columns = [
        "FirstName", "Last Name", "Full Name", "Profile Url", "Headline", "Email",
        "Location", "Company", "Job Title", "Description", "Phone Number From Drop Contact"
    ]
    filtered_columns = [col for col in required_columns if col in data.columns]
    data = data[filtered_columns]

    # Filter rows with non-empty first and last names
    data = data[(data['FirstName'].notna()) & (data['FirstName'] != "") &
                (data['Last Name'].notna()) & (data['Last Name'] != "")]

    # Add 'Nro' column at the beginning with continuous numbering
    data.insert(0, 'Nro', range(last_id + 1, last_id + 1 + len(data)))

    # Add 'log' column with a unique identifier
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    data['log'] = data['Nro'].apply(lambda x: f"{timestamp}-{x}")

    # Cleaning and validation as in previous steps
    replacement_dict = {
        "Ã¡": "á", "Ã©": "é", "Ã­": "í", "Ã³": "ó", "Ãº": "ú",
        "Ã±": "ñ", "Ã": "Ñ", "â": "'", "â": "-", "Ã¼": "ü",
        "â€œ": "\"", "â€": "\"", "â€˜": "'", "â€¢": "-", "â‚¬": "€",
        "â„¢": "™", "âˆ’": "-", "Â": ""
    }

    def clean_text(text):
        if pd.isna(text):
            return ""
        for bad, good in replacement_dict.items():
            text = text.replace(bad, good)
        return re.sub(r'[^\w\s@.-]', '', text).strip()

    for column in filtered_columns:
        if column not in {"Email", "Profile Url", "Phone Number From Drop Contact"}:
            data[column] = data[column].apply(clean_text)

    def validate_email(email):
        if re.match(r"[^@]+@[^@]+\.[^@]+", email):
            return email
        return "invalid"

    data["Email"] = data["Email"].apply(validate_email)

    def clean_phone(phone):
        if pd.isna(phone) or phone.strip() == "":
            return "invalid"
        cleaned = re.sub(r'[^\d+]', '', phone)
        if len(cleaned) >= 8:
            return cleaned
        return "invalid"

    data["Phone Number From Drop Contact"] = data["Phone Number From Drop Contact"].apply(clean_phone)

    def detect_language(description):
        if description:
            try:
                return detect(description)
            except LangDetectException:
                return "en"
        return "en"

    data['language'] = data['Description'].apply(detect_language)

    # Clear previous data and update the output Google Sheets
    worksheet2.clear()
    worksheet2.update([data.columns.values.tolist()] + data.values.tolist())

    # Save data as a CSV file with the timestamp
    csv_path = f"cleaned_data_{timestamp}.csv"
    data.to_csv(csv_path, index=False)

    # Upload the CSV file to Google Drive
    folder_id = "1M7Ou_EZwp5ltj501ClkAYoHXEI6Fvlof"
    file_metadata = {'name': f"{timestamp}.csv", 'parents': [folder_id]}
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
    app.run(debug=True, port=5000)
