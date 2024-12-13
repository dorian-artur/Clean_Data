# Function to parse location using geopy
def parse_location(location):
    """Parse the Location field into components using geopy."""
    if pd.isna(location) or location.strip() == "":
        return {"City": "Unknown", "State": "Unknown", "Country": "Unknown", "Postal Code": "Unknown"}
    
    try:
        # Attempt to geocode the location
        geo_location = geolocator.geocode(location, timeout=10)
        if geo_location and geo_location.raw.get('address'):
            address = geo_location.raw['address']
            return {
                "City": address.get('city', address.get('town', address.get('village', "Unknown"))),
                "State": address.get('state', "Unknown"),
                "Country": address.get('country', "Unknown"),
                "Postal Code": address.get('postcode', "Unknown")
            }
    except GeocoderTimedOut:
        print(f"Geocoder timed out for location: {location}")
    except Exception as e:
        print(f"Error parsing location '{location}': {e}")
    
    # Fallback in case of an error
    return {"City": "Unknown", "State": "Unknown", "Country": "Unknown", "Postal Code": "Unknown"}

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

    # Ensure email and phone columns exist
    for col in ["Mail From Dropcontact", "Email", "Professional Email", "Phone", "Phone Number From Drop Contact"]:
        if col not in data.columns:
            data[col] = None

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
        cleaned = re.sub(r'[^\d+]', '', phone)
        if len(cleaned) >= 8:
            return cleaned
        return ""

    # Clean both phone columns
    data["Phone"] = data["Phone"].apply(clean_phone)
    data["Phone Number From Drop Contact"] = data["Phone Number From Drop Contact"].apply(clean_phone)

    # Combine phones into a single field
    def get_combined_phone(row):
        if row["Phone"]:
            return row["Phone"]
        if row["Phone Number From Drop Contact"]:
            return row["Phone Number From Drop Contact"]
        return ""

    data["Combined Phone"] = data.apply(get_combined_phone, axis=1)

    # Detect language from the description
    def detect_language(description):
        if description:
            try:
                return detect(description)
            except LangDetectException:
                return "en"
        return "en"

    data['language'] = data['Description'].apply(detect_language)

    # Parse location
    location_components = data["Location"].apply(parse_location)
    data["City"] = location_components.apply(lambda x: x["City"])
    data["State"] = location_components.apply(lambda x: x["State"])
    data["Country"] = location_components.apply(lambda x: x["Country"])
    data["Postal Code"] = location_components.apply(lambda x: x["Postal Code"])

    # Define output columns
    output_columns = [
        "Nro", "FirstName", "Last Name", "Full Name", "Profile Url",
        "Mail From Dropcontact", "Email", "Professional Email", "Valid Email",
        "Phone", "Phone Number From Drop Contact", "Combined Phone",
        "City", "State", "Country", "Postal Code", "Company", "Job Title",
        "Description", "log", "language"
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
