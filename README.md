#About
  ![Descripción de la Imagen](https://github.com/Jorgejfp/Clean_Data/blob/main/assets/Clean_Data.png?raw=true) 
# Data Cleaning and Language Detection Tool

This repository contains a robust **data cleaning and language detection tool** designed to process data from Google Sheets, clean textual inconsistencies, and identify the language of textual content. The tool leverages **Python** libraries like `pandas`, `gspread`, `langdetect`, `pycountry`, and `langcodes` for comprehensive data enrichment.

## Features

### Google Sheets Integration
- Reads raw data from a source sheet.
- Updates processed data to a destination sheet with one-click automation.

### Data Cleaning
- Fixes encoding issues (e.g., "Ã¡" → "á") using a customizable replacement dictionary.
- Removes unwanted characters and ensures clean text formatting.

### Language Detection
- Detects text language using `langdetect`.
- Infers default language based on location data when textual detection fails.
- Adds a new column `language` with the detected or inferred language.

### Error Handling
- Ensures robust execution even when input data contains anomalies.

## How It Works

1. **Authentication**: Connects securely to Google Sheets using OAuth2 credentials.
2. **Data Processing**:
   - Reads specified columns from the source sheet.
   - Cleans text fields using predefined rules.
   - Detects or infers the language for each entry.
3. **Output**: Writes the processed data back to the destination sheet with added insights.

## Requirements

To run this tool, ensure you have the following:

- Python 3.8+
- Required Python libraries:
  - `pandas`
  - `gspread`
  - `langdetect`
  - `pycountry`
  - `langcodes`
  - `oauth2client`
- Google API credentials JSON file (e.g., `awesome-height-441419-j4-5f2808cabaf5.json`).
- Google Sheets with proper permissions.

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/data-cleaning-language-detection.git
   cd data-cleaning-language-detection

2. Install dependencies:
    ````bash
    pip install -r requirements.txt
    Add your Google API credentials JSON file to the project root.

Update the sheet URLs in the script to match your Google Sheets.

Run the script:
    ```bash
    python main.py
    
## Output Example

Here’s a snapshot of the processed data:

| Description | Headline | Location | Language |
|-------------|----------|----------|----------|
| Example 1   | Headline | USA      | en       |
| Example 2   | Titre    | France   | fr       |

