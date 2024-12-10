import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Configurer la graine pour des résultats cohérents avec langdetect
DetectorFactory.seed = 0

# Configurer l'authentification avec Google Sheets et Drive
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Charger les informations d'identification depuis le fichier JSON
creds = ServiceAccountCredentials.from_json_keyfile_name('awesome-height-441419-j4-5f2808cabaf5.json', scope)
client = gspread.authorize(creds)
drive_service = build('drive', 'v3', credentials=creds)

# Charger les données depuis la feuille Google Sheets d'entrée
sheet_input = client.open_by_url("https://docs.google.com/spreadsheets/d/1eZs3-64SL92NrcmViDehMDTn8fIJTdCZqvsshQT1nek/edit?usp=sharing")
worksheet1 = sheet_input.get_worksheet(0)

# Charger la feuille de sortie
sheet_output = client.open_by_url("https://docs.google.com/spreadsheets/d/1xRISBywX7X-tK3HSWeDlup-_VcFXc0dGwyarXvfMwCM/edit?usp=sharing")
worksheet2 = sheet_output.get_worksheet(0)

# Obtenir les données existantes dans la feuille de sortie
existing_data = worksheet2.get_all_records()

# Déterminer le prochain numéro 'Nro'
if existing_data:
    last_id = max(row['Nro'] for row in existing_data if 'Nro' in row and str(row['Nro']).isdigit())
else:
    last_id = 0  # Si la feuille est vide, commencer à 0

# Lire la ligne d'en-tête de la feuille Google Sheets d'entrée
headers = worksheet1.row_values(1)  # Lire la première ligne (en-têtes)

# Fonction pour rendre les en-têtes uniques s'ils sont dupliqués
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

# Rendre les en-têtes uniques
unique_headers = make_headers_unique(headers)

# Lire les données de la feuille en excluant la ligne d'en-tête
rows = worksheet1.get_all_values()[1:]  # Exclure la première ligne (en-tête)

# Créer un DataFrame avec des en-têtes uniques
data = pd.DataFrame(rows, columns=unique_headers)

# Sélectionner uniquement les colonnes nécessaires pour le traitement
required_columns = [
    "FirstName", "Last Name", "Full Name", "Profile Url", "Headline", "Email",
    "Location", "Company", "Job Title", "Description", "Phone Number From Drop Contact"
]
filtered_columns = [col for col in required_columns if col in data.columns]
data = data[filtered_columns]

# Filtrer les lignes qui ont un prénom et un nom non vides
data = data[(data['FirstName'].notna()) & (data['FirstName'] != "") &
            (data['Last Name'].notna()) & (data['Last Name'] != "")]

# Ajouter la colonne 'Nro' au début du DataFrame avec une numérotation continue
data.insert(0, 'Nro', range(last_id + 1, last_id + 1 + len(data)))

# Ajouter la colonne 'log' avec un identifiant unique
timestamp = datetime.now().strftime("%Y%m%d%H%M")
data['log'] = data['Nro'].apply(lambda x: f"{timestamp}-{x}")

# Nettoyage et validation comme dans les étapes précédentes
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

# Effacer les données précédentes et mettre à jour la feuille Google Sheets
worksheet2.clear()
worksheet2.update([data.columns.values.tolist()] + data.values.tolist())

# Utiliser le timestamp comme nom du fichier
csv_path = f"cleaned_data_{timestamp}.csv"
data.to_csv(csv_path, index=False)

# ID de la destination Google Drive
folder_id = "1M7Ou_EZwp5ltj501ClkAYoHXEI6Fvlof"

# Télécharger le fichier CSV vers Google Drive
file_metadata = {
    'name': f"{timestamp}.csv",
    'parents': [folder_id]
}
media = MediaFileUpload(csv_path, mimetype='text/csv')
file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

print(f"Fichier exporté en CSV et uploadé à Google Drive avec ID : {file.get('id')}")
