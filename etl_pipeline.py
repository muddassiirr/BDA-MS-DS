import pandas as pd
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pymongo import MongoClient
from datetime import datetime
from oauth2client.file import Storage
from oauth2client.client import flow_from_clientsecrets
from oauth2client.tools import run_flow
from pymongo.server_api import ServerApi


# === 1. Extract Functions ===

def extract_csv():
    return pd.read_csv("data/sample_data.csv")


def extract_json():
    with open("data/sample_weather.json", 'r') as f:
        data = json.load(f)
    return pd.json_normalize(data)


def extract_google_sheet():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    creds_file = 'config/client_secret.json'
    token_file = 'config/token.json'
    storage = Storage(token_file)

    creds = storage.get()
    if not creds or creds.invalid:
        flow = flow_from_clientsecrets(creds_file, scope)
        creds = run_flow(flow, storage)

    client = gspread.authorize(creds)    
    sheet_url = "https://docs.google.com/spreadsheets/d/1U4SvCz-F5Qu2iKU7ty0bN2TBlbn1Vyxgc36ZjUyqIEo/edit?gid=0#gid=0"
    sheet = client.open_by_url(sheet_url).sheet1    
    data = sheet.get_all_records()
    print(data)
    return pd.DataFrame(data)


def extract_weather_api():
    url = "https://api.openweathermap.org/data/2.5/weather?q=London&appid=4af61d0c0ece8febfeb3e64f467678df"
    res = requests.get(url)
    print(res)
    return pd.json_normalize(res.json())


def extract_from_mongo():
    with open('config/db_config.json') as f:
        config = json.load(f)    
    client = MongoClient(config['mongodb_uri'], server_api=ServerApi('1'))
    db = client[config["database"]]
    collection = db[config["collection"]]
    data = list(collection.find())
    print(f"✅ Extracted {len(data)} records from MongoDB.")
    return pd.DataFrame(data)


# === 2. Transform Function ===
def transform_data(df):
    import numpy as np        
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(str)        
    df.drop_duplicates(inplace=True)    
    df.ffill(inplace=True)            
    if 'temp_f' in df.columns and df['temp_f'].notna().any():
        df['temp_c'] = (pd.to_numeric(df['temp_f'], errors='coerce') - 32) * 5 / 9
    elif 'temperature_f' in df.columns and df['temperature_f'].notna().any():
        df['temp_c'] = (pd.to_numeric(df['temperature_f'], errors='coerce') - 32) * 5 / 9

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')  # ISO 8601 format

    def get_numeric_col(*cols):
        for col in cols:
            if col in df.columns:
                return pd.to_numeric(df[col], errors='coerce')
        return pd.Series(np.nan, index=df.index)

    temp_c = get_numeric_col('temp_c')
    humidity = get_numeric_col('humidity', 'main.humidity')
    wind_speed = get_numeric_col('wind_speed', 'wind.speed')

    df['weather_score'] = (temp_c + humidity + wind_speed) / 3    
    df['weather_score'] = df['weather_score'].round(2)    
    df.drop(columns=['_id'], errors='ignore', inplace=True)

    return df





# === 3. Load to MongoDB ===

def load_to_mongo(df):
    with open('config/db_config.json') as f:
        config = json.load(f)
    client = MongoClient(config['mongodb_uri'])
    db = client[config['database']]
    collection = db[config['collection']]
    collection.insert_many(df.to_dict(orient='records'))


# === 4. Main ===

def main():
    print("Starting ETL job...")
    # Extract
    df_csv = extract_csv()
    df_json = extract_json()
    df_sheet = extract_google_sheet()
    df_api = extract_weather_api()
    df_mongo = extract_from_mongo()
    # Merge all
    df_all = pd.concat([df_csv, df_json, df_sheet, df_api, df_mongo], ignore_index=True, sort=False)
    # Transform
    df_clean = transform_data(df_all)
    # Load
    load_to_mongo(df_clean)
    # Export
    df_clean.to_csv("output/final_cleaned_data.csv", index=False)
    print("ETL job completed successfully.")

if __name__ == "__main__":
    main()
