# src/pipelines/gsod_pipeline.py
import pandas as pd
import requests
import time
import os
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from src.config import DATA_DIR, RAW_DIR, PROCESSED_DIR, DATASET_CONFIGS, \
                       TARGET_COUNTRIES, START_YEAR, END_YEAR, STANDARD_COLUMNS, LOG_FILE

# get GSOD specific settings from the config
CONFIG = DATASET_CONFIGS['gsod']
STATION_LIST_PATH = DATA_DIR / "gsod_regional_stations.csv"
RAW_DATA_PATH = RAW_DIR / "gsod"
PROCESSED_DATA_PATH = PROCESSED_DIR / "gsod"

def _log_error(message):
    """Appends an error message to the central log file."""
    with open(LOG_FILE, "a") as f:
        f.write(f"{time.ctime()}: {message}\n")

def _find_stations():
    """Downloads the ISD station history and filters it for the GSOD dataset."""
    print("--- GSOD: Finding stations in target region ---")
    FIPS_MAP = CONFIG['fips_country_map']
    TARGET_FIPS_CODES = [FIPS_MAP[code] for code in TARGET_COUNTRIES if code in FIPS_MAP]

    try:
        response = requests.get(CONFIG['station_inventory_url'])
        response.raise_for_status()
        
        df = pd.read_csv(StringIO(response.text), dtype={'USAF': str, 'WBAN': str})
        
        regional_df = df[df['CTRY'].isin(TARGET_FIPS_CODES)].copy()
        regional_df['END'] = pd.to_numeric(regional_df['END'].astype(str).str[:4])
        active_df = regional_df[regional_df['END'] >= START_YEAR].copy()
        
        active_df.dropna(subset=['USAF', 'WBAN'], inplace=True)
        active_df['STATION_ID'] = active_df['USAF'] + '-' + active_df['WBAN']
        active_df['FILENAME_ID'] = active_df['USAF'] + active_df['WBAN']

        active_df.to_csv(STATION_LIST_PATH, index=False)
        print(f"Found {len(active_df)} potentially active stations. List saved to {STATION_LIST_PATH}")
        return active_df
    except Exception as e:
        _log_error(f"GSOD: Failed to download or process station list. Error: {e}")
        print("Error: Could not retrieve station list. Check log file for details.")
        return pd.DataFrame()

def _download_worker(year, filename_id):
    """Worker to download a single GSOD file for a given station and year."""
    url = f"{CONFIG['data_url_base']}{year}/{filename_id}.csv"
    year_dir = RAW_DATA_PATH / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    filepath = year_dir / f"{filename_id}.csv"

    if filepath.exists():
        return None
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            filepath.write_bytes(response.content)
            return f"Success: {filename_id} for {year}"
        elif response.status_code != 404:
            _log_error(f"GSOD Download: Failed for {filename_id}/{year} (Status: {response.status_code})")
        return None
    except requests.exceptions.RequestException as e:
        _log_error(f"GSOD Download: Error for {filename_id}/{year}. Details: {e}")
        return None

def _process_worker(filepath):
    """Worker function to process a single raw GSOD CSV file."""
    try:
        df = pd.read_csv(filepath, na_values=[99.99, 999.9, 9999.9])
        if df.empty or 'DATE' not in df.columns:
            return
            
        df_processed = pd.DataFrame()
        df_processed['date'] = pd.to_datetime(df['DATE'])
        
        if 'MAX' in df.columns:
            df_processed[STANDARD_COLUMNS['TMAX']] = (df['MAX'] - 32) * 5 / 9
        if 'MIN' in df.columns:
            df_processed[STANDARD_COLUMNS['TMIN']] = (df['MIN'] - 32) * 5 / 9
        if 'PRCP' in df.columns:
            df_processed[STANDARD_COLUMNS['PRCP']] = df['PRCP'] * 25.4
        if 'WDSP' in df.columns:
            df_processed[STANDARD_COLUMNS['WDSP']] = df['WDSP'] * 0.514444

        # getting the hyphen-less ID from the raw file, ensure its a string
        station_id_from_file = str(df['STATION'].iloc[0])

        # loading the metadata, ensuring FILENAME_ID is read as a string
        station_meta = pd.read_csv(STATION_LIST_PATH, dtype={'FILENAME_ID': str})

        # performing the lookup using the correct, hyphen-less key: 'FILENAME_ID'
        station_meta_row = station_meta.loc[station_meta['FILENAME_ID'] == station_id_from_file]
        
        if station_meta_row.empty:
            _log_error(f"GSOD Process: Could not find metadata for station {station_id_from_file}")
            return

        # getting the hyphenated ID and country code for saving the file
        output_station_id = station_meta_row['STATION_ID'].iloc[0]
        fips_country = station_meta_row['CTRY'].iloc[0]
        
        fips_map_inv = {v: k for k, v in CONFIG['fips_country_map'].items()}
        country_code = fips_map_inv.get(fips_country, "unknown")

        year = df_processed['date'].iloc[0].year
        output_dir = PROCESSED_DATA_PATH / country_code
        output_dir.mkdir(parents=True, exist_ok=True)
        # using the standard hyphenated ID for the final filename
        output_path = output_dir / f"{output_station_id}_{year}.csv"
        df_processed.to_csv(output_path, index=False, date_format='%Y-%m-%d')

    except Exception as e:
        _log_error(f"GSOD Process: Failed for {filepath.name}. Error: {e}")

def run_step(step):
    """Main controller for the GSOD pipeline."""
    if step == 'download':
        stations = _find_stations()
        if stations.empty: return
        RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
        tasks = [(year, fid) for year in range(START_YEAR, END_YEAR + 1) for fid in stations['FILENAME_ID'].dropna().unique()]
        print(f"--- GSOD: Attempting to download data for {len(stations['FILENAME_ID'].dropna().unique())} stations over {END_YEAR - START_YEAR + 1} years ---")
        print("NOTE: Most attempts will fail with a 404 error, which is normal for this dataset.")
        success_count = 0
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(_download_worker, year, fid) for year, fid in tasks]
            for future in tqdm(as_completed(futures), total=len(tasks), desc="Downloading GSOD files"):
                if future.result(): success_count += 1
        print(f"Download complete. Successfully retrieved {success_count} new files.")

    elif step == 'process':
        files_to_process = list(RAW_DATA_PATH.glob("**/*.csv"))
        if not files_to_process:
            print("No raw GSOD data found to process. Please run the 'download' step first.")
            return
        print(f"--- GSOD: Processing {len(files_to_process)} raw data files ---")
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            list(tqdm(executor.map(_process_worker, files_to_process), total=len(files_to_process)))
        print("GSOD processing complete.")
    else:
        print(f"Unknown step: {step}. Available steps are 'download', 'process'.")