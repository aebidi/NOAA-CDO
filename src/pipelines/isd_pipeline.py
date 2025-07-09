# src/pipelines/isd_pipeline.py
import pandas as pd
import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from src.config import DATA_DIR, RAW_DIR, PROCESSED_DIR, DATASET_CONFIGS, \
                       START_YEAR, END_YEAR, LOG_FILE

# get ISD specific settings from the config
CONFIG = DATASET_CONFIGS['isd_lite']
GSOD_CONFIG = DATASET_CONFIGS['gsod'] # For FIPS map
# reusing the station list from GSOD
STATION_LIST_PATH = DATA_DIR / "gsod_regional_stations.csv"
RAW_DATA_PATH = RAW_DIR / "isd_lite"
PROCESSED_DATA_PATH = PROCESSED_DIR / "isd_lite"

def _log_error(message):
    """Appends an error message to the central log file."""
    with open(LOG_FILE, "a") as f:
        f.write(f"{time.ctime()}: {message}\n")

def _download_worker(year, filename_id):
    """Worker to download a single ISD CSV file."""
    url = f"{CONFIG['data_url_base']}{year}/{filename_id}.csv"
    year_dir = RAW_DATA_PATH / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    filepath = year_dir / f"{filename_id}.csv"

    if filepath.exists():
        return None
    
    try:
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            filepath.write_bytes(response.content)
            return f"Success: {filename_id} for {year}"
        elif response.status_code != 404:
            _log_error(f"ISD Download: Failed for {filename_id}/{year} (Status: {response.status_code})")
        return None
    except requests.exceptions.RequestException as e:
        _log_error(f"ISD Download: Error for {filename_id}/{year}. Details: {e}")
        return None

def _process_worker(filepath):
    """Worker function to process a single raw ISD hourly CSV file."""
    try:
        df = pd.read_csv(filepath, low_memory=False)
        if df.empty:
            return

        df_processed = pd.DataFrame()
        df_processed['date'] = pd.to_datetime(df['DATE'])
        
        # the data format for Temperature is a string like "+0011,1" (value, scale factor)
        # we need to parse this string to get the actual value
        if 'TMP' in df.columns:
            # split the string by the comma, take the first part (the value)
            temp_val = df['TMP'].str.split(',', expand=True)[0].astype(float)
            # according to the docs, this value is in tenths of degrees C
            df_processed['temp_c'] = temp_val / 10.0

        if 'WND' in df.columns:
            # wind format is "direction,direction_quality,type,speed,speed_quality"
            # e.g., "160,1,N,0021,1". We need the 4th value (speed).
            wind_speed_val = df['WND'].str.split(',', expand=True)[3].astype(float)
            # the value is in tenths of meters per second
            df_processed['wind_speed_ms'] = wind_speed_val / 10.0
            
        # dew Point has the same format as temperature
        if 'DEW' in df.columns:
            dew_val = df['DEW'].str.split(',', expand=True)[0].astype(float)
            df_processed['dew_point_c'] = dew_val / 10.0

        # get metadata for saving
        filename_id = filepath.stem
        station_meta = pd.read_csv(STATION_LIST_PATH, dtype={'FILENAME_ID': str})
        station_meta_row = station_meta.loc[station_meta['FILENAME_ID'] == filename_id]
        
        if station_meta_row.empty:
            _log_error(f"ISD Process: Could not find metadata for station {filename_id}")
            return
            
        output_station_id = station_meta_row['STATION_ID'].iloc[0]
        fips_country = station_meta_row['CTRY'].iloc[0]
        fips_map_inv = {v: k for k, v in GSOD_CONFIG['fips_country_map'].items()}
        country_code = fips_map_inv.get(fips_country, "unknown")

        year = df_processed['date'].iloc[0].year
        output_dir = PROCESSED_DATA_PATH / country_code
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{output_station_id}_{year}.csv"
        df_processed.to_csv(output_path, index=False, date_format='%Y-%m-%d %H:%M:%S')

    except Exception as e:
        _log_error(f"ISD Process: Failed for {filepath.name}. Error: {e}")

def run_step(step):
    """Main controller for the ISD pipeline."""
    if step == 'download':
        if not STATION_LIST_PATH.exists():
            print("GSOD station list not found. Please run 'gsod' download step first to generate it.")
            return
            
        stations = pd.read_csv(STATION_LIST_PATH)
        RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
        tasks = [(year, fid) for year in range(START_YEAR, END_YEAR + 1) for fid in stations['FILENAME_ID'].dropna().unique()]

        print(f"--- ISD: Attempting to download hourly CSV data for {len(stations['FILENAME_ID'].dropna().unique())} stations ---")
        
        success_count = 0
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(_download_worker, year, fid) for year, fid in tasks]
            for future in tqdm(as_completed(futures), total=len(tasks), desc="Downloading ISD files"):
                if future.result():
                    success_count += 1
        print(f"Download complete. Successfully retrieved {success_count} new files.")

    elif step == 'process':
        files_to_process = list(RAW_DATA_PATH.glob("**/*.csv"))
        if not files_to_process:
            print("No raw ISD data found to process. Please run the 'download' step first.")
            return

        print(f"--- ISD: Processing {len(files_to_process)} raw hourly CSV data files ---")
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            list(tqdm(executor.map(_process_worker, files_to_process), total=len(files_to_process)))
        print("ISD processing complete.")

    else:
        print(f"Unknown step: {step}. Available steps are 'download', 'process'.")