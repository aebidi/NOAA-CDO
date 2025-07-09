# src/pipelines/ghcnd_pipeline.py
import pandas as pd
import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from src.config import DATA_DIR, RAW_DIR, PROCESSED_DIR, DATASET_CONFIGS, \
                       TARGET_COUNTRIES, START_YEAR, END_YEAR, STANDARD_COLUMNS, LOG_FILE

# get GHCN-D specific settings from the config
CONFIG = DATASET_CONFIGS['ghcnd']
STATION_LIST_PATH = DATA_DIR / "ghcnd_regional_stations.csv"
RAW_DATA_PATH = RAW_DIR / "ghcnd"
PROCESSED_DATA_PATH = PROCESSED_DIR / "ghcnd"

def _log_error(message):
    """Appends an error message to the central log file."""
    with open(LOG_FILE, "a") as f:
        f.write(f"{time.ctime()}: {message}\n")

def _find_stations():
    """Downloads the master station list and filters it by target countries."""
    print("--- GHCN-D: Finding stations in target region ---")
    col_specs = [(0, 11), (12, 20), (21, 30), (31, 37), (38, 40), (41, 71)]
    col_names = ["ID", "LATITUDE", "LONGITUDE", "ELEVATION", "STATE", "NAME"]
    
    try:
        df = pd.read_fwf(CONFIG['station_inventory_url'], colspecs=col_specs, names=col_names, header=None)
        df['COUNTRY_CODE'] = df['ID'].str[:2]
        regional_df = df[df['COUNTRY_CODE'].isin(TARGET_COUNTRIES)].copy()
        
        regional_df.to_csv(STATION_LIST_PATH, index=False)
        print(f"Found {len(regional_df)} stations. List saved to {STATION_LIST_PATH}")
        return regional_df
    except Exception as e:
        _log_error(f"GHCN-D: Failed to download or process station list. Error: {e}")
        print("Error: Could not retrieve station list. Check log file for details.")
        return pd.DataFrame()

def _download_worker(station_id):
    """Worker function to download data for a single station."""
    url = f"{CONFIG['data_url_base']}{station_id}.dly"
    filepath = RAW_DATA_PATH / f"{station_id}.dly"
    if filepath.exists():
        return f"Exists: {station_id}"
    
    try:
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            filepath.write_bytes(response.content)
            return f"Success: {station_id}"
        else:
            _log_error(f"GHCN-D Download: Failed for {station_id} (Status: {response.status_code})")
            return f"Failed: {station_id}"
    except requests.exceptions.RequestException as e:
        _log_error(f"GHCN-D Download: Error for {station_id}. Details: {e}")
        return f"Error: {station_id}"

def _process_worker(filepath):
    """Worker function to process a single .dly file."""
    try:
        station_id = filepath.stem
        col_specs = [(0, 11), (11, 15), (15, 17), (17, 21)]
        names = ['ID', 'YEAR', 'MONTH', 'ELEMENT']
        for day in range(1, 32):
            start = 21 + (day - 1) * 8
            col_specs.append((start, start + 5))
            names.append(f'VALUE{day}')

        df = pd.read_fwf(filepath, colspecs=col_specs, names=names, na_values=['-9999'], header=None)
        df_filtered = df[df['ELEMENT'].isin(CONFIG['required_elements'])]
        
        id_vars = ['ID', 'YEAR', 'MONTH', 'ELEMENT']
        value_vars = [f'VALUE{day}' for day in range(1, 32)]
        df_long = df_filtered.melt(id_vars, value_vars, 'DAY_FIELD', 'VALUE').dropna()
        
        df_long['DAY'] = df_long['DAY_FIELD'].str.replace('VALUE', '').astype(int)
        df_long['DATE'] = pd.to_datetime(df_long[['YEAR', 'MONTH', 'DAY']], errors='coerce')
        df_long.dropna(subset=['DATE'], inplace=True)
        
        df_pivot = df_long.pivot_table('VALUE', ['DATE'], 'ELEMENT').reset_index()
        df_pivot = df_pivot[(df_pivot['DATE'].dt.year >= START_YEAR) & (df_pivot['DATE'].dt.year <= END_YEAR)]

        # unit conversion and column standardisation
        df_pivot.rename(columns={'DATE': 'date'}, inplace=True)
        for element, new_name in STANDARD_COLUMNS.items():
            if element in df_pivot.columns:
                df_pivot[new_name] = df_pivot[element] / 10.0 # All GHCN-D values are in tenths
                df_pivot.drop(columns=[element], inplace=True)
        
        station_meta = pd.read_csv(STATION_LIST_PATH)
        country_code = station_meta.loc[station_meta['ID'] == station_id, 'COUNTRY_CODE'].iloc[0]
        
        # saving one file per year
        for year, data_for_year in df_pivot.groupby(df_pivot['date'].dt.year):
            output_dir = PROCESSED_DATA_PATH / country_code
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"{station_id}_{year}.csv"
            data_for_year.to_csv(output_path, index=False, date_format='%Y-%m-%d')
        
        return f"Processed: {station_id}"
    except Exception as e:
        _log_error(f"GHCN-D Process: Failed for {filepath.name}. Error: {e}")
        return f"Error processing {filepath.name}"

def run_step(step):
    """Main controller for the GHCN-D pipeline."""
    if step == 'download':
        stations = _find_stations()
        if stations.empty:
            return
        
        RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
        tasks = stations['ID'].tolist()
        print(f"--- GHCN-D: Downloading data for {len(tasks)} stations ---")
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(_download_worker, task) for task in tasks]
            for future in tqdm(as_completed(futures), total=len(tasks)):
                future.result()

    elif step == 'process':
        if not STATION_LIST_PATH.exists():
            print("Station list not found. Please run the 'download' step first.")
            return

        files_to_process = list(RAW_DATA_PATH.glob("*.dly"))
        if not files_to_process:
            print("No raw data found to process. Please run the 'download' step first.")
            return

        print(f"--- GHCN-D: Processing {len(files_to_process)} raw data files ---")
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [executor.submit(_process_worker, f) for f in files_to_process]
            for future in tqdm(as_completed(futures), total=len(files_to_process)):
                future.result()

    else:
        print(f"Unknown step: {step}. Available steps are 'download', 'process'.")