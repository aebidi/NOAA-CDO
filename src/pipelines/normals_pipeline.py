# src/pipelines/normals_pipeline.py
import pandas as pd
import requests
import time
import os
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from src.config import DATA_DIR, RAW_DIR, PROCESSED_DIR, DATASET_CONFIGS, LOG_FILE

CONFIG = DATASET_CONFIGS['normals_daily']
GHCND_STATION_LIST_PATH = DATA_DIR / "ghcnd_regional_stations.csv"
RAW_DATA_PATH = RAW_DIR / "normals_daily"
PROCESSED_DATA_PATH = PROCESSED_DIR / "normals_daily"

def _log_error(message):
    with open(LOG_FILE, "a") as f:
        f.write(f"{time.ctime()}: {message}\n")

def _download_worker(station_id):
    """Worker to download a Normals CSV, returning a status code."""
    url = f"{CONFIG['data_url_base']}{station_id}.csv"
    filepath = RAW_DATA_PATH / f"{station_id}.csv"
    if filepath.exists():
        return "exists"
    
    try:
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            filepath.write_bytes(response.content)
            return "success"
        else:
            return response.status_code # returns the specific error code
    except requests.exceptions.RequestException:
        return "error"

def _process_worker(filepath, station_meta_map):
    station_id = filepath.stem
    try:
        df = pd.read_csv(filepath)
        df_filtered = df[df['element'].isin(CONFIG['required_elements'])].copy()
        if df_filtered.empty: return

        df_filtered['MONTH_DAY'] = df_filtered['date'].str.strip()
        df_pivot = df_filtered.pivot_table(index='MONTH_DAY', columns='element', values='value').reset_index()

        if 'dly-tmax-normal' in df_pivot.columns:
            df_pivot['tmax_c_normal'] = ((df_pivot['dly-tmax-normal'] / 10.0) - 32) * 5 / 9
        if 'dly-tmin-normal' in df_pivot.columns:
            df_pivot['tmin_c_normal'] = ((df_pivot['dly-tmin-normal'] / 10.0) - 32) * 5 / 9
        if 'dly-prcp-normal' in df_pivot.columns:
            df_pivot['prcp_mm_normal'] = (df_pivot['dly-prcp-normal'] / 100.0) * 25.4

        final_cols = ['MONTH_DAY']
        if 'tmax_c_normal' in df_pivot.columns: final_cols.append('tmax_c_normal')
        if 'tmin_c_normal' in df_pivot.columns: final_cols.append('tmin_c_normal')
        if 'prcp_mm_normal' in df_pivot.columns: final_cols.append('prcp_mm_normal')
        df_final = df_pivot[final_cols]

        country_code = station_meta_map[station_id]['COUNTRY_CODE']
        output_dir = PROCESSED_DATA_PATH / country_code
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{station_id}_normals_1991-2020.csv"
        df_final.to_csv(output_path, index=False)
    except Exception as e:
        _log_error(f"Normals Process: Failed for {filepath.name}. Error: {e}")

def run_step(step):
    """Main controller for the Normals pipeline."""
    if step == 'download':
        if not GHCND_STATION_LIST_PATH.exists():
            print("GHCN-D station list not found. Please run 'ghcnd --step download' first.")
            return

        stations = pd.read_csv(GHCND_STATION_LIST_PATH)
        tasks = stations['ID'].tolist()
        RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)

        print(f"--- Normals: Checking availability for {len(tasks)} regional stations ---")
        
        status_counts = Counter()
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(_download_worker, task) for task in tasks]
            for future in tqdm(as_completed(futures), total=len(tasks)):
                result = future.result()
                status_counts[result] += 1
        
        print("\n--- Download Summary ---")
        print(f"Successfully downloaded: {status_counts['success']}")
        print(f"Already existed: {status_counts['exists']}")
        print(f"Not Found (404): {status_counts[404]}")
        print(f"Other network/server errors: {status_counts['error']}")
        print("------------------------")
        
        if status_counts[404] == len(tasks):
             print("\nCONCLUSION: It is confirmed that no stations in your target region have a corresponding 1991-2020 Daily Normals product on the server.")

    elif step == 'process':
        if not GHCND_STATION_LIST_PATH.exists(): return
        files_to_process = list(RAW_DATA_PATH.glob("*.csv"))
        if not files_to_process:
            print("No raw normals data found to process. Please run 'normals --step download' first.")
            return
        station_meta_map = pd.read_csv(GHCND_STATION_LIST_PATH).set_index('ID').to_dict('index')
        print(f"--- Normals: Processing {len(files_to_process)} raw data files ---")
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            list(tqdm(executor.map(_process_worker, files_to_process, [station_meta_map]*len(files_to_process)), total=len(files_to_process)))
        print("Normals processing complete.")