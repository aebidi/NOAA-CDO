# src/config.py
from pathlib import Path

# temporal scope
START_YEAR = 1981
END_YEAR = 2025 

# geographic scope
PRIMARY_COUNTRY = "ZA"
SECONDARY_COUNTRIES = ["MI", "MZ", "ZI", "AO", "CG", "TZ", "WA"]
TARGET_COUNTRIES = [PRIMARY_COUNTRY] + SECONDARY_COUNTRIES


# setting base directory for all data
DATA_DIR = Path("data")
# defining raw and processed directories
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
# log file for errors
LOG_FILE = DATA_DIR / "pipeline_errors.log"

# standardise column names for final output files
STANDARD_COLUMNS = {
    "TMAX": "tmax_c",
    "TMIN": "tmin_c",
    "PRCP": "prcp_mm",
    "WDSP": "wind_speed_ms",
    # add other mappings as needed
}


DATASET_CONFIGS = {
    "ghcnd": {
        "description": "Global Historical Climatology Network - Daily",
        "station_inventory_url": "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt",
        "data_url_base": "https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/",
        "required_elements": ['TMAX', 'TMIN', 'PRCP'],
    },
    "gsod": {
        "description": "Global Summary of the Day",
        "station_inventory_url": "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv",
        "data_url_base": "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/",
        "fips_country_map": {
            "ZA": "SF", "MI": "MI", "MZ": "MZ", "ZI": "ZI",
            "AO": "AO", "CG": "CF", "TZ": "TZ", "WA": "WA",
        }
    },
    "isd_lite": {
        "description": "Integrated Surface Dataset Lite (Hourly)",
        "station_inventory_url": "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv",
        "data_url_base": "https://www.ncei.noaa.gov/data/global-hourly/access/",
    },
    "normals_daily": {
        "description": "Daily Normals 1991-2020 (Access)",
        "data_url_base": "https://www.ncei.noaa.gov/data/normals-daily/1991-2020/access/",
        "required_elements": [
            "dly-tmax-normal",
            "dly-tmin-normal",
            "dly-prcp-normal"
        ]
    }
}