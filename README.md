# NOAA Climate Data Acquisition Pipeline

This project provides a robust, automated pipeline to download, process, and standardize station-based climate data from NOAA's public servers. It is designed to be configurable and resilient, handling different data formats, structures, and potential network errors.

## 1. Project Objective

The primary goal of this pipeline is to secure and archive daily and hourly station-based climate data for African countries (with a primary focus on Zambia) from NOAA's Climate Data Online (CDO) services. The processed data is intended to support agricultural yield modeling, drought monitoring, and historical climate analytics.

## 2. Features

The pipeline is capable of processing the following datasets:

*   **GHCN-D (Global Historical Climatology Network - Daily):** Downloads, parses, and cleans high-quality, homogenized daily data.
*   **GSOD (Global Summary of the Day):** Downloads, processes, and standardizes daily summary data from a broad network of stations.
*   **ISD Lite (Integrated Surface Dataset Lite):** Downloads, processes, and standardizes hourly data, providing intra-day granularity.
*   **Data Discovery:** The pipeline correctly identifies and confirms the availability (or unavailability) of datasets for the target region, such as the Daily Normals product.

## 3. Prerequisites

*   Python 3.8 or newer
*   `pip` (Python's package installer)

## 4. Installation and Setup

Follow these steps to set up your local environment.

1.  **Clone or Download the Project:**
    ```bash
    git clone https://your-repository-url/cdo_data_pipeline.git
    cd cdo_data_pipeline
    ```
    (If you downloaded it as a ZIP, unzip it and navigate into the main directory).

2.  **Create and Activate a Virtual Environment:** It is highly recommended to use a virtual environment to manage project dependencies.

    *   **Create the environment:**
        ```bash
        python -m venv venv
        ```

    *   **Activate the environment:**
        *   On **macOS / Linux**:
            ```bash
            source venv/bin/activate
            ```
        *   On **Windows**:
            ```bash
            .\venv\Scripts\activate
            ```

3.  **Install Required Libraries:**
    ```bash
    pip install -r requirements.txt
    ```

## 5. Project Structure

The project is organized into source code and data directories for clarity and maintainability.

![directory tree structure](<Screenshot 2025-07-09 at 12.08.44 PM.png>)

## 6. How to Run the Pipeline

The pipeline is controlled from the project's root directory via `src/main.py`. Do not run scripts from within the `src` folder directly.

### Command Structure

The main command follows this pattern:

```bash
python -m src.main --dataset <DATASET_NAME> --step <STEP_NAME>
```
* \<DATASET_NAME> can be one of: ghcnd, gsod, isd, normals.
* \<STEP_NAME> can be one of: download, process.

## 7. Output Description

The final, analysis-ready data is located in the data/processed/ directory.
* Data is separated into subdirectories by dataset (ghcnd, gsod, etc.).
* Within each dataset folder, data is further organized by country code (ZA, MI, etc.).
* Time-series data is stored as [STATION_ID]_[YEAR].csv.
* All processed files use standardized column headers (e.g., date, tmax_c, prcp_mm) for easy integration.

## 8. Configuration

The pipeline's behavior can be customised by editing the src/config.py file.

Key variables you can modify:

* TARGET_COUNTRIES: A list of 2-letter country codes to define the geographic scope.
* START_YEAR, END_YEAR: The temporal range for data acquisition.
* DATASET_CONFIGS: Contains all URLs and dataset-specific parameters. Update these if NOAA changes a URL path.

## 9. Error Handling

* Most download or processing errors for individual files will not stop the entire pipeline.
* All errors are logged with a timestamp and a descriptive message in the "data/pipeline_errors.log file".