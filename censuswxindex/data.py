import os
import logging
import subprocess
import glob
import requests
import us
import numpy as np
import pandas as pd
from ecmwfapi import ECMWFService
from dateutil.relativedelta import relativedelta

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

## IFS FORECAST DATA ##
def retrieve_forecast_data(path, param, date, lead_times, init_hours=["0000", "1200"], grid="0.125", model="ifs", bounds=None):
    '''
    Downloads NWP model forecast data from ECMWF MARS archive for a given date.
    
    Inputs:
        path: path to saved data file (str)
        date: date to retrieve data for (str, YYYY-MM-DD)
        param: tuple of parameter short name and code (str, str)
        init_hours: list of initalization hours (list of str, HHHH)
        lead_times: list of lead times (list of str)
        grid: grid resolution (str)
        model: NWP model name (str)
        bounds: latitude and longitude boundaries [deg N, deg W, deg S, deg E] (list of str) or None for global
    Outputs:
        None
    '''
    assert model in ["ifs", "aifs"]
    assert bounds is None or len(bounds) == 4
    
    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
    
    # Retrieve data
    try:
        server.execute(
            {
                'class': "ai" if model == "aifs" else "od",
                'type': "fc",
                'stream': "oper",
                'expver': "1",
                'repres': "gg",
                'levtype': "sfc",
                'param': param[1],
                'time': "/".join(init_hours),
                'step': "/".join(lead_times),
                'domain': "g",
                'resol': "auto",
                'area': "/".join(bounds if bounds is not None else ["90", "-180", "-90", "180"]),
                'grid': "/".join([grid, grid]),
                'padding': "0",
                'expect': "any",
                'date': date,
            },
            path
        )       
    except Exception as e:
        logging.error(f'Forecast retrieval failed for {date}: {e}')
        return None
    return path

## IFS ANALYSIS DATA ##
def retrieve_analysis_data(path, param, date, hours=["0000", "0600", "1200", "1800"], grid="0.125", bounds=None):
    '''
    Downloads IFS analysis data from ECMWF MARS archive

    Inputs:
        target_dir: parent directory to save data within (str)
        date: date to retrieve data for (str, YYYY-MM-DD)
        param: tuple of parameter short name and code (str, str)
        valid_times: list of valid times (list of str)
        bounds: latitude and longitude boundaries [deg N, deg W, deg S, deg E] (list of str) or None for global
        grid: grid resolution (str)
    Outputs:
        None
    '''
    assert bounds is None or len(bounds) == 4

    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
     
    # Retrieve data
    try:
            server.execute(
                {
                    'class': "od",
                    'type': "an",
                    'stream': "oper",
                    'expver': "1",
                    'repres': "gg",
                    'levtype': "sfc",
                    'param': param[1],
                    'time': "/".join(hours),
                    'step': "0",
                    'domain': "g",
                    'resol': "auto",
                    'area': "/".join(bounds if bounds is not None else ["90", "-180", "-90", "180"]),
                    'grid': "/".join([grid, grid]),
                    'padding': "0",
                    'expect': "any",
                    'date': date,
                },
                path
            )
    except Exception as e:
        logging.error(f'Analysis retrieval failed for {date}: {e}')
        return None
    return path

class ECMWFDataClient:
    def __init__(self, base_dir, param, start, end, lead_times, init_hours=["0000", "1200"], grid="0.125", model="ifs", bounds=None):
        self.base_dir = base_dir
        self.fc_dir = os.path.join(base_dir, "fc")
        self.an_dir = os.path.join(base_dir, "an")
        self.param = param
        self.dates = pd.date_range(start=start, end=end, freq='D')
        self.lead_times = lead_times
        self.init_hours = init_hours
        self.init_times = self._get_init_times()
        self.valid_times = self._get_valid_times()
        self.valid_hours = np.unique([x.strftime("%H") + "00" for x in self.valid_times])
        self.grid = grid
        self.model = model
        self.bounds = bounds

    def _write_forecast_filter_file(self):
        path = os.path.join(self.base_dir, "split_fc.txt")
        with open(path, "w") as f:
            f.write(f"write \"{self.fc_dir}/{self.grid}/[shortName]/[time]/[step]/{self.model}_fc_[shortName]_[time]_[step]_[date].grib\";")
            f.close()
        return path

    def _write_analysis_filter_file(self):
        path = os.path.join(self.base_dir, "split_an.txt")
        with open(path, "w") as f:
            f.write(f"write \"{self.an_dir}/{self.grid}/[shortName]/ifs_an_[shortName]_[date].grib\";")
            f.close()
        return path

    def _get_init_times(self):
        init_times = []
        for date in self.dates:
            for init in self.init_hours:
                hour = int(init[:2])
                init_time = date + relativedelta(hours=hour)
                init_times.append(init_time)
        return np.unique(init_times)

    def _get_valid_times(self):
        valid_times = []
        for init_time in self.init_times:
            for lead in self.lead_times:
                lead_hours = int(lead)
                valid_time = init_time + relativedelta(hours=lead_hours)
                valid_times.append(valid_time)
        return np.unique(valid_times)

    def _make_forecast_dirs(self):
        for init_hour in self.init_hours:
            for lead_time in self.lead_times:
                dir = os.path.join(self.fc_dir, self.grid, self.param[0], init_hour, lead_time)
                os.makedirs(dir, exist_ok=True)
        return
    
    def _make_analysis_dir(self):
        dir = os.path.join(self.an_dir, self.grid, self.param[0])
        os.makedirs(dir, exist_ok=True)
        return

    def _apply_filter(self, filter_file, tempfile):
        subprocess.run(f'grib_filter {filter_file} {tempfile}', shell=True)
        return

    def _rm(self, path):
        subprocess.run(['rm', path])
        return

    def _sort_by_year_month(self, dir, date):
        year, month = date.strftime("%Y"), date.strftime("%m")
        path = os.path.join(dir, year, month)
        os.makedirs(path, exist_ok=True)
        datestr = date.strftime("%Y%m%d")
        file_matching_date = glob.glob(os.path.join(dir, f"*{datestr}*"))
        for file in file_matching_date:
            newfile = os.path.join(dir, year, month, os.path.basename(file))
            subprocess.run(f'mv {file} {newfile}', shell=True)

    def _grib_to_netcdf(self, dir, remove=True):
        grib_files = glob.glob(os.path.join(dir, f"*.grib"))
        for grib_file in grib_files:
            nc_file = os.path.splitext(grib_file)[0] + ".nc"
            subprocess.run(f"grib_to_netcdf -o {nc_file} {grib_file}", shell=True)
            if remove:
                self._rm(grib_file)
        return
    
    def _does_fc_exist(self, date):
        # Duplicate: already retrieved, sorted, and converted
        paths_nc = [path for init_hour in self.init_hours for lead_time in self.lead_times for path in glob.glob(os.path.join(*[self.fc_dir, self.grid, self.param[0], init_hour, lead_time, '*', '*', f'*{date.strftime("%Y%m%d")}.nc']))]
        # Duplicate: already retrieved and sorted
        paths_grib = [path for init_hour in self.init_hours for lead_time in self.lead_times for path in glob.glob(os.path.join(*[self.fc_dir, self.grid, self.param[0], init_hour, lead_time, '*', '*', f'*{date.strftime("%Y%m%d")}.grib']))]
        if (paths_nc and all(os.path.exists(path) for path in paths_nc)) or (paths_grib and all(os.path.exists(path) for path in paths_grib)):
            # Skip already downloaded files
            logging.info(f'Skipping already downloaded forecast data for {date}')
            return True
        else:
            return False

    def _does_an_exist(self, date):
        # Duplicate: already retrieved, sorted, and converted
        paths_nc = glob.glob(os.path.join(self.an_dir, self.grid, self.param[0], '*', '*', f'*{date.strftime("%Y%m%d")}.nc'))
        # Duplicate: already retrieved and sorted
        paths_grib = glob.glob(os.path.join(self.an_dir, self.grid, self.param[0], '*', '*', f'*{date.strftime("%Y%m%d")}.grib'))
        if (paths_nc and all(os.path.exists(path) for path in paths_nc)) or (paths_grib and all(os.path.exists(path) for path in paths_grib)):
            # Skip already downloaded files
            logging.info(f'Skipping already downloaded analysis data for {date}')
            return True
        else:
            return False

    def get_forecast(self):
        self._make_forecast_dirs()
        dir = os.path.join(self.fc_dir, self.grid, self.param[0])
        filter_file = self._write_forecast_filter_file()
        for date in self.dates:
            if not self._does_fc_exist(date):
                date_str = date.strftime("%Y-%m-%d")
                tempfile = os.path.join(dir, f'ifs_fc_{date.strftime("%Y%m%d")}_temp.grib')
                outfile = retrieve_forecast_data(tempfile, self.param, date_str, self.lead_times, self.init_hours, self.grid, self.model, self.bounds)
                if outfile is not None:
                    self._apply_filter(filter_file, outfile)
                    self._rm(outfile)
                    for init_hour in self.init_hours:
                        for lead_time in self.lead_times:
                            self._grib_to_netcdf(os.path.join(dir, init_hour, lead_time))
                            self._sort_by_year_month(os.path.join(dir, init_hour, lead_time), date)
        self._rm(filter_file)

    def get_analysis(self):
        self._make_analysis_dir()
        dir = os.path.join(self.an_dir, self.grid, self.param[0])
        filter_file = self._write_analysis_filter_file()
        for date in self.dates:
            if not self._does_an_exist(date):
                date_str = date.strftime("%Y-%m-%d")
                tempfile = os.path.join(dir, f'ifs_an_{date.strftime("%Y%m%d")}_temp.grib')
                outfile = retrieve_analysis_data(tempfile, self.param, date_str, self.valid_hours, self.grid, self.bounds)
                if outfile is not None:
                    self._apply_filter(filter_file, outfile)
                    self._rm(outfile)
                    self._grib_to_netcdf(dir)
                    self._sort_by_year_month(dir, date)
        self._rm(filter_file)

def retrieve_census_data(target_dir, table_list, level, base_url, year):
    '''
    Download census data from the U.S. Census Bureau API.

    This is a convenience wrapper around CensusDataClient that
    downloads the raw CSVs and builds a cleaned, joined table of
    estimate-only columns for the requested geographic level.

    Inputs:
        target_dir: parent directory to save data within (str)
        table_list: list of census table codes to download (list of str)
        level: geographic level of data to download (str: 'state', 'county', or 'tract')
        base_url: base URL for the Census API (str)
    Outputs:
        pandas.DataFrame with the joined estimates table.
    '''
    client = CensusDataClient(base_dir=target_dir,
                              table_list=table_list,
                              level=level,
                              base_url=base_url,
                              year=year)
    client.download()
    df = client.build_joined_table()
    return df


class CensusDataClient:
    def __init__(self, base_dir, table_list, level, base_url, year):
        self.base_dir = base_dir
        self.table_list = table_list
        self.level = level
        self.base_url = base_url
        self.year = year

        os.makedirs(self.base_dir, exist_ok=True)

        self.api_key = os.environ.get("CENSUS_API_KEY")
        if not self.api_key:
            raise ValueError(
                "Census API key not found. Please set the 'CENSUS_API_KEY' "
                "environment variable. See https://api.census.gov/data/key_signup.html"
            )

        if self.level not in ["state", "county", "tract"]:
            raise ValueError("Invalid level specified. Choose from 'state', 'county', or 'tract'.")
        if not isinstance(self.table_list, list) or not all(isinstance(t, str) for t in self.table_list):
            raise ValueError("table_list must be a list of strings representing table codes.")
        if not self.table_list:
            raise ValueError("table_list cannot be empty. Please provide at least one table code.")

        # Dictionary associating state FIPS codes and state abbreviations
        self.fips_state_names = {x.fips: x.abbr for x in us.states.STATES}

        # Will hold the joined, cleaned DataFrame once built
        self.joined_df = None

    def _build_params(self, table, state_code=None):
        """Build API query parameters for a given table and (optionally) state."""
        params = {
            "get": f"group({table})",
            "key": self.api_key,
        }

        if self.level == "state":
            # One national request for all states
            params["for"] = "state:*"
        else:
            if state_code is None:
                raise ValueError("state_code must be provided for county and tract levels.")
            params["for"] = f"{self.level}:*"
            params["in"] = f"state:{state_code}"

        return params

    def _state_iter(self):
        """Iterator over (state_code, state_name) for non-state levels."""
        if self.level == "state":
            yield None, "US"
        else:
            for code, name in self.fips_state_names.items():
                yield code, name

    def download(self):
        """Download raw Census CSV files for all requested tables and levels."""
        for state_code, state_name in self._state_iter():
            if self.level == "state":
                save_dir = os.path.join(self.base_dir, "state")
            else:
                save_dir = os.path.join(self.base_dir, self.level, state_name)
            os.makedirs(save_dir, exist_ok=True)

            for table in self.table_list:
                if self.level == "state":
                    outfile = os.path.join(
                        save_dir,
                        f"acs_5yr_{self.year}_state_{state_name}_{table}.csv",
                    )
                else:
                    outfile = os.path.join(
                        save_dir,
                        f"acs_5yr_{self.year}_{self.level}_{state_name}_{table}.csv",
                    )

                if os.path.exists(outfile):
                    logging.info(f"Skipping already downloaded data for {state_name}, table {table}")
                    continue

                params = self._build_params(table, state_code=state_code)
                response = requests.get(self.base_url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    df = pd.DataFrame(data[1:], columns=data[0])
                    df.to_csv(outfile, index=False)
                else:
                    logging.error(
                        f"Error: {state_name}, table {table}, URL: {response.url}, "
                        f"Status Code: {response.status_code}"
                    )

    def _load_table_dataframe(self, table):
        """Load and clean a single table across all states for the configured level."""
        dfs = []

        for _, state_name in self._state_iter():
            if self.level == "state":
                path = os.path.join(
                    self.base_dir,
                    "state",
                    f"acs_5yr_{self.year}_state_{state_name}_{table}.csv",
                )
            else:
                path = os.path.join(
                    self.base_dir,
                    self.level,
                    state_name,
                    f"acs_5yr_{self.year}_{self.level}_{state_name}_{table}.csv",
                )

            if not os.path.exists(path):
                logging.warning(f"Missing CSV for {state_name}, table {table}: {path}")
                continue

            df = pd.read_csv(path, dtype=str)

            # Keep geography columns and estimate columns only
            geo_cols = [c for c in df.columns if c in ["NAME", "state", "county", "tract"]]
            estimate_cols = [c for c in df.columns if c.endswith("E")]
            keep_cols = geo_cols + estimate_cols
            df = df[keep_cols]

            dfs.append(df)

        if not dfs:
            raise RuntimeError(f"No data loaded for table {table}. Did you run download() first?")

        combined = pd.concat(dfs, ignore_index=True)
        return combined

    def build_joined_table(self):
        """Build a single joined DataFrame of estimate-only columns across all tables."""
        if not self.table_list:
            raise ValueError("No tables configured.")

        # Start with the first table as the base
        base_table = self.table_list[0]
        base_df = self._load_table_dataframe(base_table)

        # Determine geography join keys
        join_keys = [c for c in ["state", "county", "tract"] if c in base_df.columns]

        for table in self.table_list[1:]:
            df = self._load_table_dataframe(table)

            # Ensure we only keep non-duplicate geography/name columns when merging
            drop_cols = [c for c in ["NAME"] if c in df.columns and c in base_df.columns]
            df = df.drop(columns=drop_cols)

            df = df.drop_duplicates(subset=join_keys)
            base_df = base_df.drop_duplicates(subset=join_keys)

            base_df = base_df.merge(df, on=join_keys, how="inner")

        self.joined_df = base_df
        return self.joined_df

    def save_joined_table(self, path=None):
        """Save the joined DataFrame to CSV."""
        if self.joined_df is None:
            raise RuntimeError("Joined table has not been built. Call build_joined_table() first.")

        if path is None:
            filename = f"acs_5yr_{self.year}_{self.level}_joined.csv"
            path = os.path.join(self.base_dir, filename)

        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.joined_df.to_csv(path, index=False)
        logging.info(f"Saved joined census table to {path}")
