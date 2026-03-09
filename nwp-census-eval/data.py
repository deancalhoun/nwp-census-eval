import os
import logging
import subprocess
import glob
import re
import requests
import us
import numpy as np
import pandas as pd
from ecmwfapi import ECMWFService
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil.relativedelta import relativedelta
import uuid

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
    def __init__(self, base_dir, param, start, end, lead_times, init_hours=["0000", "1200"], grid="0.125", model="ifs", bounds=None, max_concurrent_requests=20):
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
        # ECMWF allows up to ~20 queued requests; cap concurrency accordingly
        self.max_concurrent_requests = max(1, min(int(max_concurrent_requests), 20))
        # Cache of forecast dates (YYYYMMDD) that are fully complete on disk
        # for this run (i.e., have data for all init_hour/lead_time combos).
        self._fc_existing_dates: set[str] | None = None
        self._an_existing_dates: set[str] | None = None

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
        """Remove *path*, ignoring if it does not exist.

        Concurrent workers or reruns may occasionally try to delete
        the same temporary/GRIB file more than once; using rm -f keeps
        the logs clean while remaining safe.
        """
        subprocess.run(['rm', '-f', path])
        return

    def _sort_by_year_month(self, dir, date):
        """Backwards-compatible wrapper that sorts files for a specific date.

        Newer code should use _sort_by_year_month_from_filename(), which infers
        the date from each filename. This helper is retained so older call sites
        still behave as before.
        """
        datestr = date.strftime("%Y%m%d")
        self._sort_by_year_month_from_filename(dir, datestr=datestr)

    def _sort_by_year_month_from_filename(self, dir, datestr=None):
        """Move NetCDF files in *dir* into year/month subfolders.

        The date is inferred from the filename pattern *_YYYYMMDD*.*.
        If *datestr* is provided, only files containing that YYYYMMDD
        substring are moved; otherwise all matching files are moved.
        """
        # Only move NetCDF files; GRIB files are handled separately by the
        # conversion pipeline so we don't strand unconverted GRIBs in
        # year/month folders.
        pattern = re.compile(r".*_(\d{8})(?:\D.*)?\.nc$")
        for file in glob.glob(os.path.join(dir, "*")):
            if os.path.isdir(file):
                continue
            # Another worker may already have moved this file between the
            # time glob() ran and now; skip cleanly if it disappeared.
            if not os.path.exists(file):
                continue
            basename = os.path.basename(file)
            m = pattern.match(basename)
            if not m:
                continue
            file_datestr = m.group(1)
            if datestr is not None and file_datestr != datestr:
                continue
            year, month = file_datestr[:4], file_datestr[4:6]
            dest_dir = os.path.join(dir, year, month)
            os.makedirs(dest_dir, exist_ok=True)
            dest_path = os.path.join(dest_dir, basename)
            if os.path.abspath(file) == os.path.abspath(dest_path):
                continue
            # Make the move idempotent and quiet even under concurrent runs:
            # if the file vanishes between the exists() check and mv, suppress
            # the benign "cannot stat" noise from mv.
            subprocess.run(['mv', file, dest_path], stderr=subprocess.DEVNULL)

    def _cleanup_forecast_dirs(self):
        """Pre-sort any existing forecast files before existence checks.

        This ensures that _does_fc_exist(), which looks only in year/month
        subfolders, can see already-downloaded data even if earlier runs
        left files hanging in the init/lead directories.
        """
        root = os.path.join(self.fc_dir, self.grid, self.param[0])
        for init_hour in self.init_hours:
            for lead_time in self.lead_times:
                subdir = os.path.join(root, init_hour, lead_time)
                if not os.path.isdir(subdir):
                    continue
                self._sort_by_year_month_from_filename(subdir)

    def _cleanup_analysis_dir(self):
        """Pre-sort any existing analysis files before existence checks."""
        dir = os.path.join(self.an_dir, self.grid, self.param[0])
        if os.path.isdir(dir):
            self._sort_by_year_month_from_filename(dir)

    def _convert_all_forecast_grib(self):
        """Convert any stray GRIB files anywhere under the forecast tree.

        This is primarily a cleanup step for legacy runs where GRIBs may
        have been moved into year/month folders before conversion. It is
        safe and idempotent: existing NetCDFs are simply overwritten.
        """
        root = os.path.join(self.fc_dir, self.grid, self.param[0])
        # Walk recursively so we catch GRIBs both in init/lead roots and
        # in year/month subfolders.
        for grib_file in glob.glob(os.path.join(root, "**", "*.grib"), recursive=True):
            # Skip temporary GRIBs used as filter input.
            if "temp" in os.path.basename(grib_file):
                continue
            nc_file = os.path.splitext(grib_file)[0] + ".nc"
            subprocess.run(["grib_to_netcdf", "-o", nc_file, grib_file])
            self._rm(grib_file)

    def _refresh_existing_fc_dates(self):
        """Scan forecast tree once to build a cache of *complete* dates.

        A date is considered complete only if there is at least one
        GRIB/NetCDF file for every (init_hour, lead_time) combination.
        This avoids treating partially downloaded days as finished.
        """
        root = os.path.join(self.fc_dir, self.grid, self.param[0])
        # Use NetCDF filenames (post-conversion) to infer dates.
        pattern = re.compile(r".*_(\d{8})(?:\D.*)?\.nc$")
        date_pairs: dict[str, set[tuple[str, str]]] = {}
        expected_pairs = {(init_hour, lead_time) for init_hour in self.init_hours for lead_time in self.lead_times}
        for init_hour in self.init_hours:
            for lead_time in self.lead_times:
                subdir = os.path.join(root, init_hour, lead_time)
                if not os.path.isdir(subdir):
                    continue
                for ext in ("nc", "grib"):
                    for path in glob.glob(os.path.join(subdir, "*", "*", f"*.{ext}")):
                        basename = os.path.basename(path)
                        m = pattern.match(basename)
                        if not m:
                            continue
                        datestr = m.group(1)
                        date_pairs.setdefault(datestr, set()).add((init_hour, lead_time))
        complete_dates = {
            datestr for datestr, pairs in date_pairs.items() if pairs.issuperset(expected_pairs)
        }
        self._fc_existing_dates = complete_dates
        if complete_dates:
            logging.info(f"Detected {len(complete_dates)} fully complete forecast days already on disk")
        incomplete_dates = set(date_pairs.keys()) - complete_dates
        if incomplete_dates:
            logging.info(
                f"Detected {len(incomplete_dates)} partially complete forecast days "
                "that will be re-downloaded"
            )

    def _refresh_existing_an_dates(self):
        """Scan analysis tree once to build a cache of existing dates."""
        dir = os.path.join(self.an_dir, self.grid, self.param[0])
        if not os.path.isdir(dir):
            self._an_existing_dates = set()
            return
        pattern = re.compile(r".*_(\d{8})(?:\D.*)?\.(?:nc|grib)$")
        dates: set[str] = set()
        for ext in ("nc", "grib"):
            for path in glob.glob(os.path.join(dir, "*", "*", f"*.{ext}")):
                basename = os.path.basename(path)
                m = pattern.match(basename)
                if not m:
                    continue
                dates.add(m.group(1))
        self._an_existing_dates = dates
        if dates:
            logging.info(f"Detected {len(dates)} analysis days already on disk")

    def _grib_to_netcdf(self, dir, remove=True):
        grib_files = glob.glob(os.path.join(dir, f"*.grib"))
        for grib_file in grib_files:
            # Never convert temporary GRIBs; they should be removed after filtering.
            if "temp" in os.path.basename(grib_file):
                continue
            nc_file = os.path.splitext(grib_file)[0] + ".nc"
            subprocess.run(f"grib_to_netcdf -o {nc_file} {grib_file}", shell=True)
            if remove:
                self._rm(grib_file)
        return
    
    def _does_fc_exist(self, date):
        """Return True if all forecast files for *date* already exist.

        When _fc_existing_dates is populated, this is a constant-time
        membership check; otherwise it falls back to the legacy, slower
        glob-based implementation.
        """
        datestr = date.strftime("%Y%m%d")
        if self._fc_existing_dates is not None:
            if datestr in self._fc_existing_dates:
                logging.info(f'Skipping already downloaded forecast data for {date}')
                return True
            return False

        # Fallback: legacy glob-based check
        paths_nc = [
            path
            for init_hour in self.init_hours
            for lead_time in self.lead_times
            for path in glob.glob(
                os.path.join(
                    *[
                        self.fc_dir,
                        self.grid,
                        self.param[0],
                        init_hour,
                        lead_time,
                        "*",
                        "*",
                        f"*{datestr}.nc",
                    ]
                )
            )
        ]
        paths_grib = [
            path
            for init_hour in self.init_hours
            for lead_time in self.lead_times
            for path in glob.glob(
                os.path.join(
                    *[
                        self.fc_dir,
                        self.grid,
                        self.param[0],
                        init_hour,
                        lead_time,
                        "*",
                        "*",
                        f"*{datestr}.grib",
                    ]
                )
            )
        ]
        if (paths_nc and all(os.path.exists(path) for path in paths_nc)) or (
            paths_grib and all(os.path.exists(path) for path in paths_grib)
        ):
            logging.info(f'Skipping already downloaded forecast data for {date}')
            return True
        return False

    def _does_an_exist(self, date):
        """Return True if all analysis files for *date* already exist."""
        datestr = date.strftime("%Y%m%d")
        if self._an_existing_dates is not None:
            if datestr in self._an_existing_dates:
                logging.info(f'Skipping already downloaded analysis data for {date}')
                return True
            return False

        # Fallback: legacy glob-based check
        paths_nc = glob.glob(
            os.path.join(
                self.an_dir,
                self.grid,
                self.param[0],
                "*",
                "*",
                f"*{datestr}.nc",
            )
        )
        paths_grib = glob.glob(
            os.path.join(
                self.an_dir,
                self.grid,
                self.param[0],
                "*",
                "*",
                f"*{datestr}.grib",
            )
        )
        if (paths_nc and all(os.path.exists(path) for path in paths_nc)) or (
            paths_grib and all(os.path.exists(path) for path in paths_grib)
        ):
            logging.info(f'Skipping already downloaded analysis data for {date}')
            return True
        return False

    def _download_forecast_for_date(self, date, dir, filter_file):
        """Full forecast retrieval + post-processing pipeline for a single date."""
        if self._does_fc_exist(date):
            return
        date_str = date.strftime("%Y-%m-%d")
        # Use a temp filename that does NOT include the date string so that
        # _sort_by_year_month() will never match or move it into year/month
        # subdirectories. This mirrors the analysis workflow and prevents
        # stranded temp files.
        tempfile = os.path.join(dir, f'ifs_fc_temp_{uuid.uuid4().hex}.grib')
        outfile = retrieve_forecast_data(
            tempfile,
            self.param,
            date_str,
            self.lead_times,
            self.init_hours,
            self.grid,
            self.model,
            self.bounds,
        )
        if outfile is None:
            return
        self._apply_filter(filter_file, outfile)
        self._rm(outfile)
        for init_hour in self.init_hours:
            for lead_time in self.lead_times:
                subdir = os.path.join(dir, init_hour, lead_time)
                self._grib_to_netcdf(subdir)
                # Sort any NetCDF/GRIB files in this subdirectory into
                # year/month folders based on the date encoded in the
                # filename. This is robust even if previous runs left
                # unsorted files hanging around.
                self._sort_by_year_month_from_filename(subdir)

    def _download_analysis_for_date(self, date, dir, filter_file):
        """Full analysis retrieval + post-processing pipeline for a single date."""
        if self._does_an_exist(date):
            return
        date_str = date.strftime("%Y-%m-%d")
        # Use a temp filename that does NOT include the date string so that
        # _sort_by_year_month() will never match or move it into year/month
        # subdirectories. This prevents leftover temp files from being
        # stranded in the final directory structure.
        tempfile = os.path.join(dir, f'ifs_an_temp_{uuid.uuid4().hex}.grib')
        outfile = retrieve_analysis_data(
            tempfile,
            self.param,
            date_str,
            self.valid_hours,
            self.grid,
            self.bounds,
        )
        if outfile is None:
            return
        self._apply_filter(filter_file, outfile)
        self._rm(outfile)
        self._grib_to_netcdf(dir)
        # Sort any NetCDF/GRIB files in this directory into year/month
        # folders based on the date encoded in the filename. This is
        # robust even if previous runs left unsorted files hanging
        # around in the root analysis directory.
        self._sort_by_year_month_from_filename(dir)

    def get_forecast(self):
        self._make_forecast_dirs()
        dir = os.path.join(self.fc_dir, self.grid, self.param[0])
        # First, convert any stray GRIBs from legacy runs so that only
        # NetCDFs remain to be sorted and checked for completeness.
        self._convert_all_forecast_grib()
        # First, sort any existing files so that _does_fc_exist() can
        # correctly detect previously downloaded data in year/month folders.
        self._cleanup_forecast_dirs()
        # Build a cached set of existing forecast dates to make skip checks fast.
        self._refresh_existing_fc_dates()
        filter_file = self._write_forecast_filter_file()
        dates_to_download = [d for d in self.dates if not self._does_fc_exist(d)]
        if not dates_to_download:
            logging.info("No missing forecast data; nothing to download.")
            self._rm(filter_file)
            return
        logging.info(
            f"Starting forecast retrieval for {len(dates_to_download)} dates "
            f"with up to {self.max_concurrent_requests} concurrent ECMWF requests"
        )
        try:
            with ThreadPoolExecutor(max_workers=self.max_concurrent_requests) as executor:
                future_to_date = {
                    executor.submit(self._download_forecast_for_date, date, dir, filter_file): date
                    for date in dates_to_download
                }
                for future in as_completed(future_to_date):
                    date = future_to_date[future]
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Forecast retrieval pipeline failed for {date}: {e}")
        finally:
            self._rm(filter_file)

    def get_analysis(self):
        self._make_analysis_dir()
        dir = os.path.join(self.an_dir, self.grid, self.param[0])
        # First, sort any existing files so that _does_an_exist() can
        # correctly detect previously downloaded data in year/month folders.
        self._cleanup_analysis_dir()
        # Build a cached set of existing analysis dates to make skip checks fast.
        self._refresh_existing_an_dates()
        filter_file = self._write_analysis_filter_file()
        dates_to_download = [d for d in self.dates if not self._does_an_exist(d)]
        if not dates_to_download:
            logging.info("No missing analysis data; nothing to download.")
            self._rm(filter_file)
            return
        logging.info(
            f"Starting analysis retrieval for {len(dates_to_download)} dates "
            f"with up to {self.max_concurrent_requests} concurrent ECMWF requests"
        )
        try:
            with ThreadPoolExecutor(max_workers=self.max_concurrent_requests) as executor:
                future_to_date = {
                    executor.submit(self._download_analysis_for_date, date, dir, filter_file): date
                    for date in dates_to_download
                }
                for future in as_completed(future_to_date):
                    date = future_to_date[future]
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Analysis retrieval pipeline failed for {date}: {e}")
        finally:
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
            estimate_cols = [c for c in df.columns if c.endswith("_E")]
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
