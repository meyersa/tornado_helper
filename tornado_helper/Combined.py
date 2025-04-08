import gc
import os
import numpy as np
from pyproj import Proj
from .Helper import Helper
from pathlib import Path
import logging
import pandas as pd
from typing import List, Union
from .TorNet import TorNet
from .GOES import GOES
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import xarray as xr
from itertools import islice


class Combined(Helper):
    """
    Combination of TorNet and GOES datasets joined on mutual data.

    This class combines the datasets from TorNet and GOES, allowing users
    to load and generate a combined catalog based on mutual information
    from both datasets.

    __DEFAULT_DATA_DIR (str): Default directory for storing combined data.
    __CATALOG (str): URL to the combined CSV catalog.

    Args:
        data_dir (str, optional): Directory where combined data is stored.
    """

    __DEFAULT_DATA_DIR = "./data_combined"
    __CATALOG = "https://f000.backblazeb2.com/file/TornadoPrediction-GOES/combined.csv"

    def __init__(self, data_dir: str = None) -> None:
        """
        Initializes the Combined class.

        Args:
            data_dir (str, optional): Directory where combined data is stored.
                Defaults to None, which uses the default directory.
        """
        data_dir = Path(data_dir or self.__DEFAULT_DATA_DIR)

        self.GOES = GOES()
        self.TorNet = TorNet()

        logging.info(f"Combined initialized at {data_dir}")
        super().__init__(data_dir)

    def catalog(
        self, year: Union[int, List[int], None] = None, raw: bool = False
    ) -> pd.DataFrame:
        """
        Creates a catalog based on the TorNet and GOES catalog's joined data.

        Args:
            year (Union[int, List[int], None]): The year(s) to filter the catalog by.
                Defaults to None (no filtering).
            raw (bool): If True, builds the catalog from scratch. Defaults to False.

        Returns:
            pd.DataFrame: The combined catalog data.
        """
        logging.info(f"Fetching Raw catalog (raw={raw}) for year(s): {year}")

        if raw:
            catalog = self._build_catalog(year)

        else:
            catalog = self._load_catalog_from_csv(year)

        logging.info(f"Returning Combined catalog with {len(catalog)} entries")
        return catalog

    def download(self, rows: pd.DataFrame, output_dir=None):
        """
        Download multiple rows
        """
        urls = [
            f"https://{row['GOES_SATELLITE']}.s3.amazonaws.com/{row['GOES_FILENAME']}"
            for _, row in rows.iterrows()
        ]
        return super().download(urls, output_dir=output_dir, unzip=False)

    def _load_catalog_from_csv(
        self, year: Union[int, List[int], None] = None
    ) -> pd.DataFrame:
        """
        Loads the catalog from a downloaded CSV file.

        Args:
            year (Union[int, List[int], None]): The year(s) to filter the catalog by.
                Defaults to None (no filtering).

        Returns:
            pd.DataFrame: The filtered catalog data.
        """
        logging.info("Loading Combined catalog from CSV...")
        df = pd.read_csv(self.__CATALOG, parse_dates=["start_time", "end_time"])
        logging.debug(f"Loaded {len(df)} records from CSV")

        if year is None:
            return df
        elif isinstance(year, int):
            filtered = df[df["start_time"].dt.year == year]
        else:
            filtered = df[df["start_time"].dt.year.isin(year)]

        logging.info(f"Filtered catalog to {len(filtered)} records for year(s): {year}")
        return filtered

    def _build_catalog(self, year: Union[int, List[int], None] = None) -> pd.DataFrame:
        """
        Builds a catalog by combining data from GOES and TorNet.

        Args:
            year (Union[int, List[int], None]): The year(s) to generate the catalog for.

        Returns:
            pd.DataFrame: The combined catalog data from both datasets.
        """
        if isinstance(year, int):
            years = [year]
        else:
            years = year

        if years:
            logging.info(f"Generating catalog for {years}")

        else:
            logging.info(f"Generating catalog for all years")

        goes_df = self.GOES.catalog(years)
        tor_df = self.TorNet.catalog(years)

        logging.debug("Imported GOES and TorNet catalogs")

        goes_df["datetime"] = pd.to_datetime(goes_df["datetime"])
        tor_df["start_time"] = pd.to_datetime(tor_df["start_time"])
        tor_df["end_time"] = pd.to_datetime(tor_df["end_time"])

        tor_df["start_time"] = tor_df["start_time"].dt.tz_localize(
            "UTC", ambiguous="NaT", nonexistent="NaT"
        )
        tor_df["end_time"] = tor_df["end_time"].dt.tz_localize(
            "UTC", ambiguous="NaT", nonexistent="NaT"
        )

        logging.debug("Parsed Dates for both catalogs")

        logging.debug("Starting multithreader")

        results = []
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self._enrich_row, row, goes_df)
                for _, row in tor_df.iterrows()
            ]
            for future in tqdm(as_completed(futures), total=len(futures)):
                result = future.result()
                if result:
                    results.append(result)

        logging.debug("Finished combining")

        checked_df = pd.DataFrame([r for r in results if r is not None])
        logging.info(f"Removed Null with {len(checked_df)} remaining entries")

        processed_df = self._process_all_goes_files(checked_df)

        return processed_df

    def _process_all_goes_files(
        self, df: pd.DataFrame, chunk_size: int = 10, max_workers: int = 10
    ) -> pd.DataFrame:
        """
        Downloads, processes, and clips GOES files for all rows in a given DataFrame.

        Each GOES file is downloaded, cleaned, spatially clipped around a tornado location,
        and saved. The DataFrame is updated with the processed filename for each row.

        Args:
            df (pd.DataFrame): The catalog DataFrame containing GOES_FILENAME, GOES_SATELLITE, lat, lon, and filename columns.
            chunk_size (int): Number of GOES files to download/process per batch. Defaults to 10.
            max_workers (int): Number of parallel threads for file processing. Defaults to 10.

        Returns:
            pd.DataFrame: Updated DataFrame with the "PROC_FILENAME" column filled in.
        """
        results = []

        logging.info("Starting GOES file processing")
        unique_files = (
            df[["GOES_FILENAME", "GOES_SATELLITE"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        logging.debug(f"Total unique GOES files to process: {len(unique_files)}")

        for file_chunk in self._chunked_iterable(
            unique_files["GOES_FILENAME"], chunk_size
        ):
            chunk_df = unique_files[unique_files["GOES_FILENAME"].isin(file_chunk)]
            logging.info(f"Processing chunk with {len(chunk_df)} files")

            downloaded_paths = self.download(chunk_df)
            download_map = dict(zip(chunk_df["GOES_FILENAME"], downloaded_paths))

            def process_file(filename):
                proc_files = []
                try:
                    logging.debug(f"Processing file: {filename}")
                    dname = download_map[filename]
                    matches = df[df["GOES_FILENAME"] == filename]
                    ds = xr.open_dataset(dname)

                    for _, match_row in matches.iterrows():
                        lat, lon = match_row["lat"], match_row["lon"]
                        target_dname = os.path.join(
                            self.data_dir, match_row["filename"]
                        )

                        chopped = self._clean_goes_dataset(ds)
                        clipped = self._clip_ds_to_coord(chopped, lat, lon)

                        if clipped is not None:
                            self._safe_save(clipped, target_dname)
                            proc_files.append((match_row.name, target_dname))
                            logging.debug(f"Saved clipped file to: {target_dname}")
                        else:
                            logging.warning(
                                f"Clipping failed for {filename} at ({lat}, {lon})"
                            )

                    ds.close()
                    gc.collect()
                    self._delete(dname)
                    logging.debug(f"Cleaned up: {filename}")

                except Exception as e:
                    logging.error(f"Failed processing {filename}: {e}")

                return proc_files

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(process_file, fname) for fname in download_map
                ]
                for f in as_completed(futures):
                    results.extend(f.result())

        for idx, proc_fname in results:
            df.at[idx, "PROC_FILENAME"] = proc_fname

        logging.info(f"Finished processing. {len(results)} files written.")
        return df

    @staticmethod
    def _enrich_row(row: pd.Series, goes_df: pd.DataFrame) -> Union[dict, None]:
        """
        Enriches a TorNet catalog row with matching GOES file metadata.

        Args:
            row (pd.Series): A single row from the TorNet catalog.
            goes_df (pd.DataFrame): GOES catalog with 'datetime', 'region', etc.

        Returns:
            dict or None: The enriched row as a dict, or None if no match found.
        """
        region = "west" if row["lon"] <= -105 else "east"
        time_match = goes_df[
            (goes_df["region"] == region)
            & (goes_df["datetime"] >= row["start_time"])
            & (goes_df["datetime"] <= row["end_time"])
        ]
        if not time_match.empty:
            match = time_match.iloc[0]
            enriched = row.to_dict()
            enriched["GOES_FILENAME"] = match["nc_filename"]
            enriched["GOES_SATELLITE"] = match["satellite"]
            logging.debug(f"Matched row to GOES file: {match['nc_filename']}")
            return enriched

        logging.debug("No GOES match found for row")
        return None

    @staticmethod
    def _get_ds_coords(ds):
        """
        Extracts geospatial lat/lon bounds from a GOES dataset.

        Args:
            ds (xarray.Dataset): The GOES dataset.

        Returns:
            list: [min_lat, max_lat, min_lon, max_lon]
        """
        logging.debug("Extracting lat/lon bounds from dataset")
        x = ds["x"].values
        y = ds["y"].values

        proj_attrs = ds["goes_imager_projection"].attrs
        h = proj_attrs["perspective_point_height"]
        lon_0 = proj_attrs["longitude_of_projection_origin"]
        sweep = proj_attrs.get("sweep_angle_axis", "x")
        a = proj_attrs.get("semi_major_axis", 6378137.0)

        X, Y = np.meshgrid(x, y)
        p = Proj(proj="geos", h=h, lon_0=lon_0, sweep=sweep, a=a)
        lon, lat = p(X * h, Y * h, inverse=True)

        return [np.nanmin(lat), np.nanmax(lat), np.nanmin(lon), np.nanmax(lon)]

    @staticmethod
    def _latlon_to_xy(ds, lat, lon):
        """
        Converts lat/lon to projected x/y coordinates using dataset projection.

        Args:
            ds (xarray.Dataset): GOES dataset with projection attributes.
            lat (float): Latitude.
            lon (float): Longitude.

        Returns:
            tuple: (x, y) in dataset's projection space
        """
        logging.debug(f"Projecting lat/lon ({lat}, {lon}) to x/y")
        attrs = ds["goes_imager_projection"].attrs

        h = attrs["perspective_point_height"]
        lon_0 = attrs["longitude_of_projection_origin"]
        sweep = attrs.get("sweep_angle_axis", "x")
        a = attrs.get("semi_major_axis", 6378137.0)

        p = Proj(proj="geos", h=h, lon_0=lon_0, sweep=sweep, a=a)
        x, y = p(lon, lat, inverse=False)
        return x / h, y / h

    @staticmethod
    def _clip_ds_to_coord(ds, lat, lon, buffer=1.0):
        """
        Clips a dataset spatially around a lat/lon point with a buffer.

        Args:
            ds (xarray.Dataset): GOES dataset.
            lat (float): Center latitude.
            lon (float): Center longitude.
            buffer (float): Degree buffer to apply. Defaults to 1.0.

        Returns:
            xarray.Dataset or None: Clipped dataset or None on failure.
        """
        logging.debug(f"Clipping dataset around ({lat}, {lon}) with buffer={buffer}")
        try:
            x1, y1 = Combined._latlon_to_xy(ds, lat + buffer, lon - buffer)
            x2, y2 = Combined._latlon_to_xy(ds, lat - buffer, lon + buffer)
        except Exception as e:
            logging.error(f"Projection failed: {e}")
            return None

        x_slice = slice(min(x1, x2), max(x1, x2))
        y_slice = slice(max(y1, y2), min(y1, y2))  # flipped due to orientation

        try:
            clipped = ds.sel(x=x_slice, y=y_slice)
            return clipped
        except KeyError:
            logging.error("x/y not found in dataset dimensions.")
            return None

    @staticmethod
    def _clean_goes_dataset(ds: xr.Dataset) -> xr.Dataset:
        """
        Removes unnecessary variables from a GOES dataset for tornado prediction.

        Args:
            ds (xarray.Dataset): The raw GOES dataset.

        Returns:
            xarray.Dataset: Cleaned dataset with only relevant data.
        """
        logging.debug("Cleaning GOES dataset")
        vars_to_drop = [
            var
            for var in ds.data_vars
            if (
                var.startswith("CMI_")
                or var.startswith("DQF_")
                or var.startswith("outlier_pixel_count_")
                or var.startswith("min_")
                or var.startswith("max_")
                or var.startswith("mean_")
                or var.startswith("std_dev_")
                or "uncorrectable" in var
                or "container" in var
            )
        ]
        logging.debug(f"Dropping variables: {vars_to_drop}")
        return ds.drop_vars(vars_to_drop)

    @staticmethod
    def _chunked_iterable(iterable, size):
        """
        Breaks an iterable into chunks of fixed size.

        Args:
            iterable (iterable): The data to chunk.
            size (int): Size of each chunk.

        Returns:
            Iterator[list]: An iterator yielding chunks.
        """
        logging.debug(f"Chunking iterable into size {size}")
        it = iter(iterable)
        return iter(lambda: list(islice(it, size)), [])

    @staticmethod
    def _safe_save(clipped, target_dname):
        """
        Saves a clipped dataset safely, handling common errors.

        Args:
            clipped (xarray.Dataset): Dataset to save.
            target_dname (str): Target path to save the file.
        """
        if not clipped:
            logging.warning("Attempted to save an invalid dataset")
            return

        try:
            os.makedirs(os.path.dirname(target_dname), exist_ok=True)

            for var in ["x", "y"]:
                if var in clipped:
                    clipped[var].encoding.update(
                        {"dtype": "float32", "_FillValue": -9999.0}
                    )

            clipped.to_netcdf(target_dname)
            logging.debug(f"Saved dataset to {target_dname}")
        except PermissionError:
            logging.error(f"Permission denied: {target_dname}")
        except Exception as e:
            logging.error(f"Error saving {target_dname}: {e}")
        finally:
            clipped.close()
            gc.collect()
