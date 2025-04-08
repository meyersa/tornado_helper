"""
Helper Extension for GOES data

https://registry.opendata.aws/noaa-goes/

Pairs based off of lat/long and timestamp to TorNet

Utilizes the bands:
- Infrared (Cloud-top cooling)
- Water vapor (Storm Dynamics)
- GLM (Lightning activity)
- Visible (Band 2 for daytime storms)

From the sensor ABI-L2-MCMIPC (the one with the cool data)
"""

from .Helper import Helper
from typing import List, Union
import logging
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
import re

# Spammy
logging.getLogger("botocore").setLevel(logging.INFO)


class GOES(Helper):
    """
    Helper extension for working with GOES satellite data from NOAA.

    Pairs GOES satellite imagery with TorNet by matching lat/lon and timestamp.
    Uses multiple bands: IR (cloud-top cooling), water vapor (storm dynamics),
    GLM (lightning activity), and Band 2 (visible, daytime).

    Uses ABI-L2-MCMIPC product from GOES-16 (east) and GOES-17 (west).

    Args:
        data_dir (str or Path, optional): Directory for storing downloaded data.
    """

    __DEFAULT_DATA_DIR = "./data_goes"
    __CATALOG = "https://f000.backblazeb2.com/file/TornadoPrediction-GOES/goes.csv"
    __GOES_BUCKETS = {
        "east": "noaa-goes16",   # Eastern U.S.
        "west": "noaa-goes17",   # Western U.S.
    }
    __YEARS = [2017, 2018, 2019, 2020, 2021, 2022]
    __SENSOR = "ABI-L2-MCMIPC"

    def __init__(self, data_dir=None):
        """
        Initializes the GOES class.

        Args:
            data_dir (str, optional): Directory where GOES data is stored.
                Defaults to None, which uses the default directory.
        """
        data_dir = Path(data_dir or self.__DEFAULT_DATA_DIR)
        logging.info(f"GOES initialized at {data_dir}")
        super().__init__(data_dir)

    def catalog(self, year: Union[int, List[int], None] = None, raw: bool = False) -> pd.DataFrame:
        """
        Returns a GOES catalog DataFrame. If `raw` is True, builds the catalog
        from S3 by downloading metadata. Otherwise loads a pre-built CSV catalog.

        Args:
            year (int, list of int, or None): Year(s) to include in the catalog.
            raw (bool): If True, build catalog from S3; else use static CSV.

        Returns:
            pd.DataFrame: Catalog of GOES satellite scenes.

        Raises:
            ValueError: If the CSV file can't be read or is missing columns.
        """
        logging.info(f"Fetching GOES catalog (raw={raw}) for year(s): {year}")
        
        if raw:
            catalog = self._build_catalog_from_s3(year)

        else: 
            catalog = self._load_catalog_from_csv(year)

        logging.info(f"Returning GOES catalog with {len(catalog)} entries")
        return  catalog

    def _load_catalog_from_csv(self, year: Union[int, List[int], None] = None) -> pd.DataFrame:
        """
        Loads the GOES catalog from a static CSV and filters by year.

        Args:
            year (int, list of int, or None): Year(s) to include.

        Returns:
            pd.DataFrame: Filtered GOES catalog.

        Raises:
            ValueError: If the CSV cannot be parsed or is missing columns.
        """
        logging.info("Loading GOES catalog from CSV...")
        df = pd.read_csv(self.__CATALOG, parse_dates=["datetime"])
        logging.debug(f"Loaded {len(df)} records from CSV")

        if year is None:
            return df
        elif isinstance(year, int):
            filtered = df[df["year"] == year]
        else:
            filtered = df[df["year"].isin(year)]

        logging.info(
            f"Filtered catalog to {len(filtered)} records for year(s): {year}")
        return filtered

    def _build_catalog_from_s3(self, year: Union[int, List[int], None] = None) -> pd.DataFrame:
        """
        Builds a catalog by scanning satellite files on S3 and extracting only
        basic metadata (filename, time, region). Does not extract lat/lon.

        Args:
            year (int, list of int, or None): Year(s) to include.

        Returns:
            pd.DataFrame: Catalog with filenames, timestamps, and region.
        """
        logging.info("Building simplified GOES catalog from S3 (no lat/lon)...")

        if year is None:
            years = self.__YEARS
        elif isinstance(year, int):
            years = [year]
        else:
            years = year

        records = []
        for region, bucket in self.__GOES_BUCKETS.items():
            logging.info(f"Scanning bucket '{bucket}' ({region})...")
            for yr in years:
                logging.info(f"Listing objects for year {yr}...")
                objects = self._list_s3_objects(bucket, yr)
                logging.debug(f"Found {len(objects)} objects")

                for obj in objects:
                    filename = obj.get("Key")
                    if not filename:
                        continue

                    m = re.search(r"_s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})", filename)
                    if m:
                        try:
                            file_dt = datetime.strptime(
                                f"{m.group(1)}-{m.group(2)} {m.group(3)}:{m.group(4)}:{m.group(5)}",
                                "%Y-%j %H:%M:%S"
                            ).replace(tzinfo=timezone.utc)
                        except Exception as e:
                            logging.debug(f"Failed to parse date from {filename}: {e}")
                            continue

                        record = {
                            "nc_filename": filename,
                            "satellite": bucket,
                            "year": file_dt.year,
                            "julian Day": int(m.group(2)),
                            "hour": file_dt.hour,
                            "datetime": file_dt,
                            "region": region,
                        }
                        records.append(record)

        logging.info(f"Built simplified catalog with {len(records)} records")
        return pd.DataFrame(records)

    def _list_s3_objects(self, bucket: str, year: int) -> List[dict]:
        """
        Lists all objects in a given S3 bucket for a specific year.

        Args:
            bucket (str): S3 bucket name.
            year (int): Year to search.

        Returns:
            List[dict]: List of S3 objects with keys and metadata.
        """
        logging.debug(
            f"Listing S3 objects from bucket '{bucket}' for year {year}")
        s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        prefix = f"{self.__SENSOR}/{year}/"
        paginator = s3.get_paginator("list_objects_v2")

        all_objects = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            contents = page.get("Contents", [])
            all_objects.extend(contents)
        return all_objects

    def download(self, nc_filename: str, bucket: str, output_dir=None) -> str:
        """
        Downloads a NetCDF file from the specified S3 bucket.

        Args:
            nc_filename (str): NetCDF filename to download.
            bucket (str): S3 bucket name.
            output_dir (str or Path, optional): Directory to save the file.

        Returns:
            str: Path to the downloaded file.

        Raises:
            Exception: If the download fails.
        """
        logging.debug(f"Downloading file {nc_filename} from bucket {bucket}")
        url = f"https://{bucket}.s3.amazonaws.com/{nc_filename}"
        return super().download(url, output_dir=output_dir)
