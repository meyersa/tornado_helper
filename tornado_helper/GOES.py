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
from typing import List, Tuple, Union
import logging
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import time
import pandas as pd
import os
from datetime import datetime, timezone
from pathlib import Path
import re 

# Spammy
logging.getLogger("botocore").setLevel(logging.INFO)

class GOES(Helper):
    """
    A class to handle downloading GOES satellite data, extending the Helper class.
    """

    __BUCKET_NAME = "TornadoPrediction-GOES"
    __DEFAULT_DATA_DIR = "./data_goes"

    __GOES_BUCKETS = {
        "east": "noaa-goes16",  # GOES-16 (2017-Present, Eastern U.S.)
        "west_old": "noaa-goes17",  # GOES-17 (2018-2022, Western U.S.)
        "west_new": "noaa-goes18",  # GOES-18 (2023+, replaces GOES-17)
    }
    __YEARS = [2017, 2018, 2019, 2020, 2021, 2022]
    __SENSOR = "ABI-L2-MCMIPC"

    def __init__(self, partial=True, data_dir=None):
        """
        Includes partial method to dictate how much should be used
        """
        self.partial = partial

        self.data_dir = Path(data_dir or self.__DEFAULT_DATA_DIR)

        super().__init__(self.data_dir)

    def _get_bucket(self, year: int, lon: float) -> str:
        """
        Determines the appropriate GOES satellite bucket based on the year and longitude.

        Args:
            year (int): The year of the event.
            lon (float): The longitude of the event.

        Returns:
            str: The name of the corresponding S3 bucket.

        Raises:
            ValueError: If the year is out of the allowed range.
        """
        if not (self.__YEARS[0] <= year <= self.__YEARS[-1]):
            raise ValueError(
                f"Year {year} is out of range. Allowed range: {self.__YEARS[0]}-{self.__YEARS[-1]}"
            )

        # Determine East/West
        side = "east" if lon < -105 else "west"

        # Pick correct GOES satellite
        if side == "east":
            return self.__GOES_BUCKETS["east"]
        else:
            return (
                self.__GOES_BUCKETS["west_old"]
                if year < 2023
                else self.__GOES_BUCKETS["west_new"]
            )

    def _tornet_catalog(self) -> str:
        """
        Finds the TorNet catalog or downloads it
        """
        catalog_path = os.path.join(self.data_dir, "catalog.csv")
        logging.info("Checking for TorNet Catalog")

        if not os.path.exists(catalog_path):
            logging.info("Catalog not found, downloading")
            super().download(
                "https://zenodo.org/records/12636522/files/catalog.csv?download=1",
                output_dir=self.data_dir,
            )

        else:
            logging.info("Catalog found")

        return catalog_path

    def download(self, output_dir=None, single=False):
        """
        Downloads the partial GOES files, a year, or all
        """
        if single: 
            logging.info("Downloading single NC file")
            return super().download(
                self.generate_links(self.__YEARS[0])[0], output_dir=output_dir
            )       
            
        # TODO: This doesn't make sense
        if self.partial:
            logging.info("Downloading only one year")
            return super().download(
                self.generate_links(self.__YEARS[0]), output_dir=output_dir
            )
            
        logging.info("Downloading all NC files")
        return super().download(
            self.generate_links(self.__YEARS), output_dir=output_dir
        )

    # Generate links?
    # Generate catalog?
    # Download downloads from the catalog?
    # Put catalog in s3 with upload func?

    def generate_links(self, year: Union[int, List[int]]) -> List[str]:
        """
        Generates the links to GOES from the TorNet catalog
        """
        if isinstance(year, list):
            logging.info("Multiple years requested for links, calling...")
            links = list()
            for years in year:
                links.extend(self.generate_links(years))
            return links

        # Load and preprocess the catalog
        df = pd.read_csv(self._tornet_catalog())
        
        # Parse end/start to pandas for later use
        df["start_time"] = pd.to_datetime(df["start_time"]).dt.tz_localize(timezone.utc)
        df["end_time"] = pd.to_datetime(df["end_time"]).dt.tz_localize(timezone.utc)
        df = df[df["start_time"].dt.year == year]
        
        # Drop unnecessary columns to significantly reduce size
        # Will need to keep if exporting this as a df later
        df = df[["start_time", "end_time", "lat", "lon", "event_id"]]
        
        # Filter
        df["Year"] = df["start_time"].dt.year
        
        # Limit results
        if self.partial: 
            df = df.iloc[5000:6000]
   
        # Convert to the same format as the nc files
        df["Julian Day"] = df["start_time"].dt.strftime("%j").astype(int)
        df["Hour"] = df["start_time"].dt.hour
        df["nc_filename"] = None

        # Determine the appropriate bucket (using our helper method)
        df["Satellite"] = df.apply(
            lambda row: self._get_bucket(row["Year"], row["lon"]), axis=1
        )

        matching_links = []
        cached_bucket_contents = {}

        # Local function to list S3 objects for a given bucket, year, and Julian day
        def list_s3_objects(bucket: str, year: int, julian_day: int):
            key = (bucket, year, julian_day)
            if key not in cached_bucket_contents:
                prefix = f"{self.__SENSOR}/{year}/{julian_day:03d}/"
                s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
                response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
                cached_bucket_contents[key] = response.get("Contents", [])
            return cached_bucket_contents[key]

        # Group events by Year, Julian Day, Hour, and Satellite (bucket)
        for (yr, julian_day, hour, bucket), group in df.groupby(
            ["Year", "Julian Day", "Hour", "Satellite"]
        ):
            logging.info(f"Processing {yr}-{julian_day}-{hour}-{bucket}")
            # Optionally sleep here if needed: time.sleep(30)
            objects = list_s3_objects(bucket, yr, julian_day)
            for obj in objects:
                filename = obj["Key"]
                m = re.search(r"_s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})", filename)
                if m:
                    file_dt = datetime.strptime(
                        f"{m.group(1)}-{m.group(2)} {m.group(3)}:{m.group(4)}:{m.group(5)}",
                        "%Y-%j %H:%M:%S",
                    ).replace(tzinfo=timezone.utc)
                    matched = False
                    for idx, row in group.iterrows():
                        if row["start_time"] <= file_dt <= row["end_time"]:
                            df.at[idx, "nc_filename"] = filename
                            matched = True
                    if matched:
                        link = f"https://{bucket}.s3.amazonaws.com/{filename}"
                        matching_links.append(link)

        # TODO: Maybe return DF new catalog with only matches?
        df_matched = df[df["nc_filename"].notna()]
        
        # Remove duplicates
        matching_links = list(set(matching_links))
        logging.info(f"Total Matches: {len(matching_links)}")
        
        return matching_links
