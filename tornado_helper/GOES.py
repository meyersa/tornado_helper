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
import datetime
from typing import List
import logging
import boto3
from botocore import UNSIGNED
from botocore.config import Config


class GOES(Helper):
    """
    A class to handle downloading GOES satellite data, extending the Helper class.

    Attributes:
        __GOES_BUCKETS (dict): Mapping of satellite positions to their respective S3 buckets.
        __YEAR_START (int): The earliest year for which data is available.
        __YEAR_END (int): The latest year for which data is available.
        __SENSOR (str): The sensor type to be used in the data path.
    """
    __DEFAULT_DATA_DIR = "./data_goes"
    __GOES_BUCKETS = {
        "east": "noaa-goes16",  # GOES-16 (2017-Present, Eastern U.S.)
        "west_old": "noaa-goes17",  # GOES-17 (2018-2022, Western U.S.)
        "west_new": "noaa-goes18",  # GOES-18 (2023+, replaces GOES-17)
    }
    __YEAR_START = 2017
    __YEAR_END = 2022
    __SENSOR = "ABI-L2-MCMIPC"

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
        if not (self.__YEAR_START <= year <= self.__YEAR_END):
            raise ValueError(
                f"Year {year} is out of range. Allowed range: {self.__YEAR_START}-{self.__YEAR_END}"
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

    def download(
        self,
        year: int,
        month: int,
        day: int,
        hour: int,
        lat: float,
        lon: float,
        bands: List[str] = ["M3"],
        output_dir: str = None,
    ) -> List[str]:
        """
        Downloads GOES satellite data for the specified time and location.

        Args:
            year (int): Year of the event.
            month (int): Month of the event.
            day (int): Day of the event.
            hour (int): Hour of the event (UTC).
            lat (float): Latitude of the event.
            lon (float): Longitude of the event.
            bands (List[str], optional): List of spectral bands to download. Defaults to ["M3"].
            output_dir (str, optional): Directory to save the downloaded files. Defaults to None.

        Returns:
            List[str]: List of paths to the downloaded files.

        Raises:
            ValueError: If no matching files are found for the specified parameters.
        """
        bucket = self._get_bucket(year, lon)
        logging.info(f"Downloading TorNet data from bucket {bucket}")

        # Convert to Julian day for GOES S3 path
        julian_day = datetime.date(year, month, day).timetuple().tm_yday
        logging.info(f"Julian time for bucket {julian_day}")

        # Construct S3 path for GOES data
        goes_path = f"{self.__SENSOR}/{year}/{julian_day:03d}/{hour:02d}/"
        logging.info(f"Searching GOES data in: s3://{bucket}/{goes_path}")

        # Initialize S3 client
        s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

        # List files in the directory
        response = s3.list_objects_v2(Bucket=bucket, Prefix=goes_path)

        if "Contents" not in response:
            logging.warning("No files found for the requested time and region.")
            return []

        # Collect matching file links
        file_links = [
            f"https://{bucket}.s3.amazonaws.com/{obj['Key']}"
            for obj in response["Contents"]
            if any(band in obj["Key"] for band in bands)
        ]

        if not file_links:
            logging.warning("No matching GOES files found for the requested bands.")
            return []

        # Call the super method to download the files
        downloaded_files = super().download(
            links=file_links,
            output_dir=output_dir,
        )

        logging.info(f"Successfully downloaded {len(downloaded_files)} files.")
        return downloaded_files

    def upload(self):
        """
        Overrides the upload method to prevent uploading to GOES.

        Raises:
            ValueError: Always, as uploading to GOES is not permitted.
        """
        raise ValueError("Cannot upload to GOES")

    # TODO: download -> clean -> tfrecord -> s3