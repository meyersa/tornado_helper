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
    __DEFAULT_DATA_DIR = "./data_goes"
    __GOES_BUCKETS = {
        "east": "noaa-goes16",   # Eastern U.S.
        "west": "noaa-goes17",  # Western U.S.
    }
    __YEARS = [2017, 2018, 2019, 2020, 2021, 2022]
    __SENSOR = "ABI-L2-MCMIPC"

    def __init__(self, data_dir=None):
        data_dir = Path(data_dir or self.__DEFAULT_DATA_DIR)

        logging.info(f"GOES initialized at {data_dir}")
        super().__init__(data_dir)

    def _list_s3_objects(self, bucket: str, year: int):
        s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        prefix = f"{self.__SENSOR}/{year}/"
        paginator = s3.get_paginator("list_objects_v2")

        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        all_objects = []
        for page in page_iterator:
            contents = page.get("Contents", [])
            all_objects.extend(contents)
        return all_objects

    def catalog(self, year: Union[int, List[int], None] = None) -> pd.DataFrame:
        """
        Returns a catalog DataFrame with info from all entries in the satellite buckets.
        Scans each bucket in __GOES_BUCKETS for the given year(s) (default: __YEARS).
        """
        logging.info("Creating catalog for GOES")

        if year is None:
            years = self.__YEARS
        elif isinstance(year, int):
            years = [year]
        else:
            years = year

        records = []
        for bucket in self.__GOES_BUCKETS.values():
            logging.info(f'Collecting from Bucket: {bucket}')

            for yr in years:
                logging.info(f'Collecting from year {yr}')

                objects = self._list_s3_objects(bucket, yr)
                if not objects:
                    continue
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
                        except Exception:
                            continue
                        record = {
                            "nc_filename": filename,
                            "Satellite": bucket,
                            "Year": file_dt.year,
                            "Julian Day": int(m.group(2)),
                            "Hour": file_dt.hour,
                            "datetime": file_dt,
                        }
                        records.append(record)

        logging.info(f'Returning {len(records)} records')
        return pd.DataFrame(records)

    def download(self, nc_filename: str, bucket: str, output_dir=None):
        """
        Downloads the file given its filename and bucket.
        Returns the download result.
        """
        url = f"https://{bucket}.s3.amazonaws.com/{nc_filename}"
        return super().download(url, output_dir=output_dir)
