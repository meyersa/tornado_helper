import logging
from .Helper import Helper
from pathlib import Path
import pandas as pd
from typing import List, Union


class TorNet(Helper):
    """
    Class for handling TorNet data downloads and uploads.

    This class facilitates downloading data either fully or partially from a raw Zenodo source
    or from a specified bucket, as well as uploading data to an S3 bucket.
    """

    __DEFAULT_DATA_DIR = "./data_tornet"
    __CATALOG = "https://f000.backblazeb2.com/file/TorNetBecauseZenodoSlow/catalog.csv"
    __BUCKET = "TorNetBecauseZenodoSlow"
    __YEARS = {
        2013: "tornet_2013.tar.gz",
        2014: "tornet_2014.tar.gz",
        2015: "tornet_2015.tar.gz",
        2016: "tornet_2016.tar.gz",
        2017: "tornet_2017.tar.gz",
        2018: "tornet_2018.tar.gz",
        2019: "tornet_2019.tar.gz",
        2020: "tornet_2020.tar.gz",
        2021: "tornet_2021.tar.gz",
        2022: "tornet_2022.tar.gz",
    }

    def __init__(self, data_dir: str = None):
        """
        Initializes the TorNet object with options to download raw data from Zenodo or use an existing bucket.

        Args:
            data_dir (str, optional): Directory to store downloaded data. Defaults to None.
        """
        data_dir = Path(data_dir or self.__DEFAULT_DATA_DIR)

        logging.info(f"TorNet initialized at {data_dir}")
        super().__init__(data_dir)

    def catalog(self, year: Union[int, List[int], None] = None) -> pd.DataFrame:
        """
        Returns the TorNet Catalog as a DataFrame
        If a year or list of years is provided, returns data only for those years.
        Otherwise, returns all data.

        Args:
            year (int, list of int, optional): Year or list of years to download. If None, downloads all years.

        Returns:
            pd.Dataframe of csv
        """
        logging.info(f"Fetching TorNet catalog for year(s): {year}")

        df = pd.read_csv(self.__CATALOG, parse_dates=["start_time", "end_time"])

        if year is not None:
            if isinstance(year, int):
                df = df[df["start_time"].dt.year == year]

            elif isinstance(year, list):
                df = df[df["start_time"].dt.year.isin(year)]

        logging.info(f"Returning GOES catalog with {len(df)} entries")
        return df

    def download(
        self, year: Union[int, List[int], None] = None, output_dir: str = None
    ) -> list:
        """
        Downloads TorNet data for a specific year or list of years.

        Args:
            year (int, list of int, optional): Year or list of years to download. If None, downloads all years.
            output_dir (str, optional): Directory to store the downloaded files. Defaults to class data_dir.

        Returns:
            list: List of downloaded files
        """
        logging.info("Starting download process")

        if not output_dir:
            output_dir = self.data_dir

        # Determine which years to download
        if year is None:
            files = list(self.__YEARS.values())
        elif isinstance(year, int):
            files = [self.__YEARS.get(year)]
        else:
            files = [self.__YEARS.get(y) for y in year if y in self.__YEARS]

        if not files or any(f is None for f in files):
            logging.error("Invalid year(s) specified for download.")
            return False

        urls = [
            f"{self._DEFAULT_PROXY_URL}/file/{self.__BUCKET}/{file}" for file in files
        ]
        return super().download(urls, output_dir=output_dir)
