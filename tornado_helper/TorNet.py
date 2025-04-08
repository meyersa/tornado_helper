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
    __CATALOG = "https://zenodo.org/records/12636522/files/catalog.csv?download=1"
    __YEARS = {
        2013: "https://zenodo.org/records/12636522/files/tornet_2013.tar.gz?download=1",
        2014: "https://zenodo.org/records/12637032/files/tornet_2014.tar.gz?download=1",
        2015: "https://zenodo.org/records/12655151/files/tornet_2015.tar.gz?download=1",
        2016: "https://zenodo.org/records/12655179/files/tornet_2016.tar.gz?download=1",
        2017: "https://zenodo.org/records/12655183/files/tornet_2017.tar.gz?download=1",
        2018: "https://zenodo.org/records/12655187/files/tornet_2018.tar.gz?download=1",
        2019: "https://zenodo.org/records/12655716/files/tornet_2019.tar.gz?download=1",
        2020: "https://zenodo.org/records/12655717/files/tornet_2020.tar.gz?download=1",
        2021: "https://zenodo.org/records/12655718/files/tornet_2021.tar.gz?download=1",
        2022: "https://zenodo.org/records/12655719/files/tornet_2022.tar.gz?download=1",
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
                df = df[df['start_time'].dt.year == year]

            elif isinstance(year, list):
                df = df[df['start_time'].dt.year.isin(year)]

        logging.info(f"Returning GOES catalog with {len(df)} entries")
        return df
    
    def download(self, year: Union[int, List[int], None] = None, output_dir: str = None) -> bool:
        """
        Downloads TorNet data for a specific year or list of years.
        
        Args:
            year (int, list of int, optional): Year or list of years to download. If None, downloads all years.
            output_dir (str, optional): Directory to store the downloaded files. Defaults to class data_dir.
        
        Returns:
            bool: True if download succeeds, False otherwise.
        """
        logging.info("Starting download process")

        if not output_dir:
            output_dir = self.data_dir

        # Determine which years to download
        if year is None:
            urls = list(self.__YEARS.values())
        elif isinstance(year, int):
            urls = [self.__YEARS.get(year)]
        else:
            urls = [self.__YEARS.get(y) for y in year if y in self.__YEARS]

        if not urls or any(u is None for u in urls):
            logging.error("Invalid year(s) specified for download.")
            return False

        return super().download(urls, output_dir=output_dir)

