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
    __LINKS =  [
            "https://zenodo.org/records/12636522/files/tornet_2013.tar.gz?download=1",
            "https://zenodo.org/records/12637032/files/tornet_2014.tar.gz?download=1",
            "https://zenodo.org/records/12655151/files/tornet_2015.tar.gz?download=1",
            "https://zenodo.org/records/12655179/files/tornet_2016.tar.gz?download=1",
            "https://zenodo.org/records/12655183/files/tornet_2017.tar.gz?download=1",
            "https://zenodo.org/records/12655187/files/tornet_2018.tar.gz?download=1",
            "https://zenodo.org/records/12655716/files/tornet_2019.tar.gz?download=1",
            "https://zenodo.org/records/12655717/files/tornet_2020.tar.gz?download=1",
            "https://zenodo.org/records/12655718/files/tornet_2021.tar.gz?download=1",
            "https://zenodo.org/records/12655719/files/tornet_2022.tar.gz?download=1",
        ]
        
    def __init__(self, data_dir: str = None):
        """
        Initializes the TorNet object with options to download raw data from Zenodo or use an existing bucket.
        
        Args:
            data_dir (str, optional): Directory to store downloaded data. Defaults to None.
            partial (bool, optional): If True, only the first dataset will be downloaded. Defaults to True.
            raw (bool, optional): If True, downloads data directly from Zenodo instead of the bucket. Defaults to False.
        """
        data_dir = Path(data_dir or self.__DEFAULT_DATA_DIR)

        logging.info(f"TorNet initialized at {data_dir}")
        super().__init__(data_dir)

    def catalog(self, year: Union[int, List[int], None] = None) -> pd.DataFrame:
        """
        Returns the TorNet Catalog as a DataFrame
        If a year or list of years is provided, returns data only for those years.
        Otherwise, returns all data.
        """
        file_paths = super().download(self.__CATALOG)
        df = pd.read_csv(file_paths[0])

        if year is not None:
            if isinstance(year, int):
                df = df[df['Year'] == year]

            elif isinstance(year, list):
                df = df[df['Year'].isin(year)]

        return df
    
    def download(self, partial: bool = True, output_dir: str = None) -> bool:
        """
        Downloads TorNet data based on the specified settings.
        
        If `raw` is True, it fetches data directly from Zenodo links. Otherwise, it retrieves from an S3 bucket.
        If `partial` is True, only the first dataset is downloaded; otherwise, the entire dataset is retrieved.
        
        Args:
            output_dir (str, optional): Directory where the data will be stored. Defaults to "TorNet_data".
        
        Returns:
            bool: True if download is successful, False otherwise.
        """
        logging.info("Starting download process with raw=%s, partial=%s", self.raw, self.partial)

        if not output_dir: 
            output_dir = self.data_dir
            
        if partial:
            logging.info("Downloading single file from bucket")
            return super().download(self.__LINKS[0], output_dir=output_dir)

        logging.info("Downloading full dataset from bucket")
        return super().download(self.__LINKS, output_dir=output_dir)
