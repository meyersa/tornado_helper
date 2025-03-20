from typing import List
import logging
from .Helper import Helper

class TorNet(Helper):
    """
    Class for handling TorNet data downloads and uploads.
    
    This class facilitates downloading data either fully or partially from a raw Zenodo source 
    or from a specified bucket, as well as uploading data to an S3 bucket.
    """
    __DEFAULT_DATA_DIR = "./data_tornet"
    __BUCKET_NAME = "TorNetBecauseZenodoSlow"
    __LINKS = {
        "bucket": [
            "tornet_2013.tar.gz", "tornet_2014.tar.gz", "tornet_2015.tar.gz", 
            "tornet_2016.tar.gz", "tornet_2017.tar.gz", "tornet_2018.tar.gz", 
            "tornet_2019.tar.gz", "tornet_2020.tar.gz", "tornet_2021.tar.gz", "tornet_2022.tar.gz"
        ],
        "raw": [
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
        ],
    }

    def __init__(self, data_dir: str = None, partial: bool = True, raw: bool = False):
        """
        Initializes the TorNet object with options to download raw data from Zenodo or use an existing bucket.
        
        Args:
            data_dir (str, optional): Directory to store downloaded data. Defaults to None.
            partial (bool, optional): If True, only the first dataset will be downloaded. Defaults to True.
            raw (bool, optional): If True, downloads data directly from Zenodo instead of the bucket. Defaults to False.
        """
        self.partial = partial
        self.raw = raw
        
        logging.info("TorNet initialized with partial=%s, raw=%s", partial, raw)
        super().__init__(data_dir)

    def upload(self, files: List[str], application_key: str, application_key_id: str) -> bool:
        """
        Uploads the specified files to the predefined S3 bucket.
        
        Args:
            files (List[str]): List of file paths to be uploaded.
            application_key (str): Application key for authentication.
            application_key_id (str): Application key ID for authentication.
        
        Returns:
            bool: True if upload is successful, False otherwise.
        """
        logging.info("Uploading files to bucket: %s", self.__BUCKET_NAME)
        logging.debug("Files to upload: %s", files)
        return super().upload(files, self.__BUCKET_NAME, application_key, application_key_id)

    def download(self, output_dir: str = "TorNet_data") -> bool:
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

        if self.raw:
            if self.partial:
                logging.info("Downloading single file from raw Zenodo link")
                return super().download(self.__LINKS["raw"][0], output_dir=output_dir)
            
            logging.info("Downloading full dataset from raw Zenodo links")
            return super().download(self.__LINKS["raw"], output_dir=output_dir)

        if self.partial:
            logging.info("Downloading single file from bucket")
            return super().download(self.__LINKS["bucket"][0], bucket=self.__BUCKET_NAME, output_dir=output_dir)

        logging.info("Downloading full dataset from bucket")
        return super().download(self.__LINKS["bucket"], bucket=self.__BUCKET_NAME, output_dir=output_dir)
