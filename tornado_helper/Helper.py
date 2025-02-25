from pathlib import Path
import logging
import tempfile
import os
import subprocess
import shutil
from typing import List, Optional
from b2sdk.v2 import InMemoryAccountInfo, B2Api, AuthInfoCache, Bucket

logging.basicConfig(level=logging.INFO)


class Helper:
    """
    A helper class providing default methods to upload/download files using Backblaze B2 and aria2c.

    Attributes:
        __DATA_DIR (Path): Path to the directory for storing downloaded data.
        __TEMP_DIR (Path): Path to a temporary directory for intermediate file handling.
    """

    __DEFAULT_PROXY_URL = "https://bbproxy.meyerstk.com"
    __DEFAULT___DATA_DIR = "./data"

    def __init__(self, __DATA_DIR: Optional[str] = None) -> None:
        """
        Initializes the Helper instance by setting up data and temporary directories.

        Args:
            __DATA_DIR (Optional[str]): Custom directory to store data. Defaults to './data'.
        """
        self.__DATA_DIR = Path(__DATA_DIR or self.__DEFAULT___DATA_DIR)
        self.__DATA_DIR.mkdir(parents=True, exist_ok=True)

        self.__TEMP_DIR = Path(tempfile.mkdtemp())
        logging.info(f"Data directory set at: {self.__DATA_DIR}")
        logging.debug(f"Temporary directory created at: {self.__TEMP_DIR}")

    def _check_dependency(self, dependency: str) -> bool:
        """
        Checks if a required command-line dependency is installed.

        Args:
            dependency (str): The dependency to check.

        Returns:
            bool: True if dependency exists, otherwise raises an error.

        Raises:
            RuntimeError: If the dependency is not found.
        """
        if shutil.which(dependency):
            logging.debug(f"Dependency '{dependency}' found.")
            return True

        error_message = f"Missing dependency: '{dependency}'. Please install it and try again."
        logging.error(error_message)
        raise RuntimeError(error_message)

    def upload(
        self, files: List[str], bucket: str, application_key: str, application_key_id: str
    ) -> bool:
        """
        Uploads specified files to a given Backblaze B2 bucket.

        Args:
            files (List[str]): List of file paths to upload.
            bucket (str): Name of the B2 bucket.
            application_key (str): B2 application key for authentication.
            application_key_id (str): B2 application key ID for authentication.

        Returns:
            bool: True if uploads are successful.

        Raises:
            Exception: If authentication or upload process fails.
        """
        try:
            info = InMemoryAccountInfo()
            b2_api = B2Api(info, cache=AuthInfoCache(info))
            b2_api.authorize_account(
                "production", application_key=application_key, application_key_id=application_key_id
            )
            logging.info(f"Authorized successfully to B2 bucket: {bucket}")

            b2_bucket: Bucket = b2_api.get_bucket_by_name(bucket)

            for file in files:
                logging.info(f"Uploading file '{file}' to bucket '{bucket}'")
                b2_bucket.upload_local_file(file, os.path.basename(file))

            logging.info("All files uploaded successfully.")
            return True
        except Exception as e:
            logging.error(f"Upload failed: {e}")
            raise

    def delete(self, files: List[str]) -> None:
        """
        Deletes specified files from local storage.

        Args:
            files (List[str]): List of file paths to delete.
        """
        logging.info("Deleting specified files...")

        for file in files:
            try:
                os.remove(file)
                logging.debug(f"Deleted file: {file}")
            except OSError as e:
                logging.warning(f"Failed to delete file '{file}': {e}")

    def download(self, links: List[str], bucket: str = None, output_dir: str = None) -> bool:
        """
        Downloads files from provided URLs using aria2c.

        Args:
            links (List[str]): URLs to download files from.
            bucket: Str name of bucket to download from.
            output_dir: Directory to output to, optional. 

        Returns: 
            bool: If successful

        Raises:
            subprocess.CalledProcessError: If the download process fails.
            Exception: For other unexpected errors during download.
        """
        temp_file: Optional[tempfile.NamedTemporaryFile] = None

        self._check_dependency("aria2c")

        # If bucket convert to S3 URL
        if bucket: 
            links = [f"{self.__DEFAULT_PROXY_URL}/file/{bucket}/{link}" for link in links]

        try:
            # Write download links to a temporary file

            temp_file = tempfile.NamedTemporaryFile(delete=False, mode="w")
            temp_file.writelines(link + "\n" for link in links)
            temp_file.close()

            logging.debug(f"Temporary link file created at: {temp_file.name}")

            # Execute download command
            logging.info(f"Initiating downloads for: {', '.join(links)}")
            command = ["aria2c", "-j", "3", "-x",
                       "16", "-s", "16", "-i", temp_file.name]
            
            if output_dir: 
                command.extend(["-d", output_dir])

            logging.debug(f'Running download with options {command}')
            subprocess.run(command, check=True)
            logging.info("Downloads completed successfully.")

            os.remove(temp_file.name)
            logging.debug(f"Temporary file '{temp_file.name}' deleted.")

            return True 
        
        except subprocess.CalledProcessError as e:
            logging.error(
                f"aria2c failed with exit status {e.returncode}: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during download: {e}")
            raise