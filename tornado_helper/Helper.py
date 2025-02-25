from pathlib import Path
import logging
import tempfile
import os
import subprocess
import shutil
import zipfile
import tarfile
from tqdm import tqdm
from typing import List, Optional
from b2sdk.v2 import InMemoryAccountInfo, B2Api, AuthInfoCache, Bucket
from urllib.parse import urlparse

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

    def unzip(self, files: List[str], output_dir: str) -> List[str]:
        """
        Extracts files if they are .zip, .tar.gz, or .tgz.
        Wrapped in tqdm for progress tracking.

        Args:
            files (List[str]): List of file paths to extract.
            output_dir (str): Directory to extract files to.

        Returns:
            List[str]: List of extracted file paths.
        """
        extracted_files = []
        with tqdm(total=len(files), desc="Extracting", unit="file") as pbar:
            for file_path in files:
                try:
                    if file_path.endswith('.zip'):
                        with zipfile.ZipFile(file_path, 'r') as zip_ref:
                            zip_ref.extractall(output_dir)
                            extracted_files.extend([os.path.join(output_dir, name) for name in zip_ref.namelist()])
                        os.remove(file_path)
                        logging.info(f"Extracted and deleted: {file_path}")
                    elif file_path.endswith(('.tar.gz', '.tgz')):
                        with tarfile.open(file_path, 'r:gz') as tar_ref:
                            tar_ref.extractall(output_dir)
                            extracted_files.extend([os.path.join(output_dir, member.name)
                                                    for member in tar_ref.getmembers() if member.isfile()])
                        os.remove(file_path)
                        logging.info(f"Extracted and deleted: {file_path}")
                    else:
                        extracted_files.append(file_path)
                except Exception as e:
                    logging.error(f"Failed to extract {file_path}: {e}")
                pbar.update(1)
        
        return extracted_files

    def download(self, links: List[str], bucket: str = None, output_dir: str = None, unzip: bool = True) -> List[str]:
        """
        Downloads files from provided URLs using aria2c with TQDM progress,
        extracts archives (.zip, .tar.gz, .tgz) if needed, and returns file paths.
        """
        temp_file = None
        self._check_dependency("aria2c")
        
        if bucket:
            links = [f"{self.__DEFAULT_PROXY_URL}/file/{bucket}/{link}" for link in links]
        
        try:
            temp_file = tempfile.NamedTemporaryFile(delete=False, mode="w")
            temp_file.writelines(link + "\n" for link in links)
            temp_file.close()
            logging.debug(f"Temporary link file created at: {temp_file.name}")
            
            command = ["aria2c", "-j", "3", "-x", "16", "-s", "16", "-i", temp_file.name]
            if output_dir:
                command.extend(["-d", output_dir])
            logging.debug(f"Running download with options {command}")
            
            with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True) as process:
                pbar = tqdm(total=len(links), desc="Downloading", unit="file")
                for line in process.stdout:
                    logging.debug(line.strip())
                    if "Download complete:" in line:
                        pbar.update(1)
                process.wait()
                pbar.close()
            
            os.remove(temp_file.name)
            logging.debug(f"Temporary file '{temp_file.name}' deleted.")
            
            download_dir = output_dir if output_dir else os.getcwd()
            downloaded_files = [os.path.join(download_dir, os.path.basename(urlparse(link).path)) for link in links]

            logging.info("Downloads and extraction completed successfully.")

            if unzip:
                return self.unzip(downloaded_files, download_dir)
            
            return downloaded_files
        
        except subprocess.CalledProcessError as e:
            logging.error(f"aria2c failed with exit status {e.returncode}: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during download: {e}")
            raise