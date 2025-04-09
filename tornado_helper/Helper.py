from pathlib import Path
import logging
import tempfile
import os
import subprocess
import shutil
import zipfile
import tarfile
from tqdm.auto import tqdm
from typing import List, Optional, Union
from b2sdk.v2 import InMemoryAccountInfo, B2Api, AuthInfoCache, Bucket
from urllib.parse import urlparse
import aria2p
import time
import socket
import shutil

ARIA_ADDRESS = "localhost"
ARIA_PORT = 6800

# Spammy
logging.getLogger("urllib3").setLevel(logging.INFO)


class Helper:
    """
    A helper class providing default methods to upload/download files using Backblaze B2 and aria2c.

    Attributes:
        __DATA_DIR (Path): Path to the directory for storing downloaded data.
        __TEMP_DIR (Path): Path to a temporary directory for intermediate file handling.
    """

    __DEFAULT_PROXY_URL = "https://bbproxy.meyerstk.com"
    __DEFAULT_ENDPOINT_URL = "https://s3.us-west-000.backblazeb2.com"
    __DEFAULT_DATA_DIR = "./data_helper"
    __TEMP_DIR = tempfile.mkdtemp()

    __DEFAULT_ARIA_OPTIONS = {
        "max-concurrent-downloads": "3",
        "max-connection-per-server": "16",
        "split": "16",
        "disable-ipv6": "true",
        "dir": __TEMP_DIR,
    }

    def __init__(self, data_dir: Optional[str] = None) -> None:
        """
        Initializes the Helper instance by setting up data and temporary directories.

        Args:
            data_dir (Optional[str]): Custom directory to store data. Defaults to './data'.
        """
        self.data_dir = Path(data_dir or self.__DEFAULT_DATA_DIR)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        logging.info(f"Data directory set at: {self.data_dir}")
        logging.debug(f"Temporary directory created at: {self.__TEMP_DIR}")

        self.aria2 = aria2p.API(
            aria2p.Client(host="http://localhost", port=6800, secret="")
        )
        logging.info("Created Aria2 Downloader")

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

        error_message = (
            f"Missing dependency: '{dependency}'. Please install it and try again."
        )
        logging.error(error_message)
        raise RuntimeError(error_message)

    def _delete(self, files: Union[str, List[str]]) -> bool:
        """
        Deletes a file or list of files.

        Args:
            files (Union[str, List[str]]): File path or list of file paths to delete.

        Returns:
            bool: True if files are deleted successfully.

        Raises:
            ValueError: If no file is provided.
        """
        if isinstance(files, list):
            logging.debug("Deleting list of files...")
            for itFile in files:
                self._delete(itFile)
            logging.debug("Deleted list of files")
            return True

        file = files
        if not file:
            raise ValueError("File has to be provided to be deleted")

        logging.debug(f"Deleting file {file}")
        if not os.path.exists(file):
            logging.warning(f"File doesn't exist {file}")
            return False

        os.remove(file)
        return True

    def _unzip(
        self,
        files: Union[str, List[str]],
        output_dir: str = None,
        delete: bool = True,
    ) -> List[str]:
        """
        Extracts files if they are .zip, .tar.gz, or .tgz.

        Args:
            files (Union[str, List[str]]): File path or list of file paths to unzip.
            output_dir (str): Directory where files should be extracted.
            delete (bool): Whether to delete the original compressed file after extraction.

        Returns:
            List[str]: List of extracted file paths.

        Raises:
            ValueError: If no file is provided.
            FileNotFoundError: If the file does not exist.
        """
        unzipped_files = []

        if isinstance(files, list):
            logging.debug("Unzipping list of files...")
            for itFile in files:
                unzipped_files.extend(self._unzip(itFile))
            logging.debug("Unzipped list of files")
            return unzipped_files

        file = files
        if not file:
            raise ValueError("File has to be provided to be unzipped")

        if not os.path.exists(file):
            raise FileNotFoundError(f"Could not find file to unzip {file}")

        if not output_dir:
            output_dir = self.data_dir

        try:
            if file.endswith(".zip"):
                logging.debug(f"Unzipping (ZipFile): {file}")
                with zipfile.ZipFile(file, "r") as zip_ref:
                    zip_ref.extractall(output_dir)
                    unzipped_files.extend(
                        [os.path.join(output_dir, name) for name in zip_ref.namelist()]
                    )

            elif file.endswith((".tar.gz", ".tgz")):
                logging.debug(f"Unzipping (TarFile): {file}")
                with tarfile.open(file, "r:gz") as tar_ref:
                    tar_ref.extractall(output_dir)
                    unzipped_files.extend(
                        [
                            os.path.join(output_dir, member.name)
                            for member in tar_ref.getmembers()
                            if member.isfile()
                        ]
                    )
            else:
                logging.debug("File does not need to be unzipped")

                # Move since isn't being unzipped to location
                new_path = os.path.join(output_dir, os.path.basename(file))
                shutil.move(file, new_path)
                unzipped_files.append(new_path)

                return unzipped_files

            if delete:
                self._delete(file)

            return unzipped_files

        except Exception as e:
            err_msg = f"Failed to extract {file}: {e}"
            logging.error(err_msg)
            raise ValueError(err_msg)

    def upload(
        self,
        files: Union[str, List[str]],
        bucket: str,
        application_key: str,
        application_key_id: str,
    ) -> bool:
        """
        Uploads specified files to a given Backblaze B2 bucket.

        Args:
            files (Union[str, List[str]]): File path or list of file paths to upload.
            bucket (str): Name of the target B2 bucket.
            application_key (str): Application key for B2 authentication.
            application_key_id (str): Application key ID for B2 authentication.

        Returns:
            bool: True if upload is successful.

        Raises:
            Exception: If the upload fails.
        """
        try:
            # Clear up logging for b2sdk...
            logging.getLogger("b2sdk").setLevel(logging.WARNING)

            info = InMemoryAccountInfo()
            b2_api = B2Api(info, cache=AuthInfoCache(info))
            b2_api.authorize_account(
                "production",
                application_key=application_key,
                application_key_id=application_key_id,
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

    def _start_aria2(self):
        """
        Method to start Aria2 in server mode

        I really don't understand why there isn't just a python library for Aria
        """
        logging.debug("Checking if Aria2 is running as server")

        try:
            logging.debug("Polling socket")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((ARIA_ADDRESS, ARIA_PORT))
            s.close()

            logging.debug("Aria is running, returning")
            return

        except Exception:
            logging.debug("Aria is not running, starting")
            return subprocess.Popen(
                ["aria2c", "--enable-rpc", f"--rpc-listen-port={ARIA_PORT}"]
            )

    def _check_aria2_status(
        self, download: Union[aria2p.Download, List[aria2p.Download]]
    ) -> bool:
        """
        Check if an Aria2 download is active

        Args:
            gid (aria2p.Download | List[aria2p.Download]): Gid to check

        Returns:
            bool: True if active, false if not
        """
        if isinstance(download, list):
            return all(self._check_aria2_status(dl) for dl in download)

        try:
            status = self.aria2.get_download(download.gid).is_complete
            logging.debug(f"Status for {download} is {status}")
            return status

        # Handle not found error
        except aria2p.ClientException:
            return False

    def download(
        self,
        links: Union[str, List[str]],
        bucket: str = None,
        output_dir: str = None,
        unzip: bool = True,
    ) -> List[str]:
        """
        Downloads files from provided URLs using aria2c with TQDM progress,
        extracts archives (.zip, .tar.gz, .tgz) if needed, and returns file paths.

        Args:
            links (str | List[str]): List of URLs to download.
            bucket (str, optional): Name of the B2 bucket if using proxy links. Defaults to None.
            output_dir (str, optional): Directory to store downloaded files. Defaults to None.
            unzip (bool, optional): Whether to extract compressed files. Defaults to True.

        Returns:
            List[str]: List of paths to downloaded (and extracted) files.

        Raises:
            subprocess.CalledProcessError: If the aria2c process fails.
            Exception: If an unexpected error occurs.
        """
        self._check_dependency("aria2c")
        self._start_aria2()

        if not output_dir:
            output_dir = self.data_dir

        if isinstance(links, str):
            links = [links]

        if bucket:
            links = [f"{self.__DEFAULT_PROXY_URL}/file/{bucket}/{link}" for link in links]

        try:
            logging.info(f"Downloading {len(links)} files with Aria2")

            downloads = [
                self.aria2.add_uris([link], self.__DEFAULT_ARIA_OPTIONS) for link in links
            ]

            logging.debug("Added links to Aria")
            logging.debug("Calculating download time...")

            # Wait for total lengths to be populated
            total_size = 0
            while True:
                total_size = 0
                all_ready = True
                for dl in downloads:
                    dl.update()
                    if dl.total_length == 0:
                        all_ready = False
                    total_size += dl.total_length
                if all_ready:
                    break
                time.sleep(1)

            logging.debug("Tracking progress...")

            pbar = tqdm(total=total_size, unit="B", unit_scale=True, desc="Aria Download", leave=False)

            downloaded_files = []
            prev_total = 0
            while True:
                current_total = sum(dl.completed_length for dl in downloads)
                pbar.update(current_total - prev_total)

                prev_total = current_total

                if all(dl.is_complete for dl in downloads):
                    break
                time.sleep(1)
                for dl in downloads:
                    dl.update()

            pbar.close()

            # Increased Logic here
            for dl in downloads:
                dl.update()
                files = [str(f) for f in dl.files]
                downloaded_files.extend(files)
                if not unzip:
                    for f in files:
                        dest = os.path.join(output_dir, os.path.basename(f))
                        os.makedirs(output_dir, exist_ok=True)
                        
                        # Handle duplicates
                        if os.path.exists(dest):
                            os.remove(dest)
                        shutil.move(f, dest)
                    
            if unzip:
                logging.info("Unzipping files...")
                return self._unzip(downloaded_files, output_dir)

            downloaded_files = [
                os.path.join(output_dir, os.path.basename(f)) for f in downloaded_files
            ]

            logging.info(f"Successfully downloaded {len(downloaded_files)} files")
            return downloaded_files

        except subprocess.CalledProcessError as e:
            logging.error(f"aria2c failed with exit status {e.returncode}: {e}")
            raise 
        
        except Exception as e:
            logging.error(f"Unexpected error during download: {e}")
            raise 
        
    def sync(
        self,
        application_key: str,
        application_key_id: str,
        delete: bool = True,
        links: List[str] = None,
        bucket: str = None,
    ) -> bool:
        """
        Synchronizes by downloading from links or bucket and uploading to B2.

        Args:
            application_key (str): Application key for B2 authentication.
            application_key_id (str): Application key ID for B2 authentication.
            delete (bool, optional): Whether to delete downloaded files after upload. Defaults to True.
            links (List[str], optional): List of URLs to download. Defaults to None.
            bucket (str, optional): Name of the B2 bucket to download from. Defaults to None.

        Returns:
            bool: True if the sync process is completed successfully.

        Raises:
            ValueError: If neither links nor bucket are provided.
        """
        logging.info("Beginning Sync Process")
        cur_files = []

        if links:
            cur_files = self.download(links=links)

        elif bucket:
            cur_files = self.download(bucket=bucket)

        else:
            raise ValueError("Must provide either links or bucket as download source")

        self.upload(
            files=cur_files,
            bucket=bucket,
            application_key=application_key,
            application_key_id=application_key_id,
        )

        if delete:
            self._delete(cur_files)

        logging.info("Sync process complete")
        return True
