from .Helper import Helper
from pathlib import Path
import logging
import pandas as pd
from typing import List, Union
from .TorNet import TorNet
from .GOES import GOES
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm


class Combined(Helper):
    """
    Combination of TorNet and GOES datasets joined on mutual data.

    This class combines the datasets from TorNet and GOES, allowing users
    to load and generate a combined catalog based on mutual information
    from both datasets.

    __DEFAULT_DATA_DIR (str): Default directory for storing combined data.
    __CATALOG (str): URL to the combined CSV catalog.

    Args:
        data_dir (str, optional): Directory where combined data is stored.
    """
    __DEFAULT_DATA_DIR = "./data_combined"
    __CATALOG = "https://f000.backblazeb2.com/file/TornadoPrediction-GOES/combined.csv"

    def __init__(self, data_dir: str = None) -> None:
        """
        Initializes the Combined class.

        Args:
            data_dir (str, optional): Directory where combined data is stored.
                Defaults to None, which uses the default directory.
        """
        data_dir = Path(data_dir or self.__DEFAULT_DATA_DIR)

        self.GOES = GOES()
        self.TorNet = TorNet()

        logging.info(f"Combined initialized at {data_dir}")
        super().__init__(data_dir)

    def catalog(self, year: Union[int, List[int], None] = None, raw: bool = False) -> pd.DataFrame:
        """
        Creates a catalog based on the TorNet and GOES catalog's joined data.

        Args:
            year (Union[int, List[int], None]): The year(s) to filter the catalog by.
                Defaults to None (no filtering).
            raw (bool): If True, builds the catalog from scratch. Defaults to False.

        Returns:
            pd.DataFrame: The combined catalog data.
        """
        logging.info(f"Fetching Raw catalog (raw={raw}) for year(s): {year}")

        if raw:
            catalog = self._build_catalog(year)

        else:
            catalog = self._load_catalog_from_csv(year)

        logging.info(f"Returning Combined catalog with {len(catalog)} entries")
        return catalog

    def _load_catalog_from_csv(self, year: Union[int, List[int], None] = None) -> pd.DataFrame:
        """
        Loads the catalog from a downloaded CSV file.

        Args:
            year (Union[int, List[int], None]): The year(s) to filter the catalog by.
                Defaults to None (no filtering).

        Returns:
            pd.DataFrame: The filtered catalog data.
        """
        logging.info("Loading GOES catalog from CSV...")
        df = pd.read_csv(self.__CATALOG, parse_dates=["start_time", "end_time"])
        logging.debug(f"Loaded {len(df)} records from CSV")

        if year is None:
            return df
        elif isinstance(year, int):
            filtered = df[df["year"] == year]
        else:
            filtered = df[df["year"].isin(year)]

        logging.info(
            f"Filtered catalog to {len(filtered)} records for year(s): {year}")
        return filtered

    def _build_catalog(self, year: Union[int, List[int], None] = None) -> pd.DataFrame:
        """
        Builds a catalog by combining data from GOES and TorNet.

        Args:
            year (Union[int, List[int], None]): The year(s) to generate the catalog for.

        Returns:
            pd.DataFrame: The combined catalog data from both datasets.
        """
        if isinstance(year, int):
            years = [year]
        else:
            years = year

        if years:
            logging.info(f'Generating catalog for {years}')

        else:
            logging.info(f'Generating catalog for all years')

        goes_df = self.GOES.catalog(years)
        tor_df = self.TorNet.catalog(years)

        logging.debug("Imported GOES and TorNet catalogs")

        goes_df['datetime'] = pd.to_datetime(goes_df['datetime'])
        tor_df['start_time'] = pd.to_datetime(tor_df['start_time'])
        tor_df['end_time'] = pd.to_datetime(tor_df['end_time'])

        tor_df['start_time'] = tor_df['start_time'].dt.tz_localize('UTC')
        tor_df['end_time'] = tor_df['end_time'].dt.tz_localize('UTC')

        logging.debug("Parsed Dates for both catalogs")

        logging.debug("Starting multithreader")

        with ProcessPoolExecutor() as executor:
            results = list(tqdm(
                executor.map(lambda row: self._enrich_row(row, goes_df), [
                             row for _, row in tor_df.iterrows()]),
                total=len(tor_df)
            ))
        logging.debug("Finished combining")

        combined_df = pd.DataFrame([r for r in results if r is not None])
        logging.info(
            f"Returning Combined catalog with {len(combined_df)} entries")
        return combined_df

    @staticmethod
    def _enrich_row(row: pd.Series, goes_df: pd.DataFrame) -> Union[dict, None]:
        """
        Enriches a single row from the TorNet dataset with data from GOES.

        Args:
            row (pd.Series): A row from the TorNet catalog.
            goes_df (pd.DataFrame): The GOES catalog dataframe.

        Returns:
            dict or None: The enriched row as a dictionary, or None if no match was found.
        """
        region = 'west' if row['lon'] <= -105 else 'east'
        time_match = goes_df[
            (goes_df['region'] == region) &
            (goes_df['datetime'] >= row['start_time']) &
            (goes_df['datetime'] <= row['end_time'])
        ]
        if not time_match.empty:
            match = time_match.iloc[0]
            enriched = row.to_dict()
            enriched['GOES_FILENAME'] = match['nc_filename']
            enriched['GOES_SATELLITE'] = match['satellite']
            return enriched
        return None
