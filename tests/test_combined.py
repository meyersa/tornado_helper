import unittest 
from tornado_helper import Combined
import logging
import os 
import pandas as pd 
from datetime import datetime

class test_goes(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logging.info("Starting Combined Tests")

        logging.debug("Loading Combined class")
        cls.combined = Combined()

        # Sample data for testing
        cls.go_data = {
            'region': ['west', 'east'],
            'datetime': [datetime(2023, 4, 1, 12, 0), datetime(2023, 4, 1, 12, 50)],
            'nc_filename': ['file_west.nc', 'file_east.nc'],
            'satellite': ['sat1', 'sat2']
        }
        cls.goes_df = pd.DataFrame(cls.go_data)

        cls.tor_data = {
            'lon': [-106, -75],
            'start_time': [datetime(2023, 4, 1, 11, 50), datetime(2023, 4, 1, 12, 10)],
            'end_time': [datetime(2023, 4, 1, 12, 10), datetime(2023, 4, 1, 12, 40)],
            'other_data': [123, 456]
        }
        cls.tor_df = pd.DataFrame(cls.tor_data)

    def test_instance(self): 
        self.assertTrue(os.path.exists(self.combined.data_dir))

    def test_catalog(self): 
        catalog = self.combined.catalog()

        self.assertIsInstance(catalog, pd.DataFrame)   
        self.assertGreater(len(catalog), 100000)

    def test_enrich_row_match(self):
        row = self.tor_df.iloc[0]
        enriched_row = self.combined._enrich_row(row, self.goes_df)

        self.assertIsInstance(enriched_row, dict)
        self.assertIn('GOES_FILENAME', enriched_row)
        self.assertEqual(enriched_row['GOES_FILENAME'], 'file_west.nc')
        self.assertEqual(enriched_row['GOES_SATELLITE'], 'sat1')

    def test_enrich_row_no_match(self):
        row = self.tor_df.iloc[1]
        enriched_row = self.combined._enrich_row(row, self.goes_df)

        self.assertIsNone(enriched_row)

    def test_enrich_row_empty_goes_df(self):
        empty_goes_df = pd.DataFrame(columns=['region', 'datetime', 'nc_filename', 'satellite'])
        row = self.tor_df.iloc[0]
        enriched_row = self.combined._enrich_row(row, empty_goes_df)

        self.assertIsNone(enriched_row)

    def test_enrich_row_empty_tor_df(self):
        empty_tor_df = pd.DataFrame(columns=['lon', 'start_time', 'end_time', 'other_data'])
        row = empty_tor_df.iloc[0] if not empty_tor_df.empty else None
        enriched_row = self.combined._enrich_row(row, self.goes_df) if row is not None else None

        self.assertIsNone(enriched_row)
