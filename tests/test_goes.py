import unittest 
from tornado_helper import GOES
import logging
import os 
import pandas as pd 

class test_goes(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logging.info("Starting GOES Tests")

        logging.debug("Loading GOES class")
        cls.GOES = GOES()

    def test_instance(self): 
        self.assertTrue(os.path.exists(self.GOES.data_dir))

    def test_catalog(self): 
        catalog = self.GOES.catalog()

        self.assertIsInstance(catalog, pd.DataFrame)   
        self.assertGreater(len(catalog), 100000)
