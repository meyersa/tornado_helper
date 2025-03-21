import unittest 
from tornado_helper import GOES
import logging
import os 

class test_goes(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logging.info("Starting GOES Tests")

        logging.debug("Loading GOES class")
        cls.GOES = GOES(partial=True)

    def test_instance(self): 
        self.assertTrue(os.path.exists(self.GOES.data_dir))

    def test_catalog(self): 
        self.GOES._tornet_catalog()
        self.assertTrue(os.path.exists(os.path.join(self.GOES.data_dir, "catalog.csv")))       

    def test_generate_links(self):
        links = self.GOES.generate_links(2017)         
        self.assertTrue(len(links) > 10)