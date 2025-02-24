import unittest 
from dotenv import load_dotenv
import os 
from src import Helper 

class test_helper(unittest.TestCase):

    def setUp(self) -> None:
        load_dotenv() 
        self.Helper = Helper()

    def test_instance(self): 
        # Should be an instance of the Obj
        self.assertIsInstance(self.Helper, Helper)

        # Should also create Data dir
        self.assertTrue(os.path.exists("./data"))

    def test_download(self): 
        self.assertTrue(self.Helper.download(["http://link.testfile.org/150MB"], output_dir="data"))