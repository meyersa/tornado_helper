import unittest 
from dotenv import load_dotenv
import os 
from tornado_helper import Helper 
from pathlib import Path 

DATA_DIR = "data"
TEST_LINK = "http://link.testfile.org/150MB"
TEST_FILE = "150MB-Corrupt-Testfile.Org.zip"

class test_helper(unittest.TestCase):

    def setUp(self) -> None:
        load_dotenv() 

        self.bucket = os.getenv("bucket_name")
        self.app_key = os.getenv("application_key")
        self.app_key_id = os.getenv("application_key_id")

        self.full_file = Path.joinpath(Path.cwd(), DATA_DIR, TEST_FILE)

        self.Helper = Helper()

    def test_instance(self): 
        # Should be an instance of the Obj
        self.assertIsInstance(self.Helper, Helper)

        # Should also create Data dir
        self.assertTrue(os.path.exists("./data"))

    def test_download(self): 
        self.assertTrue(self.Helper.download([TEST_LINK], output_dir=DATA_DIR))
        self.assertTrue(os.path.exists(self.full_file))

    def test_upload(self): 
        self.assertTrue(self.Helper.upload([str(self.full_file)], self.bucket, self.app_key, self.app_key_id))

    def test_delete(self): 
        self.Helper.delete([self.full_file])
        self.assertTrue(not os.path.exists(self.full_file))

    def test_bucket_download(self): 
        self.assertTrue(self.Helper.download([TEST_FILE], self.bucket, output_dir=DATA_DIR))
        self.assertTrue(os.path.exists(self.full_file))