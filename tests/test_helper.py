import unittest 
from dotenv import load_dotenv
import os 
from tornado_helper import Helper 
from pathlib import Path 

DATA_DIR = "data"
TEST_FILE = "test.tar.gz"
DL_FILE = "test"

class test_helper(unittest.TestCase):

    def setUp(self) -> None:
        load_dotenv() 

        self.bucket = os.getenv("bucket_name")
        self.app_key = os.getenv("application_key")
        self.app_key_id = os.getenv("application_key_id")

        self.Helper = Helper()

    def test_instance(self): 
        # Should be an instance of the Obj
        self.assertIsInstance(self.Helper, Helper)

        # Should also create Data dir
        self.assertTrue(os.path.exists("./data"))

    def test_download(self): 
        # Download test file from bucket
        downloads = self.Helper.download([TEST_FILE], bucket=self.bucket, output_dir=DATA_DIR)

        print(downloads)
        self.assertEqual(len(downloads), 2) # Should be two files, macos metadata:(
        self.assertTrue(os.path.exists(Path.joinpath(Path.cwd(), DATA_DIR, os.path.basename(downloads[0])))) # Path should exist not

    def test_upload(self): 
        self.assertTrue(self.Helper.upload([Path.joinpath(Path.cwd(), DATA_DIR, DL_FILE)], self.bucket, self.app_key, self.app_key_id))

    def test_delete(self): 
        self.Helper.delete([Path.joinpath(Path.cwd(), DATA_DIR, DL_FILE)])
        self.assertTrue(not os.path.exists(Path.joinpath(Path.cwd(), DATA_DIR, DL_FILE)))

    def test_bucket_download(self): 
        self.assertTrue(self.Helper.download([os.path.basename(TEST_FILE)], self.bucket, output_dir=DATA_DIR))
        self.assertTrue(os.path.exists(Path.joinpath(Path.cwd(), DATA_DIR, DL_FILE)))