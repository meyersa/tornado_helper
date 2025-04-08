import unittest 
from dotenv import load_dotenv
import os 
from tornado_helper import Helper 
from pathlib import Path 
import tarfile
import logging 
import datetime
import time 

class test_helper(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        logging.info("Starting Helper Tests")

        logging.debug("Loading ENVs")
        load_dotenv() 
        cls.bucket = os.getenv("bucket_name")
        cls.app_key = os.getenv("application_key")
        cls.app_key_id = os.getenv("application_key_id")

        logging.debug("Loading Helper class")
        cls.Helper = Helper()

        # For real time tracking 
        cls.tID = datetime.datetime.now().second
        
    def test_instance(self): 
        logging.info("Testing instance")
        
        # Should be an instance of the Obj
        self.assertIsInstance(self.Helper, Helper)

        # Should also create Data dir
        self.assertTrue(os.path.exists(self.Helper.data_dir))

    def test_check_deps(self): 
         # Should pass for real aria
         self.assertTrue(self.Helper._check_dependency("aria2c"))

         # Should fail for fake aria
         with self.assertRaises(RuntimeError):
              self.Helper._check_dependency("fakearia2c")
    
    def test_delete(self): 
        logging.info("Testing delete method")

        logging.debug("Creating test file")
        test_file = "test_delete.txt"
        with open(test_file, "w") as f: 
                f.write("test")        
                
        logging.debug("Deleting test file")
        self.assertTrue(self.Helper._delete(test_file))
        
        # Should not exist anymore
        self.assertTrue(not os.path.exists(test_file))

        logging.debug("Deleting test file (already deleted)")
        self.assertTrue(not self.Helper._delete(test_file))
        
    def test_unzip(self): 
        logging.info("Testing unzip method")

        logging.debug("Creating a test file")
        tar_raw = "test_unzip.txt"
        with open(tar_raw, "w") as f: 
                f.write("test")   
                
        logging.debug("Making test file into Tar")
        tar_zip = "test.tar.gz"
        with tarfile.open(tar_zip, "w:gz") as tar: 
            tar.add(tar_raw)

        # Ensure that actually exists 
        self.assertTrue(os.path.exists(tar_zip))

        logging.debug("Deleting test file")
        self.Helper._delete(tar_raw)
        self.assertTrue(not os.path.exists(tar_raw))

        logging.debug("Unzipping file")
        unzipped = self.Helper._unzip(tar_zip)[0]
        self.assertEqual(unzipped, os.path.join(str(self.Helper.data_dir), tar_raw))

        logging.debug("Opening file to check contents")
        with open(unzipped) as f: 
            self.assertEqual(f.readline(), "test")

        logging.debug("Deleting file")
        self.Helper._delete(unzipped)
        self.assertTrue(not os.path.exists(unzipped))

    def test_upload(self): 
        logging.info("Testing file upload")
        
        logging.debug("Creating test files to upload")
        files = [f"test-1-{self.tID}", f"test-2-{self.tID}", f"test-3-{self.tID}"]
        for file in files: 
            with open(file, "w") as f: 
                f.write("test")
            
            # Confirm they exist
            self.assertTrue(os.path.exists(file))

        logging.debug("Uploading test files")
        self.assertTrue(self.Helper.upload(files, self.bucket, self.app_key, self.app_key_id))
        
        logging.debug("Deleting test files")
        for file in files: 
            self.Helper._delete(file)
            self.assertTrue(not os.path.exists(file))
            
    def test_download(self): 
        logging.info("Testing file download")

        logging.debug("Downloading test files")
        dlFiles = self.Helper.download("test.tar.gz", bucket=self.bucket)
        
        # Should have downloaded one file
        self.assertEqual(len(dlFiles), 1)
        
        # Tar should be deleted
        self.assertTrue(not os.path.exists("test.tar.gz"))

        # New file should be unzipped
        self.assertTrue(os.path.exists(os.path.join(self.Helper.data_dir, "test")))
        
        logging.debug("Deleting file")
        self.Helper._delete(os.path.join(self.Helper.data_dir, "test"))
        self.assertTrue(not os.path.exists(os.path.join(self.Helper.data_dir, "test")))
