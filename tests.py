import unittest
import logging
import sys 

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

logging.info("Starting tests...")
loader = unittest.TestLoader()
tests = loader.discover(start_dir="./tests")
logging.info(f'Discovered {tests.countTestCases()} test cases')

logging.info("Running tests")
runner = unittest.TextTestRunner()
runner.run(tests)