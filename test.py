import unittest
from tests.fetchers import test_outagesFetcher
from tests.rawDataCreators import test_outagesRawDataCreator

# initialize the test suite
loader = unittest.TestLoader()
rohitTestSuite = unittest.TestSuite()

# add tests to the test suite
rohitTestSuite.addTests(loader.loadTestsFromModule(test_outagesFetcher))
rohitTestSuite.addTests(loader.loadTestsFromModule(test_outagesFetcher))

# initialize a runner, pass it your suite and run it
runner = unittest.TextTestRunner(verbosity=3)
result = runner.run(rohitTestSuite)