import unittest
from src.rawDataCreators.outagesRawDataCreator import createOutageEventsRawData
import datetime as dt
from src.appConfig import getConfig


class TestOutageEventsRawDataCreation(unittest.TestCase):
    appConfig = None

    def setUp(self):
        self.appConfig = getConfig()

    def test_run(self) -> None:
        """tests the raw outages fetching and creation process
        """
        startDate = dt.datetime(2020, 8, 1)
        endDate = dt.datetime(2020, 8, 12)

        self.assertTrue(createOutageEventsRawData(
            self.appConfig, startDate, endDate))

    def test_demo(self):
        """This is a demo test method
        """
        self.assertTrue(1 == 1)
