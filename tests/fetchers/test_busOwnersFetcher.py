import unittest
from src.fetchers.busOwnersFetcher import getOwnersForBusIds
import datetime as dt
from src.appConfig import getConfig


class TestBusOwnersFetcher(unittest.TestCase):
    appConfig: dict = {}

    def setUp(self):
        self.appConfig = getConfig()

    def test_run(self) -> None:
        """tests the function that fetches the owners of 
        Buses from reporting software
        """
        elemIds = [41, 44]
        ownersDict = getOwnersForBusIds(
            self.appConfig['reportsConStr'], elemIds)
        expectedDict = {
            41: "POWERGRID-WR1 (PGCIL)", 44: "APL-Adani"}
        self.assertTrue(ownersDict == expectedDict)
