import unittest
from src.fetchers.hvdcPoleOwnersFetcher import getOwnersForHvdcPoleIds
import datetime as dt
from src.appConfig import getConfig


class TestHvdcPoleOwnersFetcher(unittest.TestCase):
    appConfig: dict = {}

    def setUp(self):
        self.appConfig = getConfig()

    def test_run(self) -> None:
        """tests the function that fetches the owners of 
        HvdcPoles from reporting software
        """
        elemIds = [4, 5]
        ownersDict = getOwnersForHvdcPoleIds(
            self.appConfig['reportsConStr'], elemIds)
        expectedDict = {
            5: "POWERGRID-WR1 (PGCIL)", 4: "ATIL-Adani"}
        self.assertTrue(ownersDict == expectedDict)
