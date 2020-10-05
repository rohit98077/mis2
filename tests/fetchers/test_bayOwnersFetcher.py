import unittest
from src.fetchers.bayOwnersFetcher import getOwnersForBayIds
import datetime as dt
from src.appConfig import getConfig


class TestBayOwnersFetcher(unittest.TestCase):
    appConfig: dict = {}

    def setUp(self):
        self.appConfig = getConfig()

    def test_run(self) -> None:
        """tests the function that fetches the owners of 
        Bays from reporting software
        """
        elemIds = [35, 36]
        ownersDict = getOwnersForBayIds(
            self.appConfig['reportsConStr'], elemIds)
        expectedDict = {35: "BDTCL-Sterlite", 36: "POWERGRID-WR2 (PGCIL)"}
        self.assertTrue(ownersDict == expectedDict)
