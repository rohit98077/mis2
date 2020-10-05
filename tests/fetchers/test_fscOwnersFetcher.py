import unittest
from src.fetchers.fscOwnersFetcher import getOwnersForFscIds
import datetime as dt
from src.appConfig import getConfig


class TestFscOwnersFetcher(unittest.TestCase):
    appConfig: dict = {}

    def setUp(self):
        self.appConfig = getConfig()

    def test_run(self) -> None:
        """tests the function that fetches the owners of 
        Fscs from reporting software
        """
        elemIds = [2, 3]
        ownersDict = getOwnersForFscIds(
            self.appConfig['reportsConStr'], elemIds)
        expectedDict = {
            2: "ATIL-Adani", 3: "POWERGRID-WR2 (PGCIL)"}
        self.assertTrue(ownersDict == expectedDict)
