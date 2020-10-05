import unittest
from src.fetchers.busReactorOwnersFetcher import getOwnersForBusReactorIds
import datetime as dt
from src.appConfig import getConfig


class TestBusReactorOwnersFetcher(unittest.TestCase):
    appConfig: dict = {}

    def setUp(self):
        self.appConfig = getConfig()

    def test_run(self) -> None:
        """tests the function that fetches the owners of 
        BusReactors from reporting software
        """
        elemIds = [20, 21]
        ownersDict = getOwnersForBusReactorIds(
            self.appConfig['reportsConStr'], elemIds)
        expectedDict = {
            20: "POWERGRID-WR2 (PGCIL)", 21: "MEGPTCL"}
        self.assertTrue(ownersDict == expectedDict)
