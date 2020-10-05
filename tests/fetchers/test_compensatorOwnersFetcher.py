import unittest
from src.fetchers.compensatorOwnersFetcher import getOwnersForCompensatorIds
import datetime as dt
from src.appConfig import getConfig


class TestCompensatorOwnersFetcher(unittest.TestCase):
    appConfig: dict = {}

    def setUp(self):
        self.appConfig = getConfig()

    def test_run(self) -> None:
        """tests the function that fetches the owners of 
        Compensators from reporting software
        """
        elemIds = [19, 20]
        ownersDict = getOwnersForCompensatorIds(
            self.appConfig['reportsConStr'], elemIds)
        expectedDict = {
            20: "POWERGRID-WR1 (PGCIL)", 19: "POWERGRID-WR2 (PGCIL)"}
        self.assertTrue(ownersDict == expectedDict)
