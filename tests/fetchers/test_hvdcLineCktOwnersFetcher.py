import unittest
from src.fetchers.hvdcLineCktOwnersFetcher import getOwnersForHvdcLineCktIds
import datetime as dt
from src.appConfig import getConfig


class TestHvdcLineCktOwnersFetcher(unittest.TestCase):
    appConfig: dict = {}

    def setUp(self):
        self.appConfig = getConfig()

    def test_run(self) -> None:
        """tests the function that fetches the owners of 
        HvdcLineCkts from reporting software
        """
        elemIds = [12, 13]
        ownersDict = getOwnersForHvdcLineCktIds(
            self.appConfig['reportsConStr'], elemIds)
        expectedDict = {
            12: "POWERGRID-WR1 (PGCIL)", 13: "POWERGRID-SR,POWERGRID-WR1 (PGCIL)"}
        self.assertTrue(ownersDict == expectedDict)
