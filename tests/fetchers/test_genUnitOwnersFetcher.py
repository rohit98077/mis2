import unittest
from src.fetchers.genUnitOwnersFetcher import getOwnersForGenUnitIds
import datetime as dt
from src.appConfig import getConfig


class TestGenUnitOwnersFetcher(unittest.TestCase):
    appConfig: dict = {}

    def setUp(self):
        self.appConfig = getConfig()

    def test_run(self) -> None:
        """tests the function that fetches the owners of 
        generating units from reporting software
        """
        elemIds = [4, 6]
        ownersDict = getOwnersForGenUnitIds(self.appConfig['reportsConStr'], elemIds)
        expectedDict = {4: "ATIL-Adani", 6: "ATIL-Adani"}
        self.assertTrue(ownersDict == expectedDict)
