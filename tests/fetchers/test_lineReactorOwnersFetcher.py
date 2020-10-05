import unittest
from src.fetchers.lineReactorOwnersFetcher import getOwnersForLineReactorIds
import datetime as dt
from src.appConfig import getConfig


class TestLineReactorOwnersFetcher(unittest.TestCase):
    appConfig: dict = {}

    def setUp(self):
        self.appConfig = getConfig()

    def test_run(self) -> None:
        """tests the function that fetches the owners of 
        LineReactors from reporting software
        """
        elemIds = [63,65]
        ownersDict = getOwnersForLineReactorIds(
            self.appConfig['reportsConStr'], elemIds)
        expectedDict = {
            63: "BDTCL-Sterlite", 65: "NTPC"}
        self.assertTrue(ownersDict == expectedDict)
