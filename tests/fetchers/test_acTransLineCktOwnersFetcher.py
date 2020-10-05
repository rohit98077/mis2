import unittest
from src.fetchers.acTransLineCktOwnersFetcher import getOwnersForAcTransLineCktIds
import datetime as dt
from src.appConfig import getConfig


class TestAcTransLineOwnersFetcher(unittest.TestCase):
    appConfig: dict = {}

    def setUp(self):
        self.appConfig = getConfig()

    def test_run(self) -> None:
        """tests the function that fetches the owners of 
        AC Transmission line Circuits from reporting software
        """
        elemIds = [1, 27]
        ownersDict = getOwnersForAcTransLineCktIds(
            self.appConfig['reportsConStr'], elemIds)
        expectedDict = {1: "Maharashtra", 27: "Chattisgarh"}
        self.assertTrue(ownersDict == expectedDict)
