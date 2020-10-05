import unittest
from src.fetchers.transformerOwnersFetcher import getOwnersForTransformerIds
import datetime as dt
from src.appConfig import getConfig


class TestTransformerOwnersFetcher(unittest.TestCase):
    appConfig: dict = {}

    def setUp(self):
        self.appConfig = getConfig()

    def test_run(self) -> None:
        """tests the function that fetches the owners of 
        Transformers from reporting software
        """
        elemIds = [28, 31]
        ownersDict = getOwnersForTransformerIds(
            self.appConfig['reportsConStr'], elemIds)
        expectedDict = {28: "Maharashtra", 31: "POWERGRID-WR2 (PGCIL)"}
        self.assertTrue(ownersDict == expectedDict)
