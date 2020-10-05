import unittest
from src.fetchers.outagesFetcher import fetchOutages
import datetime as dt
from src.appConfig import getConfig


class TestFetchOutages(unittest.TestCase):
    appConfig = None

    def setUp(self):
        self.appConfig = getConfig()

    def test_run(self) -> None:
        """tests the function that fetches the ouatges from reporting software
        """
        startDate = dt.datetime(2019, 10, 20)
        endDate = dt.datetime(2019, 10, 21)

        outages = fetchOutages(self.appConfig, startDate, endDate)
        targetColumns = ['PWC_ID', 'ELEMENT_ID', 'ELEMENT_NAME', 'ENTITY_ID',
                         'ENTITY_NAME', 'CAPACITY', 'OUTAGE_DATETIME', 'REVIVED_DATETIME',
                         'CREATED_DATETIME', 'MODIFIED_DATETIME', 'SHUTDOWN_TAG', 'SHUTDOWN_TAG_ID',
                         'SHUTDOWN_TYPENAME', 'SHUT_DOWN_TYPE_ID', 'OUTAGE_REMARKS', 'REASON',
                         'REASON_ID', 'REVIVAL_REMARKS', 'REGION_ID', 'SHUTDOWNREQUEST_ID', 'OWNERS']
        self.assertFalse(False in [(col in targetColumns)
                                   for col in outages['columns']])
        self.assertFalse(len(outages['rows']) == 0)
        self.assertTrue(len(outages['rows'][0]) == len(targetColumns))
