from typing import List, Tuple, TypedDict
import cx_Oracle

class Outages(TypedDict):
    columns: List[str]
    rows: List[Tuple]

class OutagesRepo():
    """Repository class for outages data of application"""
    localConStr: str = ""
    def __init__(self, dbConStr: str) -> None:
        self.localConStr = dbConStr
    def insertOutages(self, outages: Outages) -> bool:
        """inserts outages into the app db
        Args:
            outages (Outages): outages to be inserted
        Returns:
            bool: returns true if process is ok"""
        # get connection with raw data table
        conLocal = cx_Oracle.connect(self.localConStr)
        isInsertSuccess = True
        try:
            # column names of the raw data table
            colNames = outages['columns']
            curLocal = conLocal.cursor()
            # text for sql place holders
            sqlPlceHldrsTxt = ','.join([':{0}'.format(x+1) for x in range(len(colNames))])
            # delete the rows which are already present
            pwcIdColInd = colNames.index('PWC_ID')
            pwcIds = [(x[pwcIdColInd],) for x in outages['rows']]
            curLocal.executemany("delete from outage_events where PWC_ID=:1", pwcIds)
            # insert the raw data
            ouatageEvntsInsSql = 'insert into outage_events({0}) values ({1})'.format(','.join(colNames), sqlPlceHldrsTxt)
            curLocal.executemany(ouatageEvntsInsSql, outages['rows'])
            conLocal.commit()
        except Exception as e:
            isInsertSuccess = False
            print('Error while bulk insertion of outage events into raw data db')
            print(e)
        finally:
            if curLocal is not None:
                curLocal.close()
            conLocal.close()
        return isInsertSuccess
