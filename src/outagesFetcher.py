import datetime as dt
import cx_Oracle
import pandas as pd
from src.repos.outagesRepo import Outages
from src.utils.timeUtils import getTimeDeltaFromDbStr
from src.utils.stringUtils import extractVoltFromName
from src.getOwners import getOwnersForAcTransLineCktIds
from src.getOwners import getOwnersForBayIds
from src.getOwners import getOwnersForBusIds
from src.getOwners import getOwnersForBusReactorIds
from src.getOwners import getOwnersForCompensatorIds
from src.getOwners import getOwnersForFscIds
from src.getOwners import getOwnersForGenUnitIds
from src.getOwners import getOwnersForHvdcLineCktIds
from src.getOwners import getOwnersForHvdcPoleIds
from src.getOwners import getOwnersForLineReactorIds
from src.getOwners import getOwnersForTransformerIds

def fetchOutages(appConfig: dict, startDate: dt.datetime, endDate: dt.datetime) -> Outages:
    """fetches outages from reports database
    Args:
        appConfig (dict): application configuration
        startDate (dt.datetime): start date
        endDate (dt): end date
    Returns:
        Outages: Each tuple will have the following attributes column names
        'PWC_ID', 'ELEMENT_ID', 'ELEMENT_NAME', 'ENTITY_ID', 'ENTITY_NAME','CAPACITY', 'OUTAGE_DATETIME', 'REVIVED_DATETIME', 
        'CREATED_DATETIME', 'MODIFIED_DATETIME', 'SHUTDOWN_TAG','SHUTDOWN_TAG_ID', 'SHUTDOWN_TYPENAME', 'SHUT_DOWN_TYPE_ID', 
        'OUTAGE_REMARKS', 'REASON', 'REASON_ID', 'REVIVAL_REMARKS','REGION_ID', 'SHUTDOWNREQUEST_ID', 'OWNERS'
    """
    reportsConnStr = appConfig['reportsConStr']
    con = cx_Oracle.connect(reportsConnStr)
    # sql query to fetch the outages
    outagesFetchSql = '''
    SELECT rto.ID as pwc_id,
    rto.ELEMENT_ID,
    rto.ELEMENTNAME as ELEMENT_NAME,
    rto.ENTITY_ID,
    ent_master.ENTITY_NAME,
    gen_unit.installed_capacity as CAPACITY,
    rto.OUTAGE_DATE as OUTAGE_DATETIME,
    rto.REVIVED_DATE as REVIVED_DATETIME,
    rto.CREATED_DATE as CREATED_DATETIME,
    rto.MODIFIED_DATE as MODIFIED_DATETIME,
    sd_tag.name as shutdown_tag,
    rto.SHUTDOWN_TAG_ID,
    sd_type.name as shutdown_typename,
    rto.SHUT_DOWN_TYPE as SHUT_DOWN_TYPE_ID,
    rto.OUTAGE_REMARKS,
    rto.REASON_ID,
    reas.reason,
    rto.REVIVAL_REMARKS,
    rto.REGION_ID,
    rto.SHUTDOWNREQUEST_ID,
    rto.OUTAGE_TIME,
    rto.REVIVED_TIME
    from REPORTING_WEB_UI_UAT.real_time_outage rto 
    left join REPORTING_WEB_UI_UAT.outage_reason reas on reas.id = rto.reason_id
    left join REPORTING_WEB_UI_UAT.shutdown_outage_tag sd_tag on sd_tag.id = rto.shutdown_tag_id
    left join REPORTING_WEB_UI_UAT.shutdown_outage_type sd_type on sd_type.id = rto.shut_down_type
    left join REPORTING_WEB_UI_UAT.entity_master ent_master on ent_master.id = rto.ENTITY_ID
    left join REPORTING_WEB_UI_UAT.generating_unit gen_unit on gen_unit.id = rto.element_id 
    where (rto.OUTAGE_DATE between :1 and :2) or (rto.revived_date between :1 and :2) 
    or (rto.MODIFIED_DATE between :1 and :2) or (rto.CREATED_DATE between :1 and :2)'''
    cur = con.cursor()
    cur.execute(outagesFetchSql, (startDate, endDate))
    colNames = [row[0] for row in cur.description]
    colNames = colNames[0:-2]
    colNames.append('OWNERS')
    dbRows = cur.fetchall()
    # print(dbRows)
    instCapIndex: int = 5
    outDateIndex: int = 6
    revDateIndex: int = 7
    elemIdIndex: int = 1
    elemIdNameIndex: int = 2
    elemTypeIndex: int = 4

    # initialize owners list
    acTransLineCktOwners=[]
    bayOwners=[]
    busOwners=[]
    busReactorOwners=[]
    compensatorOwners=[]
    fscOwners=[]
    genUnitOwners=[]
    hvdcLineCktOwners=[]
    hvdcPoleOwners=[]
    lineReactorOwners=[]
    transfomerOwners=[]

    # iterate through db rows
    for rIter in range(len(dbRows)):
        # convert tuple to list to facilitate manipulation
        dbRows[rIter] = list(dbRows[rIter])
        # get the element Id and element type of outage entry
        elemName = dbRows[rIter][elemIdNameIndex]
        elemId = dbRows[rIter][elemIdIndex]
        elemType = dbRows[rIter][elemTypeIndex]
        if elemType == 'AC_TRANSMISSION_LINE_CIRCUIT':
            acTransLineCktOwners.append(elemId)
        elif elemType == 'GENERATING_UNIT':
            genUnitOwners.append(elemId)
        elif elemType == 'FSC':
            fscOwners.append(elemId)
        elif elemType == 'HVDC_LINE_CIRCUIT':
            hvdcLineCktOwners.append(elemId)
        elif elemType == 'BUS REACTOR':
            busReactorOwners.append(elemId)
        elif elemType == 'LINE_REACTOR':
            lineReactorOwners.append(elemId)
        elif elemType == 'TRANSFORMER':
            transfomerOwners.append(elemId)
        elif elemType == 'HVDC POLE':
            hvdcPoleOwners.append(elemId)
        elif elemType == 'BUS':
            busOwners.append(elemId)
        elif elemType == 'Bay':
            bayOwners.append(elemId)
        elif elemType in ['TCSC', 'MSR', 'MSC', 'STATCOM']:
            compensatorOwners.append(elemId)
        # convert installed capacity to string
        instCap = dbRows[rIter][instCapIndex]
        if elemType == 'GENERATING_UNIT':
            instCap = str(int(instCap))
        else:
            instCap = extractVoltFromName(elemType, elemName)
        dbRows[rIter][instCapIndex] = instCap

        outageDateTime = dbRows[rIter][outDateIndex]
        if not pd.isnull(outageDateTime):
            outTimeStr = dbRows[rIter][-2]
            # strip off hours and minute components
            outageDateTime = outageDateTime.replace(hour=0, minute=0, second=0, microsecond=0)
            outageDateTime += getTimeDeltaFromDbStr(outTimeStr)
            dbRows[rIter][outDateIndex] = outageDateTime

        revivalDateTime = dbRows[rIter][revDateIndex]
        if not pd.isnull(revivalDateTime):
            revTimeStr = dbRows[rIter][-1]
            # strip off hours and minute components
            revivalDateTime = revivalDateTime.replace(hour=0, minute=0, second=0, microsecond=0)
            revivalDateTime += getTimeDeltaFromDbStr(revTimeStr)
            dbRows[rIter][revDateIndex] = revivalDateTime
        # remove last 2 column of the row
        dbRows[rIter] = dbRows[rIter][0:-2]

    # fetch owners for each type separately
    acTransLineCktOwners = getOwnersForAcTransLineCktIds(reportsConnStr, acTransLineCktOwners)
    bayOwners = getOwnersForBayIds(reportsConnStr, bayOwners)
    busOwners = getOwnersForBusIds(reportsConnStr, busOwners)
    busReactorOwners = getOwnersForBusReactorIds(reportsConnStr, busReactorOwners)
    compensatorOwners = getOwnersForCompensatorIds(reportsConnStr, compensatorOwners)
    fscOwners = getOwnersForFscIds(reportsConnStr, fscOwners)
    genUnitOwners = getOwnersForGenUnitIds(reportsConnStr, genUnitOwners)
    hvdcLineCktOwners = getOwnersForHvdcLineCktIds(reportsConnStr, hvdcLineCktOwners)
    hvdcPoleOwners = getOwnersForHvdcPoleIds(reportsConnStr, hvdcPoleOwners)
    lineReactorOwners = getOwnersForLineReactorIds(reportsConnStr, lineReactorOwners)
    transfomerOwners = getOwnersForTransformerIds(reportsConnStr, transfomerOwners)

    # iterate through db rows and assign owner string to each row
    for rIter in range(len(dbRows)):
        elemId = dbRows[rIter][elemIdIndex]
        elemType = dbRows[rIter][elemTypeIndex]
        if elemType == 'AC_TRANSMISSION_LINE_CIRCUIT':
            dbRows[rIter].append(acTransLineCktOwners[elemId])
        elif elemType == 'GENERATING_UNIT':
            dbRows[rIter].append(genUnitOwners[elemId])
        elif elemType == 'FSC':
            dbRows[rIter].append(fscOwners[elemId])
        elif elemType == 'HVDC_LINE_CIRCUIT':
            dbRows[rIter].append(hvdcLineCktOwners[elemId])
        elif elemType == 'BUS REACTOR':
            dbRows[rIter].append(busReactorOwners[elemId])
        elif elemType == 'LINE_REACTOR':
            dbRows[rIter].append(lineReactorOwners[elemId])
        elif elemType == 'TRANSFORMER':
            dbRows[rIter].append(transfomerOwners[elemId])
        elif elemType == 'HVDC POLE':
            dbRows[rIter].append(hvdcPoleOwners[elemId])
        elif elemType == 'BUS':
            dbRows[rIter].append(busOwners[elemId])
        elif elemType == 'Bay':
            dbRows[rIter].append(bayOwners[elemId])
        elif elemType in ['TCSC', 'MSR', 'MSC', 'STATCOM']:
            dbRows[rIter].append(compensatorOwners[elemId])
        # convert row to tuple
        dbRows[rIter] = tuple(dbRows[rIter])
    return {'columns': colNames, 'rows': dbRows}
