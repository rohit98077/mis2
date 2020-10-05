from typing import Dict,List
import cx_Oracle
def getOwnersForAcTransLineCktIds(reportsConnStr: str, ids: List[int]) -> Dict[int, str]:
    """fetches the owner names for a given list of AC Transmission line ckt ids
    Args:
        reportsConnStr (str): connection string to reports database
        ids (List[int]): list of AC Transmission line ckt ids
    Returns:
        Dict[int, str]: keys will be element Ids, values will be comma separated owner names
    """
    if len(ids) == 0:
        return {}
    # requiredIds in tuple list form
    reqIdsTxt = ','.join(tuple(set([str(x) for x in ids])))
    con = cx_Oracle.connect(reportsConnStr)

    fetchSql = '''SELECT ckt.id as ckt_id,owner_details.owners
    from REPORTING_WEB_UI_UAT.ac_transmission_line_circuit ckt
    left join REPORTING_WEB_UI_UAT.ac_trans_line_master ac_line on ckt.line_id = ac_line.id
    left join ( select LISTAGG(own.owner_name, ',') WITHIN GROUP (ORDER BY owner_name) AS owners,parent_entity_attribute_id as element_id
                from REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln 
                left join REPORTING_WEB_UI_UAT.owner own on own.id = ent_reln.child_entity_attribute_id
                where ent_reln.CHILD_ENTITY = 'OWNER' and ent_reln.parent_entity = 'AC_TRANSMISSION_LINE'
                and ent_reln.CHILD_ENTITY_ATTRIBUTE = 'OwnerId' and ent_reln.PARENT_ENTITY_ATTRIBUTE = 'Owner'
                group by parent_entity_attribute_id
                ) owner_details 
    on owner_details.element_id = ac_line.id 
    where ckt.id in ({0})'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict: Dict[int, str] = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForBayIds(reportsConnStr: str, ids: List[int]) -> Dict[int, str]:
    """fetches the owner names for a given list of Bay ids
    Args:
        reportsConnStr (str): connection string to reports database
        ids (List[int]): list of Bay ids
    Returns:
        Dict[int, str]: keys will be element Ids, values will be comma separated owner names
    """
    if len(ids) == 0:
        return {}
    # requiredIds in tuple list form
    reqIdsTxt = ','.join(tuple(set([str(x) for x in ids])))
    con = cx_Oracle.connect(reportsConnStr)

    fetchSql = '''SELECT bay.id,owner_details.owners 
    FROM REPORTING_WEB_UI_UAT.bay bay
    LEFT JOIN ( SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(ORDER BY owner_name) AS owners,parent_entity_attribute_id AS element_id
                FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                WHERE ent_reln.child_entity = 'OWNER' AND ent_reln.parent_entity = 'BAY'
                AND ent_reln.child_entity_attribute = 'OwnerId' AND ent_reln.parent_entity_attribute = 'Owner'
                GROUP BY parent_entity_attribute_id
                ) owner_details 
    ON owner_details.element_id = bay.id
    where bay.id in ({0})'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict: Dict[int, str] = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForBusIds(reportsConnStr: str, ids: List[int]) -> Dict[int, str]:
    """fetches the owner names for a given list of Bus ids
    Args:
        reportsConnStr (str): connection string to reports database
        ids (List[int]): list of Bus ids
    Returns:
        Dict[int, str]: keys will be element Ids, values will be comma separated owner names
    """
    if len(ids) == 0:
        return {}
    # requiredIds in tuple list form
    reqIdsTxt = ','.join(tuple(set([str(x) for x in ids])))
    con = cx_Oracle.connect(reportsConnStr)

    fetchSql = '''SELECT bus.id,owner_details.owners 
    FROM REPORTING_WEB_UI_UAT.bus bus
    left join REPORTING_WEB_UI_UAT.associate_substation subs on subs.id = bus.fk_substation_id 
    LEFT JOIN ( SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(ORDER BY owner_name) AS owners,parent_entity_attribute_id AS element_id
                FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                WHERE ent_reln.child_entity = 'OWNER' AND ent_reln.parent_entity = 'ASSOCIATE_SUBSTATION'
                AND ent_reln.child_entity_attribute = 'OwnerId' AND ent_reln.parent_entity_attribute = 'Owner'
                GROUP BY parent_entity_attribute_id
                ) owner_details 
    ON owner_details.element_id = subs.id
    where bus.id in ({0})'''.format(reqIdsTxt)

    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict: Dict[int, str] = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForBusReactorIds(reportsConnStr: str, ids: List[int]) -> Dict[int, str]:
    """fetches the owner names for a given list of BusReactor ids
    Args:
        reportsConnStr (str): connection string to reports database
        ids (List[int]): list of BusReactor ids
    Returns:
        Dict[int, str]: keys will be element Ids, values will be comma separated owner names
    """
    if len(ids) == 0:
        return {}
    # requiredIds in tuple list form
    reqIdsTxt = ','.join(tuple(set([str(x) for x in ids])))
    con = cx_Oracle.connect(reportsConnStr)

    fetchSql = '''SELECT bus_reactor.id,owner_details.owners 
    FROM REPORTING_WEB_UI_UAT.bus_reactor bus_reactor
    LEFT JOIN ( SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(ORDER BY owner_name) AS owners,parent_entity_attribute_id AS element_id
                FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                WHERE ent_reln.child_entity = 'OWNER' AND ent_reln.parent_entity = 'BUS_REACTOR'
                AND ent_reln.child_entity_attribute = 'OwnerId' AND ent_reln.parent_entity_attribute = 'Owner'
                GROUP BY parent_entity_attribute_id
                ) owner_details 
    ON owner_details.element_id = bus_reactor.id
    where bus_reactor.id in ({0})'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict: Dict[int, str] = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForCompensatorIds(reportsConnStr: str, ids: List[int]) -> Dict[int, str]:
    """fetches the owner names for a given list of Compensator ids
    Args:
        reportsConnStr (str): connection string to reports database
        ids (List[int]): list of Compensator ids
    Returns:
        Dict[int, str]: keys will be element Ids, values will be comma separated owner names
    """
    if len(ids) == 0:
        return {}
    # requiredIds in tuple list form
    reqIdsTxt = ','.join(tuple(set([str(x) for x in ids])))
    con = cx_Oracle.connect(reportsConnStr)

    fetchSql = '''SELECT tcsc.id,owner_details.owners 
    FROM REPORTING_WEB_UI_UAT.tcsc tcsc
    LEFT JOIN ( SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(ORDER BY owner_name) AS owners,parent_entity_attribute_id AS element_id
                FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                WHERE ent_reln.child_entity = 'OWNER' AND ent_reln.parent_entity IN ('STATCOM','TCSC','MSR','MSC')
                AND ent_reln.child_entity_attribute = 'OwnerId' AND ent_reln.parent_entity_attribute = 'Owner'
                GROUP BY parent_entity_attribute_id
                ) owner_details 
    ON owner_details.element_id = tcsc.id
    where tcsc.id in ({0})'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict: Dict[int, str] = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForFscIds(reportsConnStr: str, ids: List[int]) -> Dict[int, str]:
    """fetches the owner names for a given list of Fsc ids
    Args:
        reportsConnStr (str): connection string to reports database
        ids (List[int]): list of Fsc ids
    Returns:
        Dict[int, str]: keys will be element Ids, values will be comma separated owner names
    """
    if len(ids) == 0:
        return {}
    # requiredIds in tuple list form
    reqIdsTxt = ','.join(tuple(set([str(x) for x in ids])))
    con = cx_Oracle.connect(reportsConnStr)
    
    fetchSql = '''SELECT fsc.id,owner_details.owners 
    FROM REPORTING_WEB_UI_UAT.fsc fsc
    LEFT JOIN (SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(ORDER BY owner_name) AS owners,parent_entity_attribute_id AS element_id
                FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                WHERE ent_reln.child_entity = 'OWNER' AND ent_reln.parent_entity = 'FSC'
                AND ent_reln.child_entity_attribute = 'OwnerId' AND ent_reln.parent_entity_attribute = 'Owner'
                GROUP BY parent_entity_attribute_id
                ) owner_details 
    ON owner_details.element_id = fsc.id
    where fsc.id in ({0})'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict: Dict[int, str] = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForGenUnitIds(reportsConnStr: str, ids: List[int]) -> Dict[int, str]:
    """fetches the owner names for a given list of generating unit ids
    Args:
        reportsConnStr (str): connection string to reports database
        ids (List[int]): list of generating unit ids
    Returns:
        Dict[int, str]: keys will be element Ids, values will be comma separated owner names
    """
    if len(ids) == 0:
        return {}
    # requiredIds in tuple list form
    reqIdsTxt = ','.join(tuple(set([str(x) for x in ids])))
    con = cx_Oracle.connect(reportsConnStr)

    fetchSql = '''SELECT gen_unit.id,owner_details.owners 
    FROM REPORTING_WEB_UI_UAT.generating_unit gen_unit
    LEFT JOIN REPORTING_WEB_UI_UAT.generating_station gen_stn ON gen_stn.id = gen_unit.fk_generating_station
    LEFT JOIN (SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(ORDER BY owner_name) AS owners,parent_entity_attribute_id AS element_id
                FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                WHERE ent_reln.child_entity = 'Owner' AND ent_reln.parent_entity = 'GENERATING_STATION'
                AND ent_reln.child_entity_attribute = 'OwnerId' AND ent_reln.parent_entity_attribute = 'Owner'
                GROUP BY parent_entity_attribute_id
                ) owner_details 
    ON owner_details.element_id = gen_stn.id
    where gen_unit.id in ({0})'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict: Dict[int, str] = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForHvdcLineCktIds(reportsConnStr: str, ids: List[int]) -> Dict[int, str]:
    """fetches the owner names for a given list of HvdcLineCkt ids
    Args:
        reportsConnStr (str): connection string to reports database
        ids (List[int]): list of HvdcLineCkt ids
    Returns:
        Dict[int, str]: keys will be element Ids, values will be comma separated owner names
    """
    if len(ids) == 0:
        return {}
    # requiredIds in tuple list form
    reqIdsTxt = ','.join(tuple(set([str(x) for x in ids])))
    con = cx_Oracle.connect(reportsConnStr)
    
    fetchSql = '''SELECT hvdc_ckt.id, owner_details.owners 
    FROM REPORTING_WEB_UI_UAT.hvdc_line_circuit hvdc_ckt
    LEFT JOIN (SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(ORDER BY owner_name) AS owners,parent_entity_attribute_id AS element_id
                FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                WHERE ent_reln.child_entity = 'OWNER' AND ent_reln.parent_entity = 'HVDC_LINE'
                AND ent_reln.child_entity_attribute = 'OwnerId' AND ent_reln.parent_entity_attribute = 'Owner'
                GROUP BY parent_entity_attribute_id
                ) owner_details 
    ON owner_details.element_id = hvdc_ckt.id
    where hvdc_ckt.id in ({0})'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict: Dict[int, str] = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForHvdcPoleIds(reportsConnStr: str, ids: List[int]) -> Dict[int, str]:
    """fetches the owner names for a given list of HvdcPole ids
    Args:
        reportsConnStr (str): connection string to reports database
        ids (List[int]): list of HvdcPole ids
    Returns:
        Dict[int, str]: keys will be element Ids, values will be comma separated owner names
    """
    if len(ids) == 0:
        return {}
    # requiredIds in tuple list form
    reqIdsTxt = ','.join(tuple(set([str(x) for x in ids])))
    con = cx_Oracle.connect(reportsConnStr)

    fetchSql = '''SELECT hvdc_pole.id,owner_details.owners 
    FROM REPORTING_WEB_UI_UAT.hvdc_pole hvdc_pole
    LEFT JOIN (SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(ORDER BY owner_name) AS owners,parent_entity_attribute_id AS element_id
                FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                WHERE ent_reln.child_entity = 'OWNER' AND ent_reln.parent_entity = 'HVDC_POLE'
                AND ent_reln.child_entity_attribute = 'OwnerId' AND ent_reln.parent_entity_attribute = 'Owner'
                GROUP BY parent_entity_attribute_id
                ) owner_details 
    ON owner_details.element_id = hvdc_pole.id
    where hvdc_pole.id in ({0})'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict: Dict[int, str] = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForLineReactorIds(reportsConnStr: str, ids: List[int]) -> Dict[int, str]:
    """fetches the owner names for a given list of LineReactor ids
    Args:
        reportsConnStr (str): connection string to reports database
        ids (List[int]): list of LineReactor ids
    Returns:
        Dict[int, str]: keys will be element Ids, values will be comma separated owner names
    """
    if len(ids) == 0:
        return {}
    # requiredIds in tuple list form
    reqIdsTxt = ','.join(tuple(set([str(x) for x in ids])))
    con = cx_Oracle.connect(reportsConnStr)

    fetchSql = '''SELECT line_reactor.id,owner_details.owners
    FROM REPORTING_WEB_UI_UAT.line_reactor line_reactor
    LEFT JOIN (SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(ORDER BY owner_name) AS owners,parent_entity_attribute_id AS element_id
                FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                WHERE ent_reln.child_entity = 'OWNER' AND ent_reln.parent_entity = 'LINE_REACTOR'
                AND ent_reln.child_entity_attribute = 'OwnerId' AND ent_reln.parent_entity_attribute = 'Owner'
                GROUP BY parent_entity_attribute_id
                ) owner_details 
    ON owner_details.element_id = line_reactor.id
    where line_reactor.id in ({0})'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict: Dict[int, str] = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict

def getOwnersForTransformerIds(reportsConnStr: str, ids: List[int]) -> Dict[int, str]:
    """fetches the owner names for a given list of Transformer ids
    Args:
        reportsConnStr (str): connection string to reports database
        ids (List[int]): list of Transformer ids
    Returns:
        Dict[int, str]: keys will be element Ids, values will be comma separated owner names
    """
    if len(ids) == 0:
        return {}
    # requiredIds in tuple list form
    reqIdsTxt = ','.join(tuple(set([str(x) for x in ids])))
    con = cx_Oracle.connect(reportsConnStr)

    fetchSql = '''SELECT transformer.id,owner_details.owners
    FROM REPORTING_WEB_UI_UAT.transformer transformer
    LEFT JOIN ( SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(ORDER BY owner_name) AS owners,parent_entity_attribute_id AS element_id
                FROM REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
                LEFT JOIN REPORTING_WEB_UI_UAT.owner own ON own.id = ent_reln.child_entity_attribute_id
                WHERE ent_reln.child_entity = 'OWNER' AND ent_reln.parent_entity = 'TRANSFORMER'
                AND ent_reln.child_entity_attribute = 'OwnerId' AND ent_reln.parent_entity_attribute = 'Owner'
                GROUP BY parent_entity_attribute_id
                ) owner_details 
    ON owner_details.element_id = transformer.id
    where transformer.id in ({0})'''.format(reqIdsTxt)
    
    cur = con.cursor()
    cur.execute(fetchSql, [])
    dbRows = cur.fetchall()
    ownersDict: Dict[int, str] = {}
    for row in dbRows:
        ownersDict[row[0]] = row[1]
    return ownersDict
