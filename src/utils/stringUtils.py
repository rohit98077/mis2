def extractVoltFromName(elemType: str, elemName: str) -> str:
    """extracts voltage level from element name of reporting software outage entry
    Args:
        elemType (str): element type, like TRANSFORMER, GENERATING_UNIT etc
        elemName (str): Name of the element
    Returns:
        str: voltage level
    """
    elemVoltLvl: str = ''
    try:
        if elemType in ['FSC', 'AC_TRANSMISSION_LINE_CIRCUIT', 'LINE_REACTOR']:
            # FSC like '400KV-APL-MUNDRA-SAMI-1 FSC@ SAMI'
            # ac trans line ckt like '132KV-BINA-MP-MORWA-1'
            # line reactor like '400KV-AKOLA-AURANGABAD-2 L/R@ AKOLA - 400KV'
            # find the index of 'KV-' to get the voltage level string
            kvInd = elemName.index('KV-')
            elemVoltLvl = elemName[0:kvInd+2]
        elif elemType == 'HVDC_LINE_CIRCUIT':
            # HVDC400KV-Vindyachal(PS)-RIHAND-1
            # find the index of 'KV-' to get the voltage level string
            kvInd = elemName.index('KV-')
            elemVoltLvl = elemName[4:kvInd+2]
        elif elemType == 'BUS REACTOR':
            # AKOLA (2) - 765KV B/R 1
            # find the index of ' - ', 'KV B/R' to get the voltage level string
            kvStartInd = elemName.index(' - ') + 3
            kvInd = elemName.index('KV B/R')
            elemVoltLvl = elemName[kvStartInd:kvInd+2]
        elif elemType == 'TRANSFORMER':
            # 1200KV/400KV BINA-ICT-1
            # find the index of 'KV ' to get the voltage level string
            kvInd = elemName.index('KV ')
            elemVoltLvl = elemName[0:kvInd+2]
        elif elemType == 'HVDC POLE':
            # HVDC 500KV APL  POLE 1
            # find the index of ' ', 'KV ' to get the voltage level string
            kvStartInd = elemName.index(' ') + 1
            kvInd = elemName.index('KV ')
            elemVoltLvl = elemName[kvStartInd:kvInd+2]
        elif elemType == 'BUS':
            # ACBIL - 400KV - BUS 2
            # find the index of ' - ', 'KV ' to get the voltage level string
            kvStartInd = elemName.index(' - ') + 3
            kvInd = elemName.index('KV ')
            elemVoltLvl = elemName[kvStartInd:kvInd+2]
        elif elemType == 'Bay':
            # MAIN BAY - 765KV/400KV BHOPAL-ICT-1 AND BHOPAL - 765KV - BUS 2 at BHOPAL - 765KV
            # split the name by ' AND '
            nameSegs = elemName.split(' AND ')
            reqElName = nameSegs[0]
            # if first element has '/' in it, then extract from second element
            if '/' in reqElName:
                reqElName = nameSegs[1]
            # find the index of 'KV-' or 'KV '
            if 'KV-' in reqElName:
                kvEndInd = reqElName.index('KV-')
            elif 'KV ' in reqElName:
                kvEndInd = reqElName.index('KV ')
            # go back from KV till numbers end to find kv start
            kvStartInd = kvEndInd-1
            while kvStartInd > 0:
                if not(reqElName[kvStartInd-1] in [str(x) for x in range(0, 10)]+['.']):
                    break
                kvStartInd += -1
            elemVoltLvl = reqElName[kvStartInd:kvEndInd+2]
        elif elemType in ['TCSC', 'MSR', 'MSC', 'STATCOM']:
            # AURANGABAD - 400KV - BUS 2 MSR@AURANGABAD
            # find the index of 'KV-' or 'KV - ' or 'KV '
            if 'KV-' in elemName:
                kvEndInd = elemName.index('KV-')
            elif 'KV - ' in elemName:
                kvEndInd = elemName.index('KV - ')
            elif 'KV ' in elemName:
                kvEndInd = elemName.index('KV ')
            # go back from KV till numbers end to find kv start
            kvStartInd = kvEndInd-1
            while kvStartInd > 0:
                if not(elemName[kvStartInd-1] in [str(x) for x in range(0, 10)]+['.']):
                    break
                kvStartInd += -1
            elemVoltLvl = elemName[kvStartInd:kvEndInd+2]
    except:
        elemVoltLvl = 'NA'
    return elemVoltLvl
