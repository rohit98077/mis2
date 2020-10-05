select MIN(rto.ELEMENTNAME),
    ent_master.ENTITY_NAME
from reporting_web_ui_uat.real_time_outage rto
    left join reporting_web_ui_uat.entity_master ent_master on ent_master.id = rto.ENTITY_ID
group by ent_master.entity_name

/*
Output would be like
MIN(RTO.ELEMENTNAME)                                                                                                                                                                                     ENTITY_NAME                                       
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- --------------------------------------------------
ACBIL (Chakabura)  - UNIT 2                                                                                                                                                                              GENERATING_UNIT                                   
400KV-APL-MUNDRA-SAMI-1 FSC@ SAMI                                                                                                                                                                        FSC                                               
HVDC400KV-Vindyachal(PS)-RIHAND-1                                                                                                                                                                        HVDC_LINE_CIRCUIT                                 
AURANGABAD - 400KV - BUS 2 MSR@AURANGABAD                                                                                                                                                                TCSC                                              
AKOLA (2) - 765KV B/R 1                                                                                                                                                                                  BUS REACTOR                                       
400KV-AKOLA-AURANGABAD-2 L/R@ AKOLA - 400KV                                                                                                                                                              LINE_REACTOR                                      
132KV-BINA-MP-MORWA-1                                                                                                                                                                                    AC_TRANSMISSION_LINE_CIRCUIT                      
1200KV/400KV BINA-ICT-1                                                                                                                                                                                  TRANSFORMER                                       
HVDC 500KV APL  POLE 1                                                                                                                                                                                   HVDC POLE                                         
ACBIL - 400KV - BUS 2                                                                                                                                                                                    BUS                                               
MAIN BAY - 765KV/400KV BHOPAL-ICT-1 AND BHOPAL - 765KV - BUS 2 at BHOPAL - 765KV                                                                                                                         Bay                                               
*/