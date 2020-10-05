-- get owner names for given AC trans line ckts
select ckt.id as ckt_id,
    ac_line.line_name,
    owner_details.owners
from REPORTING_WEB_UI_UAT.ac_transmission_line_circuit ckt
    left join REPORTING_WEB_UI_UAT.ac_trans_line_master ac_line on ckt.line_id = ac_line.id
    left join (
        select LISTAGG(own.owner_name, ',') WITHIN GROUP (
                ORDER BY owner_name
            ) AS owners,
            parent_entity_attribute_id as element_id
        from REPORTING_WEB_UI_UAT.entity_entity_reln ent_reln
            left join REPORTING_WEB_UI_UAT.owner own on own.id = ent_reln.child_entity_attribute_id
        where ent_reln.CHILD_ENTITY = 'OWNER'
            and ent_reln.parent_entity = 'AC_TRANSMISSION_LINE'
            and ent_reln.CHILD_ENTITY_ATTRIBUTE = 'OwnerId'
            and ent_reln.PARENT_ENTITY_ATTRIBUTE = 'Owner'
        group by parent_entity_attribute_id
    ) owner_details on owner_details.element_id = ac_line.id
where ckt.id in (27, 1)