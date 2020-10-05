-- get owner names for given generating units
SELECT gen_unit.id,
    gen_unit.unit_name,
    owner_details.owners
FROM reporting_web_ui_uat.generating_unit gen_unit
    LEFT JOIN reporting_web_ui_uat.generating_station gen_stn ON gen_stn.id = gen_unit.fk_generating_station
    LEFT JOIN (
        SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(
                ORDER BY owner_name
            ) AS owners,
            parent_entity_attribute_id AS element_id
        FROM reporting_web_ui_uat.entity_entity_reln ent_reln
            LEFT JOIN reporting_web_ui_uat.owner own ON own.id = ent_reln.child_entity_attribute_id
        WHERE ent_reln.child_entity = 'Owner'
            AND ent_reln.parent_entity = 'GENERATING_STATION'
            AND ent_reln.child_entity_attribute = 'OwnerId'
            AND ent_reln.parent_entity_attribute = 'Owner'
        GROUP BY parent_entity_attribute_id
    ) owner_details ON owner_details.element_id = gen_stn.id
where gen_unit.id in (4, 6)