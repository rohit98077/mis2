-- get owner names for given BUS id
SELECT bus.id,
    bus.bus_name,
    owner_details.owners
FROM reporting_web_ui_uat.bus bus
    left join REPORTING_WEB_UI_UAT.associate_substation subs on subs.id = bus.fk_substation_id
    LEFT JOIN (
        SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(
                ORDER BY owner_name
            ) AS owners,
            parent_entity_attribute_id AS element_id
        FROM reporting_web_ui_uat.entity_entity_reln ent_reln
            LEFT JOIN reporting_web_ui_uat.owner own ON own.id = ent_reln.child_entity_attribute_id
        WHERE ent_reln.child_entity = 'OWNER'
            AND ent_reln.parent_entity = 'ASSOCIATE_SUBSTATION'
            AND ent_reln.child_entity_attribute = 'OwnerId'
            AND ent_reln.parent_entity_attribute = 'Owner'
        GROUP BY parent_entity_attribute_id
    ) owner_details ON owner_details.element_id = subs.id