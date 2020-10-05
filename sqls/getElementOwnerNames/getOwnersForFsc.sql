-- get owner names for given fsc
SELECT fsc.id,
    fsc.fsc_name,
    owner_details.owners
FROM reporting_web_ui_uat.fsc fsc
    LEFT JOIN (
        SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(
                ORDER BY owner_name
            ) AS owners,
            parent_entity_attribute_id AS element_id
        FROM reporting_web_ui_uat.entity_entity_reln ent_reln
            LEFT JOIN reporting_web_ui_uat.owner own ON own.id = ent_reln.child_entity_attribute_id
        WHERE ent_reln.child_entity = 'OWNER'
            AND ent_reln.parent_entity = 'FSC'
            AND ent_reln.child_entity_attribute = 'OwnerId'
            AND ent_reln.parent_entity_attribute = 'Owner'
        GROUP BY parent_entity_attribute_id
    ) owner_details ON owner_details.element_id = fsc.id
WHERE fsc.id = 4