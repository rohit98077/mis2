-- get owner names for element
SELECT LISTAGG(own.owner_name, ',') WITHIN GROUP(
        ORDER BY owner_name
    ) AS owners,
    parent_entity AS entity_name,
    parent_entity_attribute_id AS element_id
FROM reporting_web_ui_uat.entity_entity_reln ent_reln
    LEFT JOIN reporting_web_ui_uat.owner own ON own.id = ent_reln.child_entity_attribute_id
WHERE ent_reln.child_entity = 'OWNER'
    AND ent_reln.child_entity_attribute = 'OwnerId'
    AND ent_reln.parent_entity_attribute = 'Owner'
GROUP BY parent_entity,
    parent_entity_attribute_id