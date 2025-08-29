SELECT 
	ie.schema_identifier,
	lower(org.org_identifier),
    jsonb_build_object(
        'iri', ie.id,
        'schema_name', ie.schema_name,
        'schema_alternate_name', sa.schema_alternate_name,
        'schema_description', ie.schema_description,
        'schema_abstract', ie.schema_abstract,
        'schema_transcript', str.schema_transcript,
        'meemoo_description_cast', ie.ebucore_has_cast_member,
        'meemoo_description_programme', ie.ebucore_synopsis,
        'ebucore_object_type', ie.ebucore_has_object_type,
        'schema_identifier', ie.schema_identifier,
        'premis_identifier', pi.premis_identifier,
        'schema_maintainer', sm.schema_maintainer,
        'premis_is_part_of', ie.relation_is_part_of,
        'schema_is_part_of', ipo.schema_is_part_of,
        'schema_date_created', ie.ha_des_min_date_created,
        'schema_date_published', ie.ha_des_min_date_published,
        'dcterms_available', ie.dcterms_available,
        'dcterms_format', df.dcterms_format,
        'dcterms_medium', pm.premis_medium,
        'schema_duration', sd.schema_duration,
        'schema_thumbnail_url', thu.schema_thumbnail_url,
        'schema_creator', sr.schema_creator,
        'schema_contributor', sr.schema_contributor,
        'schema_publisher', sr.schema_publisher,
        'schema_creator_text', sct.schema_creator_array,
        'schema_publisher_text', spt.schema_publisher_array,
        'schema_spatial_coverage', ss.schema_spatial,
        'schema_temporal_coverage', st.schema_temporal,
        'schema_keywords', sk.schema_keywords,
        'schema_genre', sg.schema_genre,
        'schema_in_language', sil.schema_in_language,
        'meemoofilm_color', ct.ha_des_coloring_type,
        'schema_license', sl.schema_license,
        'dcterms_rights', ie.schema_copyright_notice,
        'audio', fha.audio,
        'schema_number_of_pages', ie.ha_des_number_of_pages,
        'schema_mentions', sme.schema_name,
        'dcterms_rights_statement', drs.dcterms_rights_statement,
        'schema_location_created', slc.schema_location_created,
        'children', iec.children,
        'is_deleted', mf.is_deleted
    ),
    mf.is_deleted,
    ie.updated_at
FROM graph.intellectual_entity ie
LEFT JOIN graph.schema_maintainer sm ON sm.id = ie.schema_maintainer
LEFT JOIN organization org
  ON org.id = ie.schema_maintainer
JOIN graph.dcterms_format df
	ON df.intellectual_entity_id = ie.id
 	AND df.dcterms_format NOT IN ('set', ' document', 'newspaperpage')
LEFT JOIN LATERAL (
    SELECT 
        jsonb_agg(json_build_object(pi.type, pi.value)) AS premis_identifier
    FROM graph.premis_identifier pi
    WHERE pi.intellectual_entity_id = ie.id
) pi ON true
LEFT JOIN LATERAL (
    SELECT MAX(d) AS schema_duration
    FROM (
        -- file-based durations
        SELECT file.schema_duration::double precision * interval '1 second' AS d
        FROM graph.file file
        JOIN graph.includes inc 
          ON inc.file_id = file.id
        JOIN graph.representation rep 
          ON rep.id = inc.representation_id
        WHERE rep.premis_represents = ie.id
          AND file.ebucore_has_mime_type = 'video/mp4'
          AND file.schema_duration IS NOT NULL
        UNION ALL
        -- media-fragment-based durations
        SELECT rep.schema_end_time - rep.schema_start_time AS d
        FROM graph.representation rep
        WHERE rep.premis_represents = ie.id
          AND rep.is_media_fragment_of IS NOT NULL
    ) all_durations
) sd ON true
-- thumbnails lateral
LEFT JOIN LATERAL (
    SELECT 
        jsonb_agg(DISTINCT thumbs.schema_thumbnail_url) AS schema_thumbnail_url,
        jsonb_agg(thumbs.object_type) AS object_type
    FROM (
        -- thumbnails from fragments
        SELECT rep.premis_represents AS intellectual_entity_id,
               f.schema_thumbnail_url,
               'fragment'::text AS object_type
        FROM graph.representation rep
        JOIN graph.file f 
          ON f.id = rep.is_media_fragment_of
         AND f.schema_thumbnail_url IS NOT NULL
        WHERE rep.premis_represents = ie.id
        UNION
        -- thumbnails from representations
        SELECT rep.premis_represents AS intellectual_entity_id,
               f.schema_thumbnail_url,
               'representation'::text AS object_type
        FROM graph.file f
        JOIN graph.includes inc 
          ON f.id = inc.file_id
         AND f.schema_thumbnail_url IS NOT NULL
        JOIN graph.representation rep 
          ON rep.id = inc.representation_id
         AND rep.is_media_fragment_of IS NULL
        WHERE rep.premis_represents = ie.id
        UNION
        -- thumbnails from first-position child
        SELECT first_position.intellectual_entity_id,
               first_position.schema_thumbnail_url,
               'child'::text AS object_type
        FROM (
            SELECT thie.relation_is_part_of AS intellectual_entity_id,
                   f.schema_thumbnail_url,
                   row_number() OVER (
                       PARTITION BY thie.relation_is_part_of 
                       ORDER BY thie.schema_position
                   ) AS rn
            FROM graph.intellectual_entity thie
            JOIN graph.representation rep 
              ON rep.premis_represents = thie.id
             AND thie.relation_is_part_of IS NOT NULL
            JOIN graph.includes inc 
              ON inc.representation_id = rep.id
            JOIN graph.file f 
              ON f.id = inc.file_id
             AND f.schema_thumbnail_url IS NOT NULL
            WHERE thie.relation_is_part_of = ie.id
        ) first_position
        WHERE first_position.rn = 1
    ) thumbs
) thu ON true
LEFT JOIN LATERAL (
    SELECT
        jsonb_agg(r.roles) FILTER (WHERE r.type = 'schema_contributor') AS schema_contributor,
        jsonb_agg(r.roles) FILTER (WHERE r.type = 'schema_creator') AS schema_creator,
        jsonb_agg(r.roles) FILTER (WHERE r.type = 'schema_publisher') AS schema_publisher
    FROM (
        SELECT
            sr_1.type,
            jsonb_build_object(sr_1.schema_role_name, array_agg(th.schema_name)) AS roles
        FROM graph.schema_role sr_1
        LEFT JOIN graph.thing th ON th.id = sr_1.thing_id
        WHERE sr_1.intellectual_entity_id = ie.id
        GROUP BY sr_1.type, sr_1.schema_role_name
    ) r
) sr ON true
LEFT JOIN LATERAL (
    SELECT 
        array_agg(thing.schema_name) AS schema_creator_array
   FROM graph.schema_role role
    LEFT JOIN graph.thing thing ON thing.id = role.thing_id
    WHERE role.type = 'schema_creator'::text
    and role.intellectual_entity_id = ie.id
) sct ON true
LEFT JOIN LATERAL (
    SELECT 
        array_agg(thing.schema_name) AS schema_publisher_array
   FROM graph.schema_role role
    LEFT JOIN graph.thing thing ON thing.id = role.thing_id
    WHERE role.type = 'schema_publisher'::text
    and role.intellectual_entity_id = ie.id
) spt ON true
left join lateral (
	SELECT
	    jsonb_agg(sp.schema_spatial) AS schema_spatial
 	FROM graph.schema_spatial sp
 	where sp.intellectual_entity_id = ie.id
) ss on true
left join lateral (
	SELECT 
		jsonb_agg(st.schema_temporal) AS schema_temporal
	FROM graph.schema_temporal st
	where st.intellectual_entity_id = ie.id
) st on true
LEFT JOIN LATERAL (
    SELECT 
        jsonb_agg(sk.schema_keywords) AS schema_keywords
   FROM graph.schema_keywords sk
    WHERE sk.intellectual_entity_id = ie.id
) sk ON true
LEFT JOIN LATERAL (
    SELECT
        jsonb_agg(sg.schema_genre) AS schema_genre
    FROM graph.schema_genre sg
    WHERE sg.intellectual_entity_id = ie.id
) sg ON TRUE
LEFT JOIN LATERAL (
    SELECT 
        jsonb_agg(sil.schema_in_language) AS schema_in_language
   FROM graph.schema_in_language sil
    WHERE sil.intellectual_entity_id = ie.id
) sil ON true
LEFT JOIN LATERAL (
    SELECT 
        jsonb_agg(ct.ha_des_coloring_type) AS ha_des_coloring_type
    FROM graph.ha_des_coloring_type ct
        LEFT JOIN graph.carrier carr ON carr.id = ct.carrier_id
    where carr.intellectual_entity_id = ie.id
) ct ON true
LEFT JOIN LATERAL (
    SELECT 
        jsonb_agg(sl.schema_license) AS schema_license
    FROM graph.schema_license sl
    where sl.intellectual_entity_id = ie.id
) sl ON true
LEFT JOIN LATERAL (
    SELECT
        CASE
            WHEN 'Geluidsspoel' = ANY(array_agg(ca.type)) THEN true
            WHEN 'Beeldspoel' = ANY(array_agg(ca.type)) THEN false
            ELSE NULL::boolean
        END AS audio
    FROM graph.carrier ca
    LEFT JOIN graph.dcterms_format df
        ON df.intellectual_entity_id = ca.intellectual_entity_id
       AND ca.type IS NOT NULL
    WHERE ca.intellectual_entity_id = ie.id
      AND df.dcterms_format = 'film'
) fha ON true
LEFT JOIN LATERAL (
    SELECT 
        array_agg(DISTINCT thing.schema_name) AS schema_name
    FROM graph.schema_mentions sme
    LEFT JOIN graph.thing thing ON thing.id = sme.thing_id
    LEFT JOIN graph.intellectual_entity smeie ON smeie.id = sme.intellectual_entity_id
    where smeie.relation_is_part_of = ie.id
) sme ON true
LEFT JOIN LATERAL (
    SELECT
        CASE
            WHEN 'Publiek-Domein' = ANY(array_agg(sl.schema_license)) THEN 'https://creativecommons.org/publicdomain/mark/1.0/'
            WHEN 'COPYRIGHT-UNDETERMINED' = ANY(array_agg(sl.schema_license)) THEN 'https://rightsstatements.org/page/UND/1.0'
            ELSE NULL::text
        END AS dcterms_rights_statement
    FROM graph.schema_license sl
    WHERE sl.intellectual_entity_id = ie.id
      AND sl.schema_license = ANY (ARRAY['COPYRIGHT-UNDETERMINED', 'Publiek-Domein'])
) drs ON true
LEFT JOIN LATERAL (
    SELECT 
        jsonb_agg(DISTINCT co.schema_location_created) AS schema_location_created
    FROM graph.collection co
    RIGHT JOIN graph.schema_is_part_of sipo ON sipo.collection_id = co.id
    WHERE co.schema_location_created IS NOT NULL
        AND sipo.intellectual_entity_id = ie.id
) slc ON true
LEFT JOIN LATERAL (
    SELECT COUNT(*) AS children
    FROM graph.intellectual_entity ie_child
    JOIN graph.dcterms_format dcf
      ON dcf.intellectual_entity_id = ie_child.id
    WHERE ie_child.relation_is_part_of = ie.id
      AND dcf.dcterms_format LIKE '%fragment'
) iec ON true
LEFT JOIN LATERAL (
    SELECT jsonb_object_agg(sipo.type, sipo.schema_name) AS schema_is_part_of
    FROM (
        SELECT 
            po.type,
            array_agg(coll.schema_name) AS schema_name
        FROM graph.schema_is_part_of po
        LEFT JOIN graph.collection coll 
          ON coll.id = po.collection_id
        WHERE po.intellectual_entity_id = ie.id
        GROUP BY po.type
    ) sipo
) ipo ON TRUE
LEFT JOIN LATERAL (
    SELECT 
        jsonb_agg(mf.mh_fragment_identifier) AS mh_fragment_identifier,
        bool_or(mf.is_deleted) AS is_deleted
    FROM graph.mh_fragment_identifier mf
    WHERE mf.intellectual_entity_id = ie.id
) mf ON TRUE
 LEFT JOIN LATERAL (
    SELECT 
        jsonb_agg(DISTINCT carr.premis_medium) AS premis_medium
    FROM graph.carrier carr
    WHERE carr.intellectual_entity_id = ie.id
) pm ON TRUE
LEFT JOIN LATERAL (
    SELECT 
        jsonb_agg(sa.schema_alternate_name) AS schema_alternate_name
    FROM graph.schema_alternate_name sa
    WHERE sa.intellectual_entity_id = ie.id
) sa ON TRUE
LEFT JOIN LATERAL (
    SELECT 
        string_agg(st.schema_transcript, E'\n\r' ORDER BY ie_child.schema_position) AS schema_transcript
    FROM graph.intellectual_entity ie_child
    JOIN graph.intellectual_entity ie_parent
      ON ie_child.relation_is_part_of = ie_parent.id
    LEFT JOIN graph.representation rep
      ON rep.premis_represents = ie_child.id
    LEFT JOIN graph.schema_transcript_url st
      ON st.representation_id = rep.id
    WHERE ie_parent.id = ie.id
) str ON TRUE
WHERE ie.relation_is_part_of IS null
	and ie.updated_at >= %(since)s
	and org.org_identifier = %(id)s;