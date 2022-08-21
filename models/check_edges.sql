SELECT
    COUNT(*) AS rows_to_update,
    CASE WHEN COUNT(*) > 0 THEN 1::BOOLEAN ELSE 0::BOOLEAN END AS consolidation_needed
FROM
    {{ ref('edges') }},
    (
        SELECT DISTINCT
            a.edge_a AS edge,
            CASE WHEN a.rudder_id < b.rudder_id THEN a.rudder_id ELSE b.rudder_id END AS first_rudder_id
        FROM {{ ref('edges') }} AS a
        INNER JOIN {{ ref('edges') }} AS b
            ON LOWER(a.edge_a) = LOWER(b.edge_b)
        WHERE a.rudder_id != b.rudder_id
    ) AS ea
WHERE
    (
        LOWER(edges.edge_a) = LOWER(ea.edge)
        OR LOWER(edges.edge_b) = LOWER(ea.edge)
    )
    AND edges.rudder_id != ea.first_rudder_id
