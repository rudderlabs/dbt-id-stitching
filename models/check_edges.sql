SELECT
    COUNT(*) AS rows_to_update,
    CASE WHEN COUNT(*) > 0 THEN 1::BOOLEAN ELSE 0::BOOLEAN END AS consolidation_needed
FROM
    {{ ref('edges') }},
    (
        SELECT DISTINCT
            a.node_a AS node,
            CASE WHEN a.rudder_id < b.rudder_id THEN a.rudder_id ELSE b.rudder_id END AS first_rudder_id
        FROM {{ ref('edges') }} AS a
        INNER JOIN {{ ref('edges') }} AS b
            ON LOWER(a.node_a) = LOWER(b.node_b)
        WHERE a.rudder_id != b.rudder_id
    ) AS ea
WHERE
    (
        LOWER(edges.node_a) = LOWER(ea.node)
        OR LOWER(edges.node_b) = LOWER(ea.node)
    )
    AND edges.rudder_id != ea.first_rudder_id
