{{ config(materialized='table') }}

SELECT DISTINCT
    rudder_id,
    edge,
    {{ dbt_utils.listagg('DISTINCT edge_label', "', '") }} AS labels,
    MAX(edge_timestamp) AS latest_timestamp
FROM (
    SELECT
        rudder_id,
        edge_a AS edge,
        edge_a_label AS edge_label,
        edge_timestamp
    FROM {{ ref('edges') }}
    UNION ALL
    SELECT
        rudder_id,
        edge_b AS edge,
        edge_b_label AS edge_label,
        edge_timestamp
    FROM {{ ref('edges') }}
) AS c
GROUP BY
    rudder_id,
    edge
ORDER BY rudder_id
