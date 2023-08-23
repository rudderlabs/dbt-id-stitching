{{ config(materialized='table') }}

SELECT
    rudder_id,
    node,
    {{ dbt.listagg('DISTINCT node_label', "', '") }} AS labels,
    MAX(node_timestamp) AS latest_timestamp
FROM (
    SELECT
        rudder_id,
        node_a AS node,
        node_a_label AS node_label,
        node_timestamp
    FROM {{ ref('edges') }}
    UNION
    SELECT
        rudder_id,
        node_b AS node,
        node_b_label AS node_label,
        node_timestamp
    FROM {{ ref('edges') }}
) AS c
GROUP BY
    rudder_id,
    node
ORDER BY rudder_id
