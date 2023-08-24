{{ config(materialized='incremental', unique_key='original_rudder_id') }}

{% if not is_incremental() %}

    {% set sql_statements = dbt_utils.get_column_values(table=ref('queries'), column='sql_to_run') %}

    SELECT
        ROW_NUMBER() OVER (ORDER BY 1 DESC) AS rudder_id,
        ROW_NUMBER() OVER (ORDER BY 1 DESC) AS original_rudder_id,
        node_a,
        node_a_label,
        node_b,
        node_b_label,
        {{ dbt.current_timestamp() }} AS node_timestamp
    FROM (
        {{ ' UNION '.join(sql_statements) }}
    ) AS s
    {% if var('ids-to-exclude', undefined) %}
    WHERE
        NOT LOWER(node_a) IN {{ var('ids-to-exclude') }}
        AND NOT LOWER(node_b) IN {{ var('ids-to-exclude') }}
    {% endif %}

{% else %}

    WITH
    cte_min_node_1 AS (
        SELECT
            node,
            MIN(rudder_id) AS first_row_id
        FROM (
            SELECT
                rudder_id,
                LOWER(node_a) AS node
            FROM {{ this }}
            UNION
            SELECT
                rudder_id,
                LOWER(node_b) AS node
            FROM {{ this }}
        ) AS c
        GROUP BY node
    ),

    cte_min_node_2 AS (
        SELECT
            node,
            MIN(rudder_id) AS first_row_id
        FROM (
            SELECT
                LEAST(a.first_row_id, b.first_row_id) AS rudder_id,
                LOWER(o.node_a) AS node
            FROM {{ this }} AS o
            LEFT OUTER JOIN cte_min_node_1 AS a
                ON LOWER(o.node_a) = a.node
            LEFT OUTER JOIN cte_min_node_1 AS b
                ON LOWER(o.node_b) = b.node
            UNION
            SELECT
                LEAST(a.first_row_id, b.first_row_id) AS rudder_id,
                LOWER(o.node_b) AS node
            FROM {{ this }} AS o
            LEFT OUTER JOIN cte_min_node_1 AS a
                ON LOWER(o.node_a) = a.node
            LEFT OUTER JOIN cte_min_node_1 AS b
                ON LOWER(o.node_b) = b.node

        ) AS g
        GROUP BY node
    ),

    cte_min_node_3 AS (
        SELECT
            node,
            MIN(rudder_id) AS first_row_id
        FROM (
            SELECT
                LEAST(a.first_row_id, b.first_row_id) AS rudder_id,
                LOWER(o.node_a) AS node
            FROM {{ this }} AS o
            LEFT OUTER JOIN cte_min_node_2 AS a
                ON LOWER(o.node_a) = a.node
            LEFT OUTER JOIN cte_min_node_2 AS b
                ON LOWER(o.node_b) = b.node
            UNION
            SELECT
                LEAST(a.first_row_id, b.first_row_id) AS rudder_id,
                LOWER(o.node_b) AS node
            FROM {{ this }} AS o
            LEFT OUTER JOIN cte_min_node_2 AS a
                ON LOWER(o.node_a) = a.node
            LEFT OUTER JOIN cte_min_node_2 AS b
                ON LOWER(o.node_b) = b.node

        ) AS h
        GROUP BY node
    ),

    cte_new_id AS (
        SELECT
            o.original_rudder_id,
            LEAST(a.first_row_id, b.first_row_id) AS new_rudder_id
        FROM {{ this }} AS o
        LEFT OUTER JOIN cte_min_node_3 AS a
            ON LOWER(o.node_a) = a.node
        LEFT OUTER JOIN cte_min_node_3 AS b
            ON LOWER(o.node_b) = b.node
    )

    SELECT
        cte_new_id.new_rudder_id AS rudder_id,
        e.original_rudder_id,
        e.node_a,
        e.node_a_label,
        e.node_b,
        e.node_b_label,
        {{ dbt.current_timestamp() }} AS node_timestamp
    FROM {{ this }} AS e
    INNER JOIN cte_new_id
        ON e.original_rudder_id = cte_new_id.original_rudder_id
    WHERE e.rudder_id != cte_new_id.new_rudder_id

{% endif %}
