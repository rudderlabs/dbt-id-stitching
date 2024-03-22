WITH columns AS (
    SELECT
        column_name AS cn,
        table_catalog || '.' || table_schema || '.' || table_name AS tn
    FROM {{ var('source-database', target.database) }}.{{ source('information_schema', 'columns').include(database=false) }}
    WHERE
        LOWER(column_name) IN {{ var('id-columns') }}
        {% if var('schemas-to-include', undefined) %}
        AND LOWER(table_schema) IN {{ var('schemas-to-include') }}
        {% endif %}
        {% if var('schemas-to-exclude', undefined) %}
        AND NOT LOWER(table_schema) IN {{ var('schemas-to-exclude') }}
        {% endif %}
        {% if var('tables-to-include', undefined) %}
        AND LOWER(table_name) IN {{ var('tables-to-include') }}
        {% endif %}
        {% if var('tables-to-exclude', undefined) %}
        AND NOT LOWER(table_name) IN {{ var('tables-to-exclude') }}
        {% endif %}
        AND NOT LOWER(table_name) LIKE 'snapshot_%'
        AND NOT LOWER(table_name) LIKE 'sync_data_%'
        AND NOT LOWER(table_name) LIKE 'failed_records_%'
)

SELECT 'SELECT DISTINCT ' || a.cn || '::TEXT AS node_a, ''' || a.cn || ''' AS node_a_label, ' || b.cn || '::TEXT AS node_b, ''' || b.cn || ''' AS node_b_label FROM ' || a.tn || ' WHERE COALESCE(' || a.cn || '::TEXT, '''') != '''' AND COALESCE(' || b.cn || '::TEXT, '''') != ''''' AS sql_to_run
FROM columns AS a
INNER JOIN columns AS b
    ON a.tn = b.tn
        AND a.cn > b.cn
