with columns as (
    select
        '"' || table_catalog || '"."' || table_schema || '"."' || table_name  || '"' as tn,
        column_name as cn
    from
        {{ source('information_schema', 'columns') }}
    where
        lower(column_name) in {{ var('id-columns') }}
        and 
        NOT(LOWER(table_name) like any ('snapshot_%', 'sync_data_%', 'failed_records_%')) -- Exclude RudderStack Working Tables
        --- AND if necessary, limit schemas, tables, etc.
)

select
    'select distinct (' || a.cn || '::text) as edge_a, (''' || a.cn  || ''') as edge_a_label, (' || b.cn || '::text) as edge_b, (''' || b.cn  || ''') as edge_b_label from ' ||  a.tn || ' where coalesce(' || a.cn ||  '::text, '''') <> '''' and coalesce(' || b.cn ||  '::text, '''') <> ''''' as sql_to_run
from
    columns a
inner join
    columns b 
        on a.tn = b.tn
        and a.cn > b.cn
        