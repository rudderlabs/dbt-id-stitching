{{ config(materialized='table') }}

select
    rudder_id,
    edge,
     {{ dbt_utils.listagg('DISTINCT edge_label', "', '") }}  as labels,
    max(edge_timestamp) as latest_timestamp
from (
    select
        rudder_id,
        edge_a as edge,
        edge_a_label as edge_label, 
        edge_timestamp
    from
        {{ ref('edges') }}
    union
    select
        rudder_id,
        edge_b as edge,
        edge_b_label as edge_label, 
        edge_timestamp
    from
        {{ ref('edges') }}
) c
group by
    rudder_id,
    edge
order by
    rudder_id