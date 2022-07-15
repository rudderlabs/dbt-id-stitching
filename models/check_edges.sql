select
    case when count(*) > 0 then 1::boolean else 0::boolean end as consolidation_needed,
    count(*) as rows_to_update
from
    {{ ref('edges') }},
    (
        select
            distinct case when a.rudder_id < b.rudder_id then a.rudder_id else b.rudder_id end as first_rudder_id,
            a.edge_a as edge
        from
            {{ ref('edges') }} a
        inner join
            {{ ref('edges') }} b
                on lower(a.edge_a) = lower(b.edge_b)
        where
            a.rudder_id <> b.rudder_id
    ) ea
where
    (
        lower(edges.edge_a) = lower(ea.edge)
        or lower(edges.edge_b) = lower(ea.edge)
    )
    and edges.rudder_id <> ea.first_rudder_id