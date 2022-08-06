

{{ config(materialized='incremental', unique_key='original_rudder_id') }}



{% if not is_incremental() %}

    {% set sql_statements = dbt_utils.get_column_values(table=ref('queries'), column='sql_to_run') %}

    select
        row_number() over (order by 1 desc) as rudder_id,
        row_number() over (order by 1 desc) as original_rudder_id,
        edge_a,
        edge_a_label,
        edge_b,
        edge_b_label,
        {{ dbt_utils.current_timestamp() }} as edge_timestamp
    from (
        {{ ' union '.join(sql_statements) }}
    ) s
    where 
            NOT (lower(edge_a) like any {{var('ids-to-exclude')}} ) -- These are known violators that we want to exclude from our edges.  Make this a variable.
        AND
            NOT (lower(edge_b) like any {{var('ids-to-exclude')}} ) -- These are known violators that we want to exclude from our edges.  Make this a variable.


{% else %}

      with 
        cte_min_edge_1 as (
            select edge, min(rudder_id) as first_row_id
                From 
                (
                    Select rudder_id, lower(edge_a) as edge
                    From {{ this }}

                    UNION

                    Select rudder_id, lower(edge_b) as edge
                    From {{ this }}
                ) c
            Group by edge
            ),

        cte_min_edge_2 as (
            select edge, min(rudder_id) as first_row_id
                From 
                (
                    select least(a.first_row_id,  b.first_row_id) as rudder_id,
                        lower(o.edge_a) as edge
                    from {{ this }} o
                      left outer join cte_min_edge_1 a on lower(o.edge_a) = a.edge -- already lowercased in prior step
                      left outer join cte_min_edge_1 b on lower(o.edge_b) = b.edge -- already lowercased in prior step
                
                    UNION

                    select least(a.first_row_id,  b.first_row_id) as rudder_id,
                        lower(o.edge_b) as edge
                    from {{ this }} o
                      left outer join cte_min_edge_1 a on lower(o.edge_a) = a.edge 
                      left outer join cte_min_edge_1 b on lower(o.edge_b) = b.edge 
                  
                )
            Group by edge
        ),

        cte_min_edge_3 as (
            select edge, min(rudder_id) as first_row_id
                From 
                (
                    select least(a.first_row_id,  b.first_row_id) as rudder_id,
                        lower(o.edge_a) as edge
                    from {{ this }} o
                      left outer join cte_min_edge_2 a on lower(o.edge_a) = a.edge 
                      left outer join cte_min_edge_2 b on lower(o.edge_b) = b.edge 
                    
                    UNION

                    select least(a.first_row_id,  b.first_row_id) as rudder_id,
                        lower(o.edge_b) as edge
                    from {{ this }} o
                      left outer join cte_min_edge_2 a on lower(o.edge_a) = a.edge 
                      left outer join cte_min_edge_2 b on lower(o.edge_b) = b.edge 
                    
                )
            Group by edge
        ),

        cte_new_id as (
            select
                least(a.first_row_id, b.first_row_id) as new_rudder_id,
                o.original_rudder_id
            From {{ this }} o 
              left outer join cte_min_edge_3 a on lower(o.edge_a) = a.edge 
              left outer join cte_min_edge_3 b on lower(o.edge_b) = b.edge
        
          ) 


        Select  n.new_rudder_id as rudder_id,
            e.original_rudder_id,
            e.edge_a,
            e.edge_a_label,
            e.edge_b,
            e.edge_b_label,
            {{ dbt_utils.current_timestamp() }} as edge_timestamp
        From {{ this }} e
            Inner Join cte_new_id n ON  e.original_rudder_id = n.original_rudder_id 
        where e.rudder_id <> n.new_rudder_id        

{% endif %}