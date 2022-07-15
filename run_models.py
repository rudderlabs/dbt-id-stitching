## Execute Models
system("dbt run --full-refresh --select queries edges; dbt compile --select check_edges")

## Repeat edges sql to consolidate all identifiers 
## Testing will be required to determine how many times this needs to be run
system("dbt run --select edges")
system("dbt run --select edges")
system("dbt run --select edges")
system("dbt run --select edges")
system("dbt run --select edges")
system("dbt run --select edges")

## Create the final ID Graph table
system("dbt run --select id_graph")

## Examine the check_edges view in the warehouse to determine whether sufficient iterations of the edges model have been run