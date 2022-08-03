#!/bin/sh

# Run the queries and edges models
dbt run --full-refresh --select queries edges

# Run the edges model repeatedly to consolidate all identifiers 
# Testing will be required to determine how many times this needs to be run
for i in {1..5}
do
  dbt run --select edges
done

# Run the check_edges model
# Examine the check_edges view in the warehouse to determine whether sufficient iterations of the edges model have been run
dbt run --select check_edges

# Run the id_graph model to create the final ID Graph table
dbt run --select id_graph
