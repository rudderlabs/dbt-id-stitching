from os import system
from sqlalchemy import create_engine

db = create_engine("dialect+driver://username:password@host:port/database")

system("dbt run --full-refresh --select queries edges; dbt compile --select check_edges")

## with open("target/compiled/id_stitching/models/check_edges.sql") as file:
##     query = file.read()
##while db.execute(query).first()["count"]:

## Repeat edges sql to consolidate all identifiers 
## NOTE:  Testing will be required to determine how many times this needs to be run
system("dbt run --select edges")
system("dbt run --select edges")
system("dbt run --select edges")
system("dbt run --select edges")
system("dbt run --select edges")
system("dbt run --select edges")

## Create the final ID Graph table 
system("dbt run --select id_graph")
