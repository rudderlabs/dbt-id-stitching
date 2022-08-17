# ID Stitching dbt Package

This dbt package stitches together identifiers in an ID graph table.

## Overview

The primary ouput of this package is `id_graph`. There are a few intermediate models used to create this model.

| Model | Description |
| --- | --- |
| queries | Generates select statements which pull IDs from your tables. |
| edges | Combines the results of those select statement to create a table containing edges (IDs) the first time it is run, and matches edges on subsequent runs. |
| check_edges | Determines if there are still edges to match. |
| id_graph | Creates an ID graph table. |

## Installation

Check [dbt Hub](https://hub.getdbt.com/rudderlabs/id_stitching/latest/) for the latest installation instructions, or [read the docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

## Configuration

Set ID columns and IDs to exclude in `dbt_project.yml`:

```yaml
vars:
  id-columns: ('anonymous_id', 'user_id', 'email')
  ids-to-exclude: ('sources','user@company.com')
```

This package searches your data warehouse for tables that include multiple columns defined in `id-columns`. Any IDs defined in `ids-to-exclude` are disregarded.

## Usage

The `edges` model must be run enough times to match all edges (IDs). Five or six passes is usually sufficient. The `check_edges` model will show 0 when all edges have been matched. Edit your job commands for [dbt Cloud](https://docs.getdbt.com/docs/dbt-cloud/cloud-overview) or `run.sh` script for [dbt CLI](https://docs.getdbt.com/dbt-cli/cli-overview) to run the `edges` model however many times is necessary.

### dbt Cloud

Create a job with the following commands:

```bash
dbt run --full-refresh --select queries edges
dbt run --select edges
dbt run --select edges
dbt run --select edges
dbt run --select edges
dbt run --select edges check_edges id_graph
```

### dbt CLI

Run the included `run.sh` shell script:

```bash
./run.sh
```

Additional intstrumentation can be created to evaluate the `check_edges` model to determine programatically whether to run the `edges` model subsequent times.

## License

[MIT](LICENSE)
