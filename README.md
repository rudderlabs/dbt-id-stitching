# ID Stitching dbt Project

[Overview](#overview) · [Prerequisites](#prerequisites) · [Installation](#installation) · [Configuration](#configuration) · [Usage](#usage) · [License](#license)

## Overview

This project is comprised of three dbt models and a Python script. The [`queries`](models/queries.sql) model generates select statements which pull IDs from your tables. The [`id_graph`](models/id_graph.sql) model combines the results of those select statement to create the ID graph the first time it is run, and matches edges (IDs) on subsequent runs. The [`check_edges`](models/check_edges.sql) model determines if there are still edges to match. The [`run_models`](run_models.py) script first runs the `queries` and `id_graph` models and compiles the `check_edges` model. It then runs the `id_graph` model until the `check_edges` query indicates that all edges are matched.

## Prerequisites

- [dbt Core](https://docs.getdbt.com/dbt-cli/install/overview)
- [Python 3](https://www.python.org/downloads/)

## Installation

1. Clone repository:

    ```bash
    git clone https://github.com/esadek/id_stitching_dbt.git
    cd id_stitching_dbt
    ```

2. Install [dbt-utils](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/):

    ```bash
    dbt deps
    ```

3. Install [SQLAlchemy](https://www.sqlalchemy.org/):

    ```bash
    pip install SQLAlchemy
    ```

4. Install appropriate [dialect and DBAPI driver](https://docs.sqlalchemy.org/en/14/dialects/index.html):

    ```bash
    pip install <dialect-package>
    ```

## Configuration

1. Set [profile](https://docs.getdbt.com/dbt-cli/configure-your-profile) and ID columns in [dbt_project.yml](dbt_project.yml):

    ```yaml
    profile: 'profile_name'
    ```

    ```yaml
    id-columns: ('anonymous_id', 'user_id', 'email')
    ```

2. Set [database URL](https://docs.sqlalchemy.org/en/14/core/engines.html?highlight=url#database-urls) in [run_models.py](run_models.py):

    ```python
    db = create_engine("dialect+driver://username:password@host:port/database")
    ```

## Usage

Run models:

```bash
python run_models.py
```

## License

[MIT](LICENSE)