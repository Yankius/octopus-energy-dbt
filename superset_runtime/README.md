# Superset Runtime

This is the lightweight, image-based Superset setup for the capstone project.

It replaces the need to version a full upstream Superset source checkout in this repo.

## What it provides

- official Superset image with a tiny custom layer for BigQuery support
- local Postgres metadata persistence
- local Redis cache
- small project-owned config and startup scripts

Right now the runtime defaults to the locally cached image `superset-superset:latest`, which is already present on your machine from the old setup. This avoids a Docker registry credential problem during migration.

On a fresh machine, you can either:

- set `SUPERSET_IMAGE` to a published image you control
- or restore the Docker Hub pull/build path once your Docker registry credentials are working

## Start

```bash
cd /home/kiril/projects/DEcapstone/octopus_dbt/superset_runtime
bash scripts/start.sh
```

## Stop

```bash
cd /home/kiril/projects/DEcapstone/octopus_dbt/superset_runtime
bash scripts/stop.sh
```

Superset will be available at `http://localhost:8088`.

## Persistent data

- Postgres metadata: `storage/postgres`
- Redis cache: `storage/redis`
- Superset home: `storage/superset_home`

## Local DuckDB access

This runtime mounts the project repo with write access so Superset can open the local DuckDB file.

Use this SQLAlchemy URI when connecting Superset to your local DuckDB:

```text
duckdb:////home/kiril/projects/DEcapstone/octopus_dbt/octopus.duckdb
```

DuckDB often needs write access for lock and WAL behavior, even when you are only querying it.

## Local And Cloud

This setup keeps both options open:

- local development against DuckDB for fast iteration
- cloud reporting against BigQuery for the deployable version of the project

That means you can keep building locally now, then point Superset and dbt Cloud at BigQuery for the final cloud workflow.

## Git

Commit this directory.

Do not commit:

- `.env`
- `storage/`

The old vendored `octopus_dbt/superset/` folder can be kept locally while you transition, but it is no longer required for the reproducible capstone setup.
