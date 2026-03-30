import argparse
import os
from dataclasses import dataclass
from pathlib import Path

import duckdb
from dotenv import load_dotenv

try:
    from google.cloud import bigquery, storage
except ImportError as exc:  # pragma: no cover - import guard for local setup
    raise ImportError(
        "Missing Google Cloud dependencies. Install google-cloud-bigquery and "
        "google-cloud-storage before running this bridge."
    ) from exc


load_dotenv(Path(__file__).with_name("environment.env"))


@dataclass(frozen=True)
class TableConfig:
    table_name: str
    export_query: str
    gcs_blob: str
    partition_field: str | None = None


TABLE_CONFIGS = {
    "raw_octopus_consumption": TableConfig(
        table_name="raw_octopus_consumption",
        export_query="""
            SELECT
                interval_start_utc,
                interval_start_uk,
                kwh,
                ingestion_ts
            FROM raw_octopus_consumption
            ORDER BY interval_start_utc
        """,
        gcs_blob="bronze/raw_octopus_consumption/raw_octopus_consumption.parquet",
        partition_field="interval_start_utc",
    ),
    "raw_octopus_agreements": TableConfig(
        table_name="raw_octopus_agreements",
        export_query="""
            SELECT
                tariff_code,
                valid_from_utc,
                valid_to_utc,
                valid_from_uk,
                valid_to_uk,
                ingestion_ts
            FROM raw_octopus_agreements
            ORDER BY valid_from_utc
        """,
        gcs_blob="bronze/raw_octopus_agreements/raw_octopus_agreements.parquet",
    ),
    "raw_octopus_tariffs": TableConfig(
        table_name="raw_octopus_tariffs",
        export_query="""
            SELECT
                tariff_code,
                product_code,
                valid_from_utc,
                valid_to_utc,
                valid_from_uk,
                valid_to_uk,
                unit_rate,
                ingestion_ts
            FROM raw_octopus_tariffs
            ORDER BY valid_from_utc
        """,
        gcs_blob="bronze/raw_octopus_tariffs/raw_octopus_tariffs.parquet",
    ),
}


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Environment variable {name} is required.")
    return value


def resolve_credentials_path() -> str:
    env_value = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if env_value:
        return env_value

    repo_root = Path(__file__).resolve().parent.parent
    default_path = repo_root / "secrets" / "gcp_credentials.json"

    if default_path.exists():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(default_path)
        return str(default_path)

    raise ValueError(
        "Environment variable GOOGLE_APPLICATION_CREDENTIALS is required. "
        "Expected it in the environment or at "
        f"{default_path}."
    )


def build_clients() -> tuple[storage.Client, bigquery.Client]:
    project_id = required_env("GCP_PROJECT")
    credentials_path = resolve_credentials_path()

    if not Path(credentials_path).exists():
        raise FileNotFoundError(
            f"GOOGLE_APPLICATION_CREDENTIALS does not exist: {credentials_path}"
        )

    storage_client = storage.Client(project=project_id)
    bigquery_client = bigquery.Client(project=project_id)
    return storage_client, bigquery_client


def connect_duckdb() -> duckdb.DuckDBPyConnection:
    db_path = required_env("DUCKDB_PATH")
    return duckdb.connect(db_path)


def ensure_table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> None:
    exists = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()[0]

    if not exists:
        raise ValueError(
            f"DuckDB table {table_name} was not found. Run the local ingestion first."
        )


def export_table_to_parquet(
    con: duckdb.DuckDBPyConnection,
    table_config: TableConfig,
    output_root: Path,
) -> Path:
    ensure_table_exists(con, table_config.table_name)

    output_root.mkdir(parents=True, exist_ok=True)
    parquet_path = output_root / f"{table_config.table_name}.parquet"
    escaped_path = parquet_path.as_posix().replace("'", "''")

    con.execute(
        f"""
        COPY (
            {table_config.export_query}
        )
        TO '{escaped_path}'
        (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)
        """
    )

    return parquet_path


def upload_to_gcs(
    storage_client: storage.Client,
    bucket_name: str,
    source_path: Path,
    blob_name: str,
) -> str:
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(source_path)
    return f"gs://{bucket_name}/{blob_name}"


def load_into_bigquery(
    bigquery_client: bigquery.Client,
    dataset_name: str,
    table_config: TableConfig,
    gcs_uri: str,
    write_disposition: str,
) -> str:
    destination = f"{bigquery_client.project}.{dataset_name}.{table_config.table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition=write_disposition,
    )

    if table_config.partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=table_config.partition_field,
        )

    job = bigquery_client.load_table_from_uri(
        gcs_uri,
        destination,
        job_config=job_config,
    )
    job.result()
    return destination


def bridge_tables(
    table_names: list[str],
    output_dir: str,
    load_bigquery: bool,
    write_disposition: str,
) -> None:
    bucket_name = required_env("GCS_BUCKET_NAME")
    dataset_name = os.getenv("BQ_DATASET_NAME")
    storage_client, bigquery_client = build_clients()
    output_root = Path(output_dir)

    if load_bigquery and not dataset_name:
        raise ValueError("BQ_DATASET_NAME is required when BigQuery loading is enabled.")

    with connect_duckdb() as con:
        for table_name in table_names:
            if table_name not in TABLE_CONFIGS:
                valid_tables = ", ".join(sorted(TABLE_CONFIGS))
                raise ValueError(
                    f"Unsupported table {table_name}. Valid values: {valid_tables}"
                )

            config = TABLE_CONFIGS[table_name]
            parquet_path = export_table_to_parquet(con, config, output_root)
            gcs_uri = upload_to_gcs(
                storage_client=storage_client,
                bucket_name=bucket_name,
                source_path=parquet_path,
                blob_name=config.gcs_blob,
            )

            print(f"Uploaded {table_name} to {gcs_uri}")

            if load_bigquery:
                destination = load_into_bigquery(
                    bigquery_client=bigquery_client,
                    dataset_name=dataset_name,
                    table_config=config,
                    gcs_uri=gcs_uri,
                    write_disposition=write_disposition,
                )
                print(f"Loaded {table_name} into {destination}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export anonymized Octopus Energy tables from local DuckDB to GCS and BigQuery."
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=["raw_octopus_consumption", "raw_octopus_agreements", "raw_octopus_tariffs"],
        help="DuckDB tables to export. Supported values are the bronze Octopus tables.",
    )
    parser.add_argument(
        "--output-dir",
        default="exports",
        help="Local directory where Parquet files are written before upload.",
    )
    parser.add_argument(
        "--skip-bigquery",
        action="store_true",
        help="Upload to GCS only and skip the BigQuery load step.",
    )
    parser.add_argument(
        "--write-disposition",
        default="WRITE_TRUNCATE",
        choices=["WRITE_TRUNCATE", "WRITE_APPEND"],
        help="BigQuery write mode. WRITE_TRUNCATE keeps cloud bronze aligned with local DuckDB.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    bridge_tables(
        table_names=args.tables,
        output_dir=args.output_dir,
        load_bigquery=not args.skip_bigquery,
        write_disposition=args.write_disposition,
    )


if __name__ == "__main__":
    main()
