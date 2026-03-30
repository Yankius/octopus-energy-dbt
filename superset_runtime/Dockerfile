FROM apache/superset:4.1.2

USER root

RUN pip install --no-cache-dir \
    sqlalchemy-bigquery \
    google-cloud-bigquery \
    db-dtypes

USER superset
