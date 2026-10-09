# The dbt project from dbt-ecommerce-warehouse. It ships with the DuckDB adapter and
# its fixture so the wiring can be shown end to end; for a real warehouse, install
# that adapter (dbt-redshift, dbt-snowflake, ...) and add a target to profiles.yml.
FROM python:3.12-slim

RUN pip install --no-cache-dir "dbt-core>=1.11,<1.13" "dbt-duckdb>=1.10"

ARG REF=main
ADD https://github.com/markotalledo/dbt-ecommerce-warehouse/archive/${REF}.tar.gz /tmp/project.tgz
RUN mkdir /dbt && tar -xzf /tmp/project.tgz --strip-components=1 -C /dbt && rm /tmp/project.tgz \
 && chown -R nobody /dbt

WORKDIR /dbt
ENV DBT_PROFILES_DIR=/dbt
USER nobody
ENTRYPOINT ["dbt"]
