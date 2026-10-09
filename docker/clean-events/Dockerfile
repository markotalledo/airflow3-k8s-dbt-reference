# PySpark job from pyspark-glue-ingestion, runnable as a Kubernetes pod.
FROM python:3.11-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*

ARG REF=main
RUN pip install --no-cache-dir "git+https://github.com/markotalledo/pyspark-glue-ingestion@${REF}"

# S3 through s3a, with credentials from the pod's service account (IRSA).
COPY spark-defaults.conf /opt/spark-conf/spark-defaults.conf
ENV SPARK_CONF_DIR=/opt/spark-conf

USER nobody
ENTRYPOINT ["python", "-m", "clean_events.job"]
