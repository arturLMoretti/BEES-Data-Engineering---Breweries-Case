FROM apache/airflow:3.1.0-python3.12

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

COPY --chown=airflow:root ./src /opt/airflow/src
COPY --chown=airflow:root ./dags /opt/airflow/dags

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src"
