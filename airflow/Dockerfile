FROM apache/airflow:latest
USER root
RUN apt-get update && \
    apt-get install -y libgeos-dev && \
    rm -rf /var/lib/apt/lists/* \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow