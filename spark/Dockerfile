FROM apache/spark:3.5.1-python3

USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.10 \
    && mkdir -p /app /tmp/spark-checkpoints /tmp/ivy \
    && chown -R spark:spark /app /tmp/spark-checkpoints /tmp/ivy

USER spark
WORKDIR /app
COPY --chown=spark:spark process_stream.py /app/process_stream.py
