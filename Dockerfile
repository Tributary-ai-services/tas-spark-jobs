FROM apache/spark:3.5.0-python3

USER root
COPY requirements.txt /opt/spark/requirements.txt
RUN pip install --no-cache-dir -r /opt/spark/requirements.txt

COPY jobs/ /opt/spark/jobs/

USER spark
