FROM python:3.9.1

ENV PYTHONUNBUFFERED=1 

RUN apt-get update && apt-get install -y wget
RUN pip install --upgrade pip
RUN pip install pandas sqlalchemy psycopg2-binary pyarrow

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]
