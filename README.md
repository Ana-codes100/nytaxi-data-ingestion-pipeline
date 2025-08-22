# nytaxi-data-ingestion-pipeline
A Dockerised data pipeline for ingesting NYC Taxi data into PostgreSQL using Python, Pandas, and SQLAlchemy.
## NYC Taxi Data Pipeline

This project implements a containerised data ingestion pipeline for loading NYC Taxi data (or other datasets) into a PostgreSQL database. 
It is built with Docker and Python, using Pandas and SQLAlchemy for efficient ETL operations.

### Features
- **Dockerised environment** for portability and reproducibility
- **Automatic data download** from a given URL (supports `.csv`, `.csv.gz`, and `.parquet`)
- **Efficient chunked loading** into PostgreSQL to handle large datasets
- **Date parsing** for taxi pickup and dropoff datetime columns
- **Configurable via command-line arguments** (database credentials, table name, dataset URL)

### Tech Stack
- **Python 3.9**
- **Pandas** for data processing
- **SQLAlchemy & psycopg2** for database interaction
- **PostgreSQL** as the target database
- **Docker** for containerisation

### Example Usage
```bash
docker build -t ny_taxi_ingest .
docker run -it \
  --network=pg-network \
  ny_taxi_ingest \
  --user=root \
  --password=example \
  --host=pg-database \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_data \
  --url="https://somedataurl/ny_taxi_data.csv.gz"
