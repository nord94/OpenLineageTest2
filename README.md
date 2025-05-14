# Airflow Word Count Project

This project demonstrates a simple ETL pipeline using Apache Airflow to process text data and store results in a PostgreSQL data warehouse.

## Project Structure

```
.
├── config/
│   └── airflow.env           # Airflow environment variables and connections
├── data/
│   └── text.txt             # Sample text file for processing
├── dags/
│   └── word_count_dag.py    # DAG definition for word counting
├── logs/                    # Airflow logs directory
├── plugins/                 # Airflow plugins directory
├── docker-compose.yaml      # Docker Compose configuration
└── requirements.txt         # Python dependencies
```

## Components

- **Airflow 2.10.5**: Orchestration tool with CeleryExecutor
- **PostgreSQL 17.2**: 
  - Airflow metadata database
  - Data warehouse for storing processed data
- **Redis**: Message broker for CeleryExecutor

## Setup

1. Create required directories:
```bash
mkdir -p dags logs plugins config data
```

2. Start the services:
```bash
docker-compose up -d
```

3. Access Airflow UI:
- URL: http://localhost:8080
- Username: airflow
- Password: airflow

## Database Connections

### Airflow Metadata Database
- Host: localhost
- Port: 5432
- Database: airflow
- Username: airflow
- Password: airflow

### Data Warehouse (DWH)
- Host: localhost
- Port: 5433
- Database: dwh
- Username: dwh_user
- Password: dwh_password

## DAG Description

The `word_count_dag.py` DAG:
1. Reads text from `/opt/airflow/data/text.txt`
2. Counts word occurrences
3. Stores results in the DWH `word_counts` table

## Development

### Adding New DAGs
1. Place new DAG files in the `dags/` directory
2. Airflow will automatically detect and load them
3. Changes are picked up within 30 seconds

### Environment Variables
- All Airflow configurations are in `config/airflow.env`
- Add new connections using the format:
  ```
  AIRFLOW_CONN_CONNECTION_NAME=connection_string
  ```

### Restarting Services
```bash
docker-compose down
docker-compose up -d
```

## Monitoring

- Airflow UI: http://localhost:8080
- DAG logs: Available in the Airflow UI
- Container logs:
  ```bash
  docker-compose logs -f [service_name]
  ```

## Troubleshooting

1. If DAG changes aren't reflected:
   - Click "Refresh" in Airflow UI
   - Or run: `docker-compose exec airflow-webserver airflow dags reserialize`

2. If containers fail to start:
   - Check logs: `docker-compose logs`
   - Verify port availability
   - Ensure all required directories exist

## Security Notes

- Default credentials are for development only
- In production:
  - Change all default passwords
  - Use proper secrets management
  - Enable SSL for database connections
  - Set up proper authentication
