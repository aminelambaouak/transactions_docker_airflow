-- Active: 1754142323496@@postgres@5432
# Airflow ETL Pipeline Project

A containerized Apache Airflow setup for ETL data processing with PostgreSQL and Redis.

## 🚀 Features

- **Apache Airflow** with CeleryExecutor
- **PostgreSQL** database for data storage
- **Redis** message broker for task queue
- **Docker Compose** for easy deployment
- **ETL Pipeline** for transaction data processing
- **Example DAG** with astronaut data from Open Notify API

## 📋 Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)
- 8GB+ RAM recommended

## 🛠️ Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/airflow-etl-pipeline.git
cd airflow-etl-pipeline
```

### 2. Environment Configuration
```bash
# Copy environment template
cp .env.example .env

# Edit .env file with your settings
nano .env
```

### 3. Start Services
```bash
# Start all services
docker-compose up

# Or run in background
docker-compose up -d
```

### Connect via command line:
```bash
docker-compose exec postgres psql -U postgre -d transactions_db
```

## 📊 DAGs

### 1. Example Astronauts DAG
- Fetches current astronauts in space from Open Notify API
- Demonstrates dynamic task mapping
- Updates dataset for downstream DAGs

### 2. Transaction ETL Pipeline
- Creates transactions table in PostgreSQL
- Extracts data from Mockaroo API
- Loads and validates transaction data
- Performs data quality checks

## 🔧 Development

### Project Structure
```
├── docker-compose.yml      # Docker services configuration
├── dags/                   # Airflow DAG files
│   ├── exampledag.py      # Astronaut example DAG
│   └── etl_dag.py         # ETL pipeline DAG
├── etl/                   # ETL scripts
│   ├── connectçcreate_table.py    # Database table creation
│   └──  extract_transform.py         # Data extraction logic
│     
├── logs/                  # Airflow logs (auto-generated)
├── plugins/               # Custom Airflow plugins
└── .env                   # Environment variables
```

### Adding New DAGs
1. Place Python files in the `dags/` directory
2. Use Airflow's TaskFlow API for better maintainability
3. Test DAGs before deployment

### Custom ETL Scripts
1. Add scripts to the `etl/` directory
2. Ensure proper error handling and logging
3. Use environment variables for configuration

## 🐛 Troubleshooting


**DAG Import Errors:**
- Check Python syntax in DAG files
- Verify all imports are available
- Check Airflow logs for specific errors


## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🔗 Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Astronomer Airflow Tutorials](https://www.astronomer.io/docs/learn/)