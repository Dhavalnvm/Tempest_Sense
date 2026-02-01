import os

BASE_DIR = "cyclone_realtime_backend"

structure = [
    "data_ingestion",
    "stream_processing",
    "database",
    "api_server/routes",
    "api_server/services",
    "monitoring",
    "scripts"
]

files = [
    "README.md",
    ".env",

    "data_ingestion/producer.py",
    "data_ingestion/config.py",
    "data_ingestion/parser.py",

    "stream_processing/consumer.py",
    "stream_processing/topics.py",
    "stream_processing/serializer.py",

    "database/clickhouse_schema.sql",
    "database/init_db.py",

    "api_server/main.py",
    "api_server/routes/live.py",
    "api_server/routes/history.py",
    "api_server/routes/forecast.py",

    "api_server/services/redis_service.py",
    "api_server/services/clickhouse_service.py",
    "api_server/services/forecast_service.py",

    "monitoring/logger.py",
    "monitoring/healthcheck.py",

    "scripts/start_producer.sh",
    "scripts/start_consumer.sh",
    "scripts/start_api.sh"
]


def create_structure():
    # Create base directory
    os.makedirs(BASE_DIR, exist_ok=True)

    # Create folders
    for folder in structure:
        path = os.path.join(BASE_DIR, folder)
        os.makedirs(path, exist_ok=True)

    # Create files
    for file in files:
        path = os.path.join(BASE_DIR, file)
        folder = os.path.dirname(path)
        if folder and not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)

        if not os.path.exists(path):
            with open(path, "w") as f:
                f.write("")

    print("✅ Project structure created successfully!")


if __name__ == "__main__":
    create_structure()
