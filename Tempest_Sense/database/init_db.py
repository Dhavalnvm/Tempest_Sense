"""
Database Initialization Script
Initializes ClickHouse database and creates all required tables
"""
import os
import sys
import logging
import clickhouse_connect
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_clickhouse_client():
    """Create ClickHouse client connection"""
    host = os.getenv('CLICKHOUSE_HOST', 'localhost')
    port = int(os.getenv('CLICKHOUSE_PORT', '9000'))
    user = os.getenv('CLICKHOUSE_USER', 'admin')
    password = os.getenv('CLICKHOUSE_PASSWORD', 'admin123')

    logger.info(f"Connecting to ClickHouse at {host}:{port}")

    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password
    )


def execute_sql_file(client, sql_file_path):
    """Execute SQL commands from a file"""
    logger.info(f"Executing SQL from {sql_file_path}")

    with open(sql_file_path, 'r') as f:
        sql_content = f.read()

    # Split by semicolon and execute each statement
    statements = [s.strip() for s in sql_content.split(';') if s.strip()]

    for idx, statement in enumerate(statements, 1):
        try:
            # Skip comments and empty statements
            if statement.startswith('--') or not statement:
                continue

            logger.debug(f"Executing statement {idx}/{len(statements)}")
            client.command(statement)
            logger.debug(f"✓ Statement {idx} executed successfully")

        except Exception as e:
            logger.error(f"✗ Error executing statement {idx}: {e}")
            logger.error(f"Statement: {statement[:100]}...")
            raise


def verify_tables(client, database='cyclones'):
    """Verify that all required tables exist"""
    logger.info("Verifying database tables...")

    required_tables = [
        'cyclone_positions',
        'cyclone_forecasts',
        'cyclone_metadata',
        'cyclone_tracks',
        'cyclone_intensity_changes'
    ]

    result = client.query(f"SHOW TABLES FROM {database}")
    existing_tables = [row[0] for row in result.result_rows]

    logger.info(f"Found {len(existing_tables)} tables in database '{database}'")

    for table in required_tables:
        if table in existing_tables:
            logger.info(f"  ✓ {table}")
        else:
            logger.warning(f"  ✗ {table} - MISSING!")

    # Check materialized view
    result = client.query(f"SHOW TABLES FROM {database} WHERE engine LIKE '%MaterializedView%'")
    mv_count = len(result.result_rows)
    logger.info(f"Found {mv_count} materialized view(s)")

    missing = set(required_tables) - set(existing_tables)
    if missing:
        logger.error(f"Missing tables: {missing}")
        return False

    logger.info("✓ All required tables exist")
    return True


def get_table_stats(client, database='cyclones'):
    """Get statistics for each table"""
    logger.info("Collecting table statistics...")

    tables = [
        'cyclone_positions',
        'cyclone_forecasts',
        'cyclone_metadata',
        'cyclone_tracks',
        'cyclone_intensity_changes'
    ]

    for table in tables:
        try:
            result = client.query(f"SELECT count() FROM {database}.{table}")
            count = result.result_rows[0][0] if result.result_rows else 0
            logger.info(f"  {table}: {count} records")
        except Exception as e:
            logger.warning(f"  {table}: Error - {e}")


def initialize_database():
    """Main initialization function"""
    logger.info("=" * 60)
    logger.info("ClickHouse Database Initialization")
    logger.info("=" * 60)

    try:
        # Connect to ClickHouse
        client = get_clickhouse_client()
        logger.info("✓ Connected to ClickHouse")

        # Test connection
        result = client.query("SELECT version()")
        version = result.result_rows[0][0]
        logger.info(f"✓ ClickHouse version: {version}")

        # Find schema file
        schema_file = Path(__file__).parent / 'clickhouse_schema.sql'

        if not schema_file.exists():
            # Try alternative locations
            alternative_paths = [
                Path(__file__).parent.parent / 'database' / 'clickhouse_schema.sql',
                Path('database') / 'clickhouse_schema.sql',
                Path('clickhouse_schema.sql')
            ]

            for path in alternative_paths:
                if path.exists():
                    schema_file = path
                    break
            else:
                raise FileNotFoundError(
                    f"Could not find clickhouse_schema.sql in any expected location"
                )

        logger.info(f"✓ Found schema file: {schema_file}")

        # Execute schema
        execute_sql_file(client, schema_file)
        logger.info("✓ Schema executed successfully")

        # Verify tables
        if verify_tables(client):
            logger.info("✓ Database verification passed")
        else:
            logger.error("✗ Database verification failed")
            return False

        # Get statistics
        get_table_stats(client)

        # Close connection
        client.close()

        logger.info("=" * 60)
        logger.info("✓ Database initialization completed successfully!")
        logger.info("=" * 60)
        return True

    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"✗ Database initialization failed: {e}")
        logger.error("=" * 60)
        import traceback
        traceback.print_exc()
        return False


def drop_database(database='cyclones'):
    """Drop the entire database (use with caution!)"""
    logger.warning("=" * 60)
    logger.warning(f"DROPPING DATABASE: {database}")
    logger.warning("=" * 60)

    try:
        client = get_clickhouse_client()
        client.command(f"DROP DATABASE IF EXISTS {database}")
        logger.info(f"✓ Database '{database}' dropped")
        client.close()
        return True
    except Exception as e:
        logger.error(f"✗ Failed to drop database: {e}")
        return False


def reset_database():
    """Drop and recreate the database"""
    logger.info("Resetting database...")

    if drop_database():
        logger.info("Database dropped, initializing fresh...")
        return initialize_database()
    else:
        logger.error("Failed to drop database")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='ClickHouse Database Initialization')
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Drop and recreate the database (WARNING: destroys all data)'
    )
    parser.add_argument(
        '--verify',
        action='store_true',
        help='Only verify tables without modifying'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show table statistics'
    )

    args = parser.parse_args()

    if args.reset:
        confirm = input("This will DELETE ALL DATA. Type 'yes' to confirm: ")
        if confirm.lower() == 'yes':
            success = reset_database()
        else:
            logger.info("Reset cancelled")
            success = False
    elif args.verify:
        try:
            client = get_clickhouse_client()
            success = verify_tables(client)
            client.close()
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            success = False
    elif args.stats:
        try:
            client = get_clickhouse_client()
            get_table_stats(client)
            client.close()
            success = True
        except Exception as e:
            logger.error(f"Stats failed: {e}")
            success = False
    else:
        success = initialize_database()

    sys.exit(0 if success else 1)