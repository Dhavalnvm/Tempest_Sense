"""
Historical Data Ingestion
Loads historical cyclone data from various sources into ClickHouse
"""
import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any
import clickhouse_connect
import requests
import json
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HistoricalDataIngester:
    """Ingest historical cyclone data"""

    def __init__(self):
        self.client = self._connect_clickhouse()
        self.batch_size = 1000
        self.batch_buffer = []

    def _connect_clickhouse(self):
        """Connect to ClickHouse"""
        host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        port = int(os.getenv('CLICKHOUSE_PORT', '9000'))
        user = os.getenv('CLICKHOUSE_USER', 'admin')
        password = os.getenv('CLICKHOUSE_PASSWORD', 'admin123')
        database = os.getenv('CLICKHOUSE_DATABASE', 'cyclones')

        logger.info(f"Connecting to ClickHouse at {host}:{port}/{database}")

        return clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database
        )

    def ingest_from_json(self, json_file: str):
        """
        Ingest from JSON file format
        Expected format: array of cyclone position objects
        """
        logger.info(f"Loading data from {json_file}")

        try:
            with open(json_file, 'r') as f:
                data = json.load(f)

            if not isinstance(data, list):
                data = [data]

            logger.info(f"Found {len(data)} records")

            for record in data:
                self._add_to_batch(record)

            self._flush_batch()

            logger.info(f"✅ Successfully ingested {len(data)} records")
            return True

        except Exception as e:
            logger.error(f"Failed to ingest from JSON: {e}")
            return False

    def ingest_from_csv(self, csv_file: str):
        """
        Ingest from CSV file
        Expected columns: id,name,basin,latitude,longitude,wind,pressure,timestamp
        """
        import csv

        logger.info(f"Loading data from {csv_file}")

        try:
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                count = 0

                for row in reader:
                    record = {
                        'id': row.get('id', ''),
                        'name': row.get('name', 'UNNAMED'),
                        'basin': row.get('basin', 'Unknown'),
                        'classification': row.get('classification', 'Unknown'),
                        'intensity': row.get('intensity', 'Unknown'),
                        'latitude': float(row.get('latitude', 0)),
                        'longitude': float(row.get('longitude', 0)),
                        'movement_speed': float(row.get('movement_speed', 0)),
                        'movement_direction': float(row.get('movement_direction', 0)),
                        'central_pressure': float(row.get('pressure', 0)) if row.get('pressure') else None,
                        'max_sustained_wind': float(row.get('wind', 0)) if row.get('wind') else None,
                        'timestamp': row.get('timestamp', datetime.utcnow().isoformat())
                    }

                    self._add_to_batch(record)
                    count += 1

                self._flush_batch()

                logger.info(f"✅ Successfully ingested {count} records")
                return True

        except Exception as e:
            logger.error(f"Failed to ingest from CSV: {e}")
            return False

    def ingest_from_noaa_archive(self, year: int, basin: str = 'AL'):
        """
        Fetch historical data from NOAA HURDAT2 archive
        Basin codes: AL=Atlantic, EP=East Pacific, CP=Central Pacific

        HURDAT2 format documentation:
        https://www.nhc.noaa.gov/data/hurdat/hurdat2-format.pdf
        """
        logger.info(f"Fetching NOAA HURDAT2 archive data for {year} {basin}")

        try:
            # NOAA HURDAT2 archive URLs
            urls = {
                'AL': 'https://www.nhc.noaa.gov/data/hurdat/hurdat2-1851-2023-052624.txt',
                'EP': 'https://www.nhc.noaa.gov/data/hurdat/hurdat2-nepac-1949-2023-050524.txt'
            }

            if basin not in urls:
                logger.error(f"Unsupported basin: {basin}")
                return False

            url = urls[basin]
            logger.info(f"Downloading from: {url}")

            response = requests.get(url, timeout=30)
            response.raise_for_status()

            # Parse HURDAT2 format
            lines = response.text.split('\n')
            count = 0
            current_storm = None

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # Header line: storm ID and name
                if ',' in line and len(line.split(',')) <= 4:
                    parts = [p.strip() for p in line.split(',')]
                    current_storm = {
                        'id': parts[0],
                        'name': parts[1] if len(parts) > 1 else 'UNNAMED'
                    }
                    continue

                # Data line
                if current_storm and len(line.split(',')) >= 7:
                    parts = [p.strip() for p in line.split(',')]

                    # Parse timestamp (YYYYMMDD, HHMM)
                    date_str = parts[0]
                    time_str = parts[1]
                    timestamp = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M")

                    # Only include storms from specified year
                    if timestamp.year != year:
                        continue

                    # Parse coordinates
                    lat_str = parts[4]
                    lon_str = parts[5]

                    lat = float(lat_str[:-1])
                    if lat_str[-1] == 'S':
                        lat = -lat

                    lon = float(lon_str[:-1])
                    if lon_str[-1] == 'W':
                        lon = -lon

                    # Parse wind and pressure
                    wind = float(parts[6]) if parts[6] and parts[6] != '-999' else None
                    pressure = float(parts[7]) if parts[7] and parts[7] != '-999' else None

                    record = {
                        'id': current_storm['id'],
                        'name': current_storm['name'],
                        'basin': 'Atlantic' if basin == 'AL' else 'Eastern Pacific',
                        'classification': parts[2],
                        'intensity': parts[3],
                        'latitude': lat,
                        'longitude': lon,
                        'movement_speed': 0,  # Not in HURDAT2
                        'movement_direction': 0,  # Not in HURDAT2
                        'central_pressure': pressure,
                        'max_sustained_wind': wind,
                        'timestamp': timestamp.isoformat()
                    }

                    self._add_to_batch(record)
                    count += 1

            self._flush_batch()

            logger.info(f"✅ Successfully ingested {count} historical records from HURDAT2")
            return True

        except Exception as e:
            logger.error(f"Failed to ingest from NOAA archive: {e}")
            import traceback
            traceback.print_exc()
            return False

    def generate_sample_data(self, count: int = 100):
        """Generate sample historical data for testing"""
        import random

        logger.info(f"Generating {count} sample records")

        basins = ['Atlantic', 'Eastern Pacific', 'Western Pacific']
        classifications = ['Tropical Depression', 'Tropical Storm', 'Hurricane']
        intensities = ['Category 1', 'Category 2', 'Category 3', 'Category 4', 'Category 5']
        names = ['ALPHA', 'BETA', 'GAMMA', 'DELTA', 'EPSILON', 'ZETA', 'ETA', 'THETA']

        base_time = datetime.utcnow() - timedelta(days=30)

        for i in range(count):
            storm_num = i // 20  # Group into storms

            record = {
                'id': f'SAMPLE{storm_num:03d}2024',
                'name': names[storm_num % len(names)],
                'basin': basins[storm_num % len(basins)],
                'classification': random.choice(classifications),
                'intensity': random.choice(intensities),
                'latitude': 10 + random.uniform(-15, 15) + (i % 20) * 0.5,
                'longitude': -50 + random.uniform(-20, 20) + (i % 20) * 0.3,
                'movement_speed': random.uniform(5, 30),
                'movement_direction': random.uniform(0, 360),
                'central_pressure': random.uniform(950, 1010),
                'max_sustained_wind': random.uniform(30, 150),
                'timestamp': (base_time + timedelta(hours=i * 6)).isoformat()
            }

            self._add_to_batch(record)

        self._flush_batch()
        logger.info(f"✅ Generated {count} sample records")
        return True

    def _add_to_batch(self, record: Dict[str, Any]):
        """Add record to batch buffer"""
        try:
            row = [
                record.get('id', ''),
                record.get('name', 'UNNAMED'),
                record.get('basin', 'Unknown'),
                record.get('classification', 'Unknown'),
                record.get('intensity', 'Unknown'),
                float(record.get('latitude', 0)),
                float(record.get('longitude', 0)),
                float(record.get('movement_speed', 0)),
                float(record.get('movement_direction', 0)),
                float(record.get('central_pressure', 0)) if record.get('central_pressure') else 0,
                float(record.get('max_sustained_wind', 0)) if record.get('max_sustained_wind') else 0,
                datetime.fromisoformat(record.get('timestamp', datetime.utcnow().isoformat()).replace('Z', '+00:00')),
                'HISTORICAL'
            ]

            self.batch_buffer.append(row)

            if len(self.batch_buffer) >= self.batch_size:
                self._flush_batch()

        except Exception as e:
            logger.error(f"Error adding record to batch: {e}")

    def _flush_batch(self):
        """Flush batch to ClickHouse"""
        if not self.batch_buffer:
            return

        try:
            self.client.insert(
                'cyclone_positions',
                self.batch_buffer,
                column_names=[
                    'id', 'name', 'basin', 'classification', 'intensity',
                    'latitude', 'longitude', 'movement_speed', 'movement_direction',
                    'central_pressure', 'max_sustained_wind', 'timestamp', 'data_source'
                ]
            )

            count = len(self.batch_buffer)
            logger.info(f"✅ Inserted batch of {count} records")
            self.batch_buffer = []

        except Exception as e:
            logger.error(f"Failed to insert batch: {e}")
            self.batch_buffer = []

    def get_statistics(self):
        """Get ingestion statistics"""
        try:
            # Total records
            result = self.client.query("SELECT count() FROM cyclone_positions")
            total = result.result_rows[0][0]

            # By data source
            result = self.client.query(
                "SELECT data_source, count() FROM cyclone_positions GROUP BY data_source"
            )
            by_source = {row[0]: row[1] for row in result.result_rows}

            # By basin
            result = self.client.query(
                "SELECT basin, count() FROM cyclone_positions GROUP BY basin ORDER BY count() DESC"
            )
            by_basin = {row[0]: row[1] for row in result.result_rows}

            # Time range
            result = self.client.query(
                "SELECT min(timestamp), max(timestamp) FROM cyclone_positions"
            )
            time_range = result.result_rows[0] if result.result_rows else (None, None)

            stats = {
                'total_records': total,
                'by_source': by_source,
                'by_basin': by_basin,
                'time_range': {
                    'earliest': str(time_range[0]) if time_range[0] else None,
                    'latest': str(time_range[1]) if time_range[1] else None
                }
            }

            # Print statistics
            print("\n" + "=" * 60)
            print("📊 INGESTION STATISTICS")
            print("=" * 60)
            print(f"Total Records: {stats['total_records']:,}")
            print(f"\nBy Data Source:")
            for source, count in stats['by_source'].items():
                print(f"  {source}: {count:,}")
            print(f"\nBy Basin:")
            for basin, count in stats['by_basin'].items():
                print(f"  {basin}: {count:,}")
            print(f"\nTime Range:")
            print(f"  Earliest: {stats['time_range']['earliest']}")
            print(f"  Latest: {stats['time_range']['latest']}")
            print("=" * 60)

            return stats

        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {}

    def cleanup(self):
        """Close connections"""
        if self.client:
            self.client.close()


def main():
    parser = argparse.ArgumentParser(
        description='Historical Cyclone Data Ingestion',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate sample data
  python ingest_historical.py --sample 1000

  # Import from JSON file
  python ingest_historical.py --json data/cyclones.json

  # Import from CSV file
  python ingest_historical.py --csv data/cyclones.csv

  # Import NOAA HURDAT2 data for 2023
  python ingest_historical.py --noaa-year 2023 --basin AL

  # Show statistics
  python ingest_historical.py --stats
        """
    )
    parser.add_argument(
        '--json',
        type=str,
        help='Path to JSON file to ingest'
    )
    parser.add_argument(
        '--csv',
        type=str,
        help='Path to CSV file to ingest'
    )
    parser.add_argument(
        '--sample',
        type=int,
        metavar='COUNT',
        help='Generate sample data (specify count)'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show ingestion statistics'
    )
    parser.add_argument(
        '--noaa-year',
        type=int,
        help='Ingest NOAA HURDAT2 archive for specific year (1851-2023)'
    )
    parser.add_argument(
        '--basin',
        type=str,
        default='AL',
        choices=['AL', 'EP'],
        help='Basin code for NOAA archive (AL=Atlantic, EP=East Pacific)'
    )

    args = parser.parse_args()

    ingester = HistoricalDataIngester()

    try:
        success = False

        if args.json:
            success = ingester.ingest_from_json(args.json)
        elif args.csv:
            success = ingester.ingest_from_csv(args.csv)
        elif args.sample:
            success = ingester.generate_sample_data(args.sample)
        elif args.noaa_year:
            success = ingester.ingest_from_noaa_archive(args.noaa_year, args.basin)

        # Always show stats if requested or after ingestion
        if args.stats or success:
            ingester.get_statistics()

        if not (args.json or args.csv or args.sample or args.noaa_year or args.stats):
            parser.print_help()

    finally:
        ingester.cleanup()

    sys.exit(0 if success or args.stats else 1)


if __name__ == "__main__":
    main()