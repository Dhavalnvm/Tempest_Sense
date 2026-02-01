"""
NASA Data Synchronization
Fetches NASA satellite data for cyclone monitoring and enhances predictions

NASA Data Sources:
1. EONET (Earth Observatory Natural Event Tracker) - Real-time severe storm events
2. POWER API - Weather parameters for cyclone analysis
3. Worldview/GIBS - Satellite imagery

Note: This provides supplementary data to NOAA's primary cyclone tracking
"""
import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
import json
import clickhouse_connect

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NASADataSync:
    """Synchronize NASA Earth observation data for cyclone tracking"""

    # NASA EONET API endpoint
    EONET_API = "https://eonet.gsfc.nasa.gov/api/v3"

    # NASA POWER API endpoint
    POWER_API = "https://power.larc.nasa.gov/api/temporal/hourly/point"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CycloneTracker/1.0 (NASA Data Integration)'
        })
        self.clickhouse_client = None

    def _connect_clickhouse(self):
        """Connect to ClickHouse for data storage"""
        if self.clickhouse_client:
            return self.clickhouse_client

        host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        port = int(os.getenv('CLICKHOUSE_PORT', '9000'))
        user = os.getenv('CLICKHOUSE_USER', 'admin')
        password = os.getenv('CLICKHOUSE_PASSWORD', 'admin123')
        database = os.getenv('CLICKHOUSE_DATABASE', 'cyclones')

        self.clickhouse_client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database
        )

        return self.clickhouse_client

    def fetch_eonet_events(self, days: int = 30, category: str = "severeStorms"):
        """
        Fetch natural events from NASA EONET

        Args:
            days: Number of days to look back
            category: Event category (severeStorms, wildfires, etc.)

        Returns:
            List of events
        """
        logger.info(f"Fetching NASA EONET events for last {days} days")

        try:
            # Calculate date range
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=days)

            url = f"{self.EONET_API}/events"
            params = {
                'category': category,
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d'),
                'status': 'all'
            }

            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            events = data.get('events', [])

            logger.info(f"✅ Fetched {len(events)} events from EONET")

            # Filter for cyclone-related events
            cyclone_events = []
            for event in events:
                title = event.get('title', '').lower()
                if any(keyword in title for keyword in ['cyclone', 'hurricane', 'typhoon', 'storm']):
                    cyclone_events.append(event)

            logger.info(f"✅ Found {len(cyclone_events)} cyclone-related events")

            return cyclone_events

        except Exception as e:
            logger.error(f"Failed to fetch EONET events: {e}")
            return []

    def fetch_power_data(
            self,
            latitude: float,
            longitude: float,
            start_date: str,
            end_date: str,
            parameters: Optional[List[str]] = None
    ):
        """
        Fetch NASA POWER meteorological data for a location

        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            start_date: Start date (YYYYMMDD)
            end_date: End date (YYYYMMDD)
            parameters: List of parameters to fetch

        Returns:
            Weather data dictionary
        """
        if parameters is None:
            # Key parameters for cyclone analysis
            parameters = [
                'T2M',  # Temperature at 2 Meters
                'PRECTOTCORR',  # Precipitation
                'WS10M',  # Wind Speed at 10 Meters
                'WD10M',  # Wind Direction at 10 Meters
                'PS',  # Surface Pressure
                'RH2M',  # Relative Humidity at 2 Meters
                'CLOUD_AMT'  # Cloud Amount
            ]

        logger.info(f"Fetching NASA POWER data for ({latitude}, {longitude})")

        try:
            url = self.POWER_API
            params = {
                'parameters': ','.join(parameters),
                'community': 'RE',
                'longitude': longitude,
                'latitude': latitude,
                'start': start_date,
                'end': end_date,
                'format': 'JSON'
            }

            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()

            data = response.json()

            if 'properties' in data and 'parameter' in data['properties']:
                logger.info(f"✅ Fetched POWER data for {len(parameters)} parameters")
                return data['properties']['parameter']
            else:
                logger.warning("No parameter data in POWER response")
                return {}

        except Exception as e:
            logger.error(f"Failed to fetch POWER data: {e}")
            return {}

    def enrich_cyclone_with_nasa_data(self, storm_id: str):
        """
        Enrich existing cyclone data with NASA satellite observations

        Args:
            storm_id: Cyclone ID to enrich
        """
        logger.info(f"Enriching cyclone {storm_id} with NASA data")

        try:
            client = self._connect_clickhouse()

            # Get cyclone position history
            query = f"""
                SELECT latitude, longitude, timestamp
                FROM cyclone_positions
                WHERE id = '{storm_id}'
                ORDER BY timestamp DESC
                LIMIT 1
            """

            result = client.query(query)

            if not result.result_rows:
                logger.warning(f"No data found for storm {storm_id}")
                return False

            row = result.result_rows[0]
            lat, lon, timestamp = float(row[0]), float(row[1]), row[2]

            logger.info(f"Latest position: {lat:.2f}, {lon:.2f} at {timestamp}")

            # Fetch NASA POWER data for this location
            # Get data for past 24 hours
            end_date = datetime.now()
            start_date = end_date - timedelta(days=1)

            power_data = self.fetch_power_data(
                latitude=lat,
                longitude=lon,
                start_date=start_date.strftime('%Y%m%d'),
                end_date=end_date.strftime('%Y%m%d')
            )

            if power_data:
                logger.info(f"✅ Retrieved weather parameters from NASA POWER")
                # TODO: Store enriched data in a separate table
                # For now, just log the data
                for param, values in power_data.items():
                    if values:
                        latest_value = list(values.values())[-1] if isinstance(values, dict) else None
                        if latest_value:
                            logger.info(f"  {param}: {latest_value}")

            return True

        except Exception as e:
            logger.error(f"Failed to enrich cyclone data: {e}")
            return False

    def sync_all_active_cyclones(self):
        """Sync NASA data for all currently active cyclones"""
        logger.info("Syncing NASA data for all active cyclones")

        try:
            client = self._connect_clickhouse()

            # Get all active cyclones from last 6 hours
            query = """
                    SELECT DISTINCT id
                    FROM cyclone_positions
                    WHERE timestamp >= now() - INTERVAL 6 HOUR
                      AND data_source = 'NOAA' \
                    """

            result = client.query(query)
            storm_ids = [row[0] for row in result.result_rows]

            logger.info(f"Found {len(storm_ids)} active cyclones")

            success_count = 0
            for storm_id in storm_ids:
                if self.enrich_cyclone_with_nasa_data(storm_id):
                    success_count += 1

            logger.info(f"✅ Successfully synced {success_count}/{len(storm_ids)} cyclones")

            return True

        except Exception as e:
            logger.error(f"Failed to sync active cyclones: {e}")
            return False

    def fetch_satellite_imagery_metadata(self, latitude: float, longitude: float):
        """
        Get satellite imagery metadata from NASA Worldview/GIBS

        Note: This returns metadata URLs, not the actual images
        Actual imagery would require additional processing
        """
        logger.info("Fetching satellite imagery metadata")

        # NASA GIBS/Worldview imagery layers for cyclone monitoring
        layers = [
            'VIIRS_SNPP_CorrectedReflectance_TrueColor',  # True color
            'MODIS_Terra_CorrectedReflectance_TrueColor',  # True color
            'MODIS_Aqua_Water_Vapor_5km_Day',  # Water vapor
        ]

        # Construct Worldview URLs
        date_str = datetime.utcnow().strftime('%Y-%m-%d')

        imagery_urls = []
        for layer in layers:
            url = (
                f"https://worldview.earthdata.nasa.gov/?v="
                f"{longitude - 10},{latitude - 10},{longitude + 10},{latitude + 10}"
                f"&l={layer}&t={date_str}"
            )
            imagery_urls.append({
                'layer': layer,
                'url': url,
                'date': date_str
            })

        logger.info(f"✅ Generated {len(imagery_urls)} imagery URLs")

        for img in imagery_urls:
            logger.info(f"  {img['layer']}: {img['url']}")

        return imagery_urls

    def create_nasa_data_table(self):
        """Create table for storing NASA supplementary data"""
        logger.info("Creating NASA data table")

        try:
            client = self._connect_clickhouse()

            sql = """
                  CREATE TABLE IF NOT EXISTS nasa_cyclone_data \
                  ( \
                      storm_id \
                      String, \
                      latitude \
                      Float64, \
                      longitude \
                      Float64, \
                      timestamp \
                      DateTime, \
                      parameter_name \
                      String, \
                      parameter_value \
                      Float32, \
                      data_source \
                      String, \
                      ingestion_time \
                      DateTime \
                      DEFAULT \
                      now \
                  ( \
                  ),
                      INDEX idx_storm_id storm_id TYPE bloom_filter GRANULARITY 1
                      ) ENGINE = MergeTree \
                  ( \
                  )
                      PARTITION BY toYYYYMM \
                  ( \
                      timestamp \
                  )
                      ORDER BY \
                  ( \
                      storm_id, \
                      timestamp, \
                      parameter_name \
                  )
                      TTL timestamp + INTERVAL 90 DAY \
                  """

            client.command(sql)
            logger.info("✅ NASA data table created")

            return True

        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            return False

    def cleanup(self):
        """Close connections"""
        if self.clickhouse_client:
            self.clickhouse_client.close()
        self.session.close()


def main():
    parser = argparse.ArgumentParser(
        description='NASA Data Synchronization for Cyclone Tracking',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch NASA EONET events
  python sync_nasa_data.py --eonet --days 30

  # Enrich a specific cyclone with NASA data
  python sync_nasa_data.py --enrich AL012025

  # Sync all active cyclones
  python sync_nasa_data.py --sync-all

  # Create NASA data table
  python sync_nasa_data.py --create-table

  # Get satellite imagery links for a location
  python sync_nasa_data.py --imagery --lat 25.5 --lon -80.3
        """
    )

    parser.add_argument(
        '--eonet',
        action='store_true',
        help='Fetch NASA EONET severe storm events'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=30,
        help='Number of days to look back for EONET events'
    )
    parser.add_argument(
        '--enrich',
        type=str,
        metavar='STORM_ID',
        help='Enrich specific cyclone with NASA data'
    )
    parser.add_argument(
        '--sync-all',
        action='store_true',
        help='Sync NASA data for all active cyclones'
    )
    parser.add_argument(
        '--create-table',
        action='store_true',
        help='Create NASA data table in ClickHouse'
    )
    parser.add_argument(
        '--imagery',
        action='store_true',
        help='Get satellite imagery metadata'
    )
    parser.add_argument(
        '--lat',
        type=float,
        help='Latitude for imagery query'
    )
    parser.add_argument(
        '--lon',
        type=float,
        help='Longitude for imagery query'
    )

    args = parser.parse_args()

    syncer = NASADataSync()

    try:
        if args.create_table:
            syncer.create_nasa_data_table()

        if args.eonet:
            events = syncer.fetch_eonet_events(days=args.days)
            print(f"\n{'=' * 60}")
            print(f"NASA EONET EVENTS ({len(events)} found)")
            print(f"{'=' * 60}")
            for event in events:
                print(f"\nEvent: {event.get('title')}")
                print(f"  ID: {event.get('id')}")
                print(f"  Categories: {[c['title'] for c in event.get('categories', [])]}")
                if event.get('geometry'):
                    coords = event['geometry'][0].get('coordinates', [])
                    if coords:
                        print(f"  Location: {coords}")

        if args.enrich:
            syncer.enrich_cyclone_with_nasa_data(args.enrich)

        if args.sync_all:
            syncer.sync_all_active_cyclones()

        if args.imagery and args.lat is not None and args.lon is not None:
            imagery = syncer.fetch_satellite_imagery_metadata(args.lat, args.lon)

        if not any([args.eonet, args.enrich, args.sync_all, args.create_table, args.imagery]):
            parser.print_help()

    finally:
        syncer.cleanup()


if __name__ == "__main__":
    main()