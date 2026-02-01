"""
Health Check Utilities
Monitors system health and component availability
"""
import os
import time
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import clickhouse_connect
import redis
from kafka import KafkaAdminClient
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class HealthStatus:
    """Health status constants"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class ComponentHealth:
    """Health check result for a component"""

    def __init__(
            self,
            name: str,
            status: str,
            message: str = "",
            latency_ms: Optional[float] = None,
            details: Optional[Dict[str, Any]] = None
    ):
        self.name = name
        self.status = status
        self.message = message
        self.latency_ms = latency_ms
        self.details = details or {}
        self.timestamp = datetime.utcnow().isoformat() + 'Z'

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        result = {
            'status': self.status,
            'timestamp': self.timestamp
        }

        if self.message:
            result['message'] = self.message

        if self.latency_ms is not None:
            result['latency_ms'] = round(self.latency_ms, 2)

        if self.details:
            result['details'] = self.details

        return result

    @property
    def is_healthy(self) -> bool:
        """Check if component is healthy"""
        return self.status == HealthStatus.HEALTHY


class HealthChecker:
    """Comprehensive health checker for all system components"""

    def __init__(self):
        self.checks: Dict[str, ComponentHealth] = {}

    def check_clickhouse(self) -> ComponentHealth:
        """Check ClickHouse database health"""
        start_time = time.time()

        try:
            host = os.getenv('CLICKHOUSE_HOST', 'localhost')
            port = int(os.getenv('CLICKHOUSE_PORT', '9000'))
            user = os.getenv('CLICKHOUSE_USER', 'admin')
            password = os.getenv('CLICKHOUSE_PASSWORD', 'admin123')
            database = os.getenv('CLICKHOUSE_DATABASE', 'cyclones')

            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=user,
                password=password,
                database=database,
                connect_timeout=5
            )

            # Test query
            result = client.query("SELECT 1")

            # Get version
            version_result = client.query("SELECT version()")
            version = version_result.result_rows[0][0] if version_result.result_rows else "unknown"

            # Get database size
            size_result = client.query(
                f"SELECT formatReadableSize(sum(bytes)) FROM system.parts WHERE database = '{database}'"
            )
            size = size_result.result_rows[0][0] if size_result.result_rows else "unknown"

            # Get table count
            tables_result = client.query(f"SELECT count() FROM system.tables WHERE database = '{database}'")
            table_count = tables_result.result_rows[0][0] if tables_result.result_rows else 0

            client.close()

            latency_ms = (time.time() - start_time) * 1000

            return ComponentHealth(
                name='clickhouse',
                status=HealthStatus.HEALTHY,
                message=f"Connected to {host}:{port}",
                latency_ms=latency_ms,
                details={
                    'version': version,
                    'database': database,
                    'tables': table_count,
                    'size': size,
                    'host': host,
                    'port': port
                }
            )

        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            logger.error(f"ClickHouse health check failed: {e}")

            return ComponentHealth(
                name='clickhouse',
                status=HealthStatus.UNHEALTHY,
                message=str(e),
                latency_ms=latency_ms
            )

    def check_redis(self) -> ComponentHealth:
        """Check Redis cache health"""
        start_time = time.time()

        try:
            host = os.getenv('REDIS_HOST', 'localhost')
            port = int(os.getenv('REDIS_PORT', '6379'))
            db = int(os.getenv('REDIS_DB', '0'))

            client = redis.Redis(
                host=host,
                port=port,
                db=db,
                socket_timeout=5,
                socket_connect_timeout=5,
                decode_responses=True
            )

            # Test ping
            client.ping()

            # Get info
            info = client.info()

            # Get memory usage
            memory_used = info.get('used_memory_human', 'unknown')

            # Get key count
            db_info = client.info('keyspace')
            key_count = 0
            if f'db{db}' in db_info:
                keys_str = db_info[f'db{db}'].get('keys', 0)
                key_count = int(keys_str) if isinstance(keys_str, (int, str)) else 0

            client.close()

            latency_ms = (time.time() - start_time) * 1000

            return ComponentHealth(
                name='redis',
                status=HealthStatus.HEALTHY,
                message=f"Connected to {host}:{port}",
                latency_ms=latency_ms,
                details={
                    'version': info.get('redis_version', 'unknown'),
                    'memory_used': memory_used,
                    'keys': key_count,
                    'host': host,
                    'port': port,
                    'db': db
                }
            )

        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            logger.error(f"Redis health check failed: {e}")

            return ComponentHealth(
                name='redis',
                status=HealthStatus.UNHEALTHY,
                message=str(e),
                latency_ms=latency_ms
            )

    def check_kafka(self) -> ComponentHealth:
        """Check Kafka broker health"""
        start_time = time.time()

        try:
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id='health_checker',
                request_timeout_ms=5000
            )

            # Get cluster metadata
            cluster_metadata = admin_client.list_topics()
            topic_count = len(cluster_metadata)

            admin_client.close()

            latency_ms = (time.time() - start_time) * 1000

            return ComponentHealth(
                name='kafka',
                status=HealthStatus.HEALTHY,
                message=f"Connected to {bootstrap_servers}",
                latency_ms=latency_ms,
                details={
                    'topics': topic_count,
                    'bootstrap_servers': bootstrap_servers
                }
            )

        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            logger.error(f"Kafka health check failed: {e}")

            return ComponentHealth(
                name='kafka',
                status=HealthStatus.UNHEALTHY,
                message=str(e),
                latency_ms=latency_ms
            )

    def check_all(self, components: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Check health of all or specified components

        Args:
            components: List of component names to check (None = all)

        Returns:
            Health check results
        """
        available_checks = {
            'clickhouse': self.check_clickhouse,
            'redis': self.check_redis,
            'kafka': self.check_kafka
        }

        # Determine which checks to run
        if components is None:
            checks_to_run = available_checks
        else:
            checks_to_run = {k: v for k, v in available_checks.items() if k in components}

        # Run checks
        results = {}
        for name, check_func in checks_to_run.items():
            try:
                result = check_func()
                results[name] = result.to_dict()
            except Exception as e:
                logger.error(f"Health check failed for {name}: {e}")
                results[name] = ComponentHealth(
                    name=name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Check failed: {str(e)}"
                ).to_dict()

        # Determine overall status
        statuses = [r['status'] for r in results.values()]

        if all(s == HealthStatus.HEALTHY for s in statuses):
            overall_status = HealthStatus.HEALTHY
        elif any(s == HealthStatus.UNHEALTHY for s in statuses):
            overall_status = HealthStatus.UNHEALTHY
        else:
            overall_status = HealthStatus.DEGRADED

        return {
            'status': overall_status,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'components': results
        }

    def is_healthy(self, components: Optional[List[str]] = None) -> bool:
        """
        Quick health check

        Args:
            components: List of component names to check

        Returns:
            True if all components are healthy
        """
        result = self.check_all(components)
        return result['status'] == HealthStatus.HEALTHY


def check_system_health() -> Dict[str, Any]:
    """
    Convenience function to check overall system health

    Returns:
        Health check results
    """
    checker = HealthChecker()
    return checker.check_all()


def is_system_healthy() -> bool:
    """
    Quick check if system is healthy

    Returns:
        True if all components are healthy
    """
    checker = HealthChecker()
    return checker.is_healthy()


# CLI interface
if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description='System Health Checker')
    parser.add_argument(
        '--component',
        '-c',
        choices=['clickhouse', 'redis', 'kafka'],
        help='Check specific component only'
    )
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output as JSON'
    )
    parser.add_argument(
        '--watch',
        type=int,
        metavar='SECONDS',
        help='Continuously check every N seconds'
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.WARNING,  # Suppress debug logs
        format='%(levelname)s - %(message)s'
    )


    def run_check():
        """Run health check and print results"""
        checker = HealthChecker()

        components = [args.component] if args.component else None
        results = checker.check_all(components)

        if args.json:
            print(json.dumps(results, indent=2))
        else:
            # Pretty print
            status_symbols = {
                HealthStatus.HEALTHY: '✓',
                HealthStatus.DEGRADED: '⚠',
                HealthStatus.UNHEALTHY: '✗'
            }

            status_colors = {
                HealthStatus.HEALTHY: '\033[32m',  # Green
                HealthStatus.DEGRADED: '\033[33m',  # Yellow
                HealthStatus.UNHEALTHY: '\033[31m',  # Red
            }
            reset = '\033[0m'

            overall = results['status']
            color = status_colors.get(overall, '')
            symbol = status_symbols.get(overall, '?')

            print(f"\n{color}Overall Status: {symbol} {overall.upper()}{reset}")
            print(f"Timestamp: {results['timestamp']}\n")

            print("Components:")
            for name, component in results['components'].items():
                comp_status = component['status']
                comp_color = status_colors.get(comp_status, '')
                comp_symbol = status_symbols.get(comp_status, '?')

                latency = component.get('latency_ms', 0)
                message = component.get('message', '')

                print(f"  {comp_color}{comp_symbol} {name:12} {comp_status:10} ({latency:.0f}ms){reset}")

                if message and comp_status != HealthStatus.HEALTHY:
                    print(f"     └─ {message}")

                if 'details' in component:
                    for key, value in component['details'].items():
                        if key not in ['host', 'port', 'database']:
                            print(f"       {key}: {value}")

            print()


    # Run check(s)
    if args.watch:
        try:
            while True:
                run_check()
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        run_check()

        # Exit with error code if unhealthy
        checker = HealthChecker()
        is_healthy = checker.is_healthy([args.component] if args.component else None)
        exit(0 if is_healthy else 1)