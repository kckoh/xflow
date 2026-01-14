from typing import Dict, Any, Tuple
import boto3
import requests
from sqlalchemy import create_engine, text
from botocore.exceptions import ClientError
from kafka import KafkaConsumer

class ConnectionTester:
    """
    Factory class to test connections for different source types.
    """
    
    @staticmethod
    def test_connection(conn_type: str, config: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Test connection based on type.
        Returns (is_success, message).
        """
        if conn_type in ['postgres', 'mysql', 'mariadb']:
            return ConnectionTester._test_rdb(conn_type, config)
        elif conn_type == 's3':
            return ConnectionTester._test_s3(config)
        elif conn_type == 'mongodb':
            return ConnectionTester._test_mongodb(config)
        elif conn_type == 'api':
            return ConnectionTester._test_api(config)
        elif conn_type == 'kafka':
            return ConnectionTester._test_kafka(config)
        else:
            return False, f"Unsupported connection type: {conn_type}"

    @staticmethod
    def _test_rdb(db_type: str, config: Dict[str, Any]) -> Tuple[bool, str]:
        try:
            # Map type to SQLAlchemy driver
            driver_map = {
                'postgres': 'postgresql+psycopg2',
                'mysql': 'mysql+pymysql',
                'mariadb': 'mysql+pymysql'
            }
            driver = driver_map.get(db_type)
            
            # Construct connection string
            url = f"{driver}://{config['user_name']}:{config['password']}@{config['host']}:{config['port']}/{config['database_name']}"
            
            engine = create_engine(url, connect_args={'connect_timeout': 5})
            
            # Try to connect and run a simple query
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
            return True, "Successfully connected to database."
            
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    @staticmethod
    def _test_s3(config: Dict[str, Any]) -> Tuple[bool, str]:
        try:
            import os
            # Use IAM role from Kubernetes pod (no access keys needed)
            # boto3 automatically uses pod's IAM role in Kubernetes
            region = config.get('region', 'us-east-1')

            # Check for LocalStack endpoint (local development)
            endpoint = os.getenv('AWS_ENDPOINT') or os.getenv('S3_ENDPOINT_URL')
            if endpoint:
                s3 = boto3.client('s3', region_name=region, endpoint_url=endpoint)
            else:
                s3 = boto3.client('s3', region_name=region)
            
            bucket = config.get('bucket')
            if not bucket:
                return False, "Bucket name is required"
            
            # Test bucket access by checking if bucket exists and is accessible
            s3.head_bucket(Bucket=bucket)
            
            return True, f"Successfully connected to S3 bucket '{bucket}'."
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == '404':
                return False, f"Bucket '{config.get('bucket')}' does not exist."
            elif error_code == '403':
                return False, f"Access denied to bucket '{config.get('bucket')}'. Check IAM permissions."
            else:
                return False, f"S3 Connection failed: {str(e)}"
        except Exception as e:
            return False, f"Error: {str(e)}"

    @staticmethod
    def _test_mongodb(config: Dict[str, Any]) -> Tuple[bool, str]:
        """Test MongoDB connection."""
        try:
            from services.mongodb_connector import test_mongodb_connection

            uri = config.get('uri')
            database = config.get('database')

            if not uri or not database:
                return False, "Missing MongoDB connection parameters (uri or database)"

            return test_mongodb_connection(uri, database)

        except Exception as e:
            return False, f"MongoDB connection test failed: {str(e)}"

    @staticmethod
    def _test_api(config: Dict[str, Any]) -> Tuple[bool, str]:
        """Test REST API connection."""
        try:
            base_url = config.get('base_url')
            if not base_url:
                return False, "Base URL is required"

            # Prepare headers
            headers = config.get('headers', {}).copy() if config.get('headers') else {}

            # Add authentication
            auth_type = config.get('auth_type', 'none')
            auth_config = config.get('auth_config', {})

            if auth_type == 'api_key':
                header_name = auth_config.get('header_name')
                api_key = auth_config.get('api_key')
                if header_name and api_key:
                    headers[header_name] = api_key
            elif auth_type == 'bearer':
                token = auth_config.get('token')
                if token:
                    headers['Authorization'] = f'Bearer {token}'
            elif auth_type == 'basic':
                username = auth_config.get('username')
                password = auth_config.get('password')
                if username and password:
                    from requests.auth import HTTPBasicAuth
                    auth = HTTPBasicAuth(username, password)
                else:
                    auth = None
            else:
                auth = None

            # Test connection with a simple GET request to base URL
            # Use timeout to avoid hanging
            if auth_type == 'basic' and 'auth' in locals():
                response = requests.get(base_url, headers=headers, auth=auth, timeout=10)
            else:
                response = requests.get(base_url, headers=headers, timeout=10)

            # Consider 2xx and 3xx as successful (redirect is ok)
            # 401/403 might mean auth is required but connection works
            if response.status_code < 500:
                return True, f"Successfully connected to API (Status: {response.status_code})"
            else:
                return False, f"API returned server error (Status: {response.status_code})"

        except requests.exceptions.ConnectionError:
            return False, f"Connection failed: Unable to reach {config.get('base_url')}"
        except requests.exceptions.Timeout:
            return False, f"Connection timeout: {config.get('base_url')} did not respond in time"
        except requests.exceptions.RequestException as e:
            return False, f"Request failed: {str(e)}"
        except Exception as e:
            return False, f"API connection test failed: {str(e)}"

    @staticmethod
    def _test_kafka(config: Dict[str, Any]) -> Tuple[bool, str]:
        """Test Kafka connection by fetching cluster metadata."""
        try:
            bootstrap_servers = config.get("bootstrap_servers") or config.get("bootstrap")
            if not bootstrap_servers:
                return False, "Kafka bootstrap_servers is required"

            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                api_version_auto_timeout_ms=5000,
                request_timeout_ms=5000,
                consumer_timeout_ms=5000,
            )
            consumer.topics()
            consumer.close()
            return True, "Successfully connected to Kafka cluster."
        except Exception as e:
            return False, f"Kafka connection failed: {str(e)}"
