from typing import Dict, Any, Tuple
import boto3
from sqlalchemy import create_engine, text
from botocore.exceptions import ClientError

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
            # Use IAM role from Kubernetes pod (no access keys needed)
            # boto3 automatically uses pod's IAM role in Kubernetes
            region = config.get('region', 'us-east-1')
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
