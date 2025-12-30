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
            # TODO: Implement MongoDB test
            return True, "MongoDB connection test mocked (Success)"
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
            session = boto3.Session(
                aws_access_key_id=config['access_key'],
                aws_secret_access_key=config['secret_key'],
                region_name=config['region']
            )
            s3 = session.client('s3')
            
            # Try to list objects in the bucket (checking access)
            s3.list_objects_v2(Bucket=config['bucket'], MaxKeys=1)
            
            return True, "Successfully connected to S3 bucket."
            
        except ClientError as e:
            return False, f"S3 Connection failed: {str(e)}"
        except Exception as e:
            return False, f"Error: {str(e)}"
