import psycopg2
from typing import List


class DatabaseConnector:
    """Database connector for fetching tables from various databases"""

    def __init__(self, db_type: str, host: str, port: int, database: str, user: str, password: str):
        self.db_type = db_type
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def get_tables(self) -> List[str]:
        """Get list of tables from the database"""
        if self.db_type in ["postgres", "postgresql"]:
            return self._get_postgres_tables()
        elif self.db_type in ["mysql", "mariadb"]:
            return self._get_mysql_tables()
        else:
            raise NotImplementedError(f"Database type {self.db_type} not supported yet")



    def _get_postgres_tables(self) -> List[str]:
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()
            
            # Get all tables from public schema
            cursor.execute("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY tablename
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            return tables
        except Exception as e:
            raise Exception(f"Failed to connect to PostgreSQL: {str(e)}")

    def _get_mysql_tables(self) -> List[str]:
        """Get list of tables from MySQL/MariaDB (placeholder for future implementation)"""
        raise NotImplementedError("MySQL/MariaDB support coming soon")
