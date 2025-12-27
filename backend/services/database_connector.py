import psycopg2
from typing import List, Dict, Any


class DatabaseConnector:
    """Database connector for fetching tables from various databases"""

    def __init__(self, db_type: str, host: str, port: int, database: str, user: str, password: str):
        self.db_type = db_type
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def _get_postgres_connection(self):
        """Get PostgreSQL connection"""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )

    def get_tables(self) -> List[str]:
        """Get list of tables from the database"""
        if self.db_type in ["postgres", "postgresql"]:
            return self._get_postgres_tables()
        elif self.db_type in ["mysql", "mariadb"]:
            return self._get_mysql_tables()
        else:
            raise NotImplementedError(f"Database type {self.db_type} not supported yet")

    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """Get schema (columns) of a specific table"""
        if self.db_type in ["postgres", "postgresql"]:
            return self._get_postgres_schema(table_name)
        elif self.db_type in ["mysql", "mariadb"]:
            return self._get_mysql_schema(table_name)
        else:
            raise NotImplementedError(f"Database type {self.db_type} not supported yet")

    def _get_postgres_tables(self) -> List[str]:
        try:
            conn = self._get_postgres_connection()
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

    def _get_postgres_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get columns for a PostgreSQL table"""
        try:
            conn = self._get_postgres_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            
            columns = [
                {
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[2] == 'YES'
                }
                for row in cursor.fetchall()
            ]
            
            cursor.close()
            conn.close()
            
            return columns
        except Exception as e:
            raise Exception(f"Failed to fetch schema for {table_name}: {str(e)}")

    def _get_mysql_tables(self) -> List[str]:
        """Get list of tables from MySQL/MariaDB (placeholder for future implementation)"""
        raise NotImplementedError("MySQL/MariaDB support coming soon")

    def _get_mysql_schema(self, table_name: str) -> List[Dict[str, str]]:
        """Get schema from MySQL/MariaDB (placeholder)"""
        raise NotImplementedError("MySQL/MariaDB support coming soon")
