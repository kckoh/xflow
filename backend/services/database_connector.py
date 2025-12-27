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

    def _get_postgres_schema(self, table_name: str) -> List[Dict[str, str]]:
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()
            
            # Get columns for the specific table
            # Handle potential schema prefix (e.g., 'public.users')
            if '.' in table_name:
                schema, table = table_name.split('.', 1)
            else:
                schema, table = 'public', table_name

            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = %s
                ORDER BY ordinal_position
            """, (table, schema))
            
            columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
            
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
