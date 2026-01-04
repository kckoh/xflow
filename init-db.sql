-- Create Airflow database
CREATE DATABASE airflow;

-- ============ CDC Configuration ============

-- Create CDC user with replication privileges
CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'cdc_password';

-- Grant necessary permissions to CDC user
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
GRANT USAGE ON SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

-- Create publication for all tables (Debezium will use this)
-- Note: This creates a publication for capturing changes to all tables
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- Grant replication permission on the database
GRANT CONNECT ON DATABASE mydb TO cdc_user;

-- Display CDC configuration
\echo 'PostgreSQL CDC configured successfully!'
\echo '   - CDC User: cdc_user'
\echo '   - Publication: dbz_publication'
\echo '   - WAL Level: logical (configured in docker-compose.yml)'
