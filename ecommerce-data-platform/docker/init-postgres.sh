#!/usr/bin/env bash
# Creates the airflow database and user on PostgreSQL container first boot.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create airflow user and database
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow') THEN
            CREATE USER airflow WITH PASSWORD 'airflow';
        END IF;
    END
    \$\$;

    CREATE DATABASE airflow OWNER airflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
EOSQL
