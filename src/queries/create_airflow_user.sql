-- Create database and user
CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'your_secure_password';

-- Grant access to airflow database only
GRANT CONNECT ON DATABASE airflow TO airflow;

-- Switch to airflow database and grant permissions
\c airflow

GRANT USAGE, CREATE ON SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow;