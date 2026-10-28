CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS marts;

SELECT format('CREATE ROLE grafana_reader LOGIN PASSWORD %L', :'grafana_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') \gexec
ALTER ROLE grafana_reader PASSWORD :'grafana_password';

SELECT format('CREATE ROLE powerbi_reader LOGIN PASSWORD %L', :'powerbi_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'powerbi_reader') \gexec
ALTER ROLE powerbi_reader PASSWORD :'powerbi_password';

GRANT CONNECT ON DATABASE :"dbname" TO grafana_reader, powerbi_reader;

GRANT USAGE ON SCHEMA ops, marts TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA ops, marts TO grafana_reader;
GRANT USAGE ON SCHEMA marts TO powerbi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO powerbi_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT SELECT ON TABLES TO grafana_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO grafana_reader, powerbi_reader;
