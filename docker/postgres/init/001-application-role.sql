-- Local-development credentials only. Hosted deployments provision this role and
-- its secret outside the repository, while Alembic grants only the required access.
DO $role$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'abacus_app'
    ) THEN
        CREATE ROLE abacus_app
            LOGIN
            NOSUPERUSER
            NOCREATEDB
            NOCREATEROLE
            NOINHERIT
            NOBYPASSRLS
            PASSWORD 'abacus_app_local_only';
    END IF;
END
$role$;

GRANT CONNECT ON DATABASE abacus TO abacus_app;
