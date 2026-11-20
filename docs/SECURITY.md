# Security

## Credentials

- Keep the working `.env` file out of version control. `.env.example` contains placeholders only; replace them with unique credentials before starting services.
- Database and Airflow passwords are required by Docker Compose and the MySQL setup script. The application does not provide usable password defaults.
- The MySQL initialization script reads role passwords from the environment. Initialization scripts run only when the MySQL data directory is first created; changing `.env` does not rotate users in an existing database.
- Do not commit credentials, private keys, local database files, or generated lakehouse data.

## Database access

The initialization script creates separate source, pipeline, and analytics users. The analytics user receives read access to serving tables and selected metadata tables. The pipeline user can read source data and write serving and metadata data.

## Deployment boundary

The supplied Compose configuration is intended for local development and publishes the database, dashboard, and Airflow ports to the host. It does not configure production ingress controls, TLS, secret management, or a hardened Airflow metadata database. Review and secure these boundaries before deploying beyond a trusted local environment.
