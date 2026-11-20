#!/bin/bash
set -e

: "${MYSQL_ROOT_PASSWORD:?MYSQL_ROOT_PASSWORD must be set}"
: "${MYSQL_SOURCE_PASSWORD:?MYSQL_SOURCE_PASSWORD must be set}"
: "${MYSQL_PIPELINE_PASSWORD:?MYSQL_PIPELINE_PASSWORD must be set}"
: "${MYSQL_ANALYTICS_PASSWORD:?MYSQL_ANALYTICS_PASSWORD must be set}"

sql_escape() {
    printf '%s' "$1" | sed "s/'/''/g"
}

source_password=$(sql_escape "$MYSQL_SOURCE_PASSWORD")
pipeline_password=$(sql_escape "$MYSQL_PIPELINE_PASSWORD")
analytics_password=$(sql_escape "$MYSQL_ANALYTICS_PASSWORD")

mysql --protocol=socket -uroot --password="$MYSQL_ROOT_PASSWORD" <<SQL
SET SESSION sql_mode = CONCAT(@@sql_mode, ',NO_BACKSLASH_ESCAPES');

CREATE USER IF NOT EXISTS 'source_user'@'%' IDENTIFIED BY '$source_password';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, INDEX, ALTER, LOCK TABLES
  ON \`retaillake_source\`.* TO 'source_user'@'%';

CREATE USER IF NOT EXISTS 'pipeline_user'@'%' IDENTIFIED BY '$pipeline_password';
GRANT SELECT ON \`retaillake_source\`.* TO 'pipeline_user'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, INDEX, ALTER, CREATE VIEW
  ON \`retaillake_serving\`.* TO 'pipeline_user'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, INDEX, ALTER
  ON \`retaillake_metadata\`.* TO 'pipeline_user'@'%';

CREATE USER IF NOT EXISTS 'analytics_user'@'%' IDENTIFIED BY '$analytics_password';
GRANT SELECT ON \`retaillake_serving\`.* TO 'analytics_user'@'%';
GRANT SELECT ON \`retaillake_metadata\`.\`pipeline_runs\` TO 'analytics_user'@'%';
GRANT SELECT ON \`retaillake_metadata\`.\`data_quality_results\` TO 'analytics_user'@'%';

FLUSH PRIVILEGES;
SQL
