#!/bin/bash

# Define user credentials
MYSQL_CONF="/home/asha/server-backups/mysql_dump.cnf"
MYSQL_DATABASE="base"

# Define backup directory and file name
BACKUP_DIR="/home/asha/server-backups/${MYSQL_DATABASE}"
DATE=$(date +"%Y-%m-%d")
BACKUP_FILE="$BACKUP_DIR/${MYSQL_DATABASE}_backup_$DATE.sql"

# Execute the file dump
if ! mysqldump --defaults-extra-file=$MYSQL_CONF --databases $MYSQL_DATABASE > $BACKUP_FILE; then
  echo "Error: failed to execute mysqldump" >&2
  exit 1
fi

# Compress the output
if ! gzip -f $BACKUP_FILE; then
  echo "Error: failed to compress mysqldump" >&2
  exit 1
fi

echo "Backup executed successfully: $BACKUP_FILE"