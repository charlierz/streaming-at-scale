#!/bin/bash

set -euo pipefail

echo "creating postgresql server"
echo ". server: $POSTGRESQL_SERVER_NAME"
echo ". database: $POSTGRESQL_DATABASE_NAME"

az extension add --name db-up \
  -o json >> log.txt

echo "postgres up"

az postgres up \
  --resource-group $RESOURCE_GROUP \
  --server-name $POSTGRESQL_SERVER_NAME \
  --database-name $POSTGRESQL_DATABASE_NAME \
  --admin-user serveradmin \
  --admin-password $POSTGRESQL_ADMIN_PASS \
  --sku-name $POSTGRESQL_SKU \
  --storage-size $POSTGRESQL_STORAGE_SIZE \
  --version 11 \
  -o json >> log.txt

az postgres server show --resource-group $RESOURCE_GROUP --name $POSTGRESQL_SERVER_NAME

echo "adding timescaledb library"
az postgres server configuration set \
    --resource-group $RESOURCE_GROUP \
    --server $POSTGRESQL_SERVER_NAME \
    --name shared_preload_libraries \
    --value timescaledb \
    -o json >> log.txt

echo "restart server to load timescaledb library"
az postgres server restart \
    --resource-group $RESOURCE_GROUP \
    --name $POSTGRESQL_SERVER_NAME \
    -o json >> log.txt

echo "connect to database with psql and run sql file"
sudo -u postgres PGPASSWORD=$POSTGRESQL_ADMIN_PASS psql \
    --host=$POSTGRESQL_SERVER_NAME.postgres.database.azure.com \
    --port=5432 \
    --username=serveradmin@$POSTGRESQL_SERVER_NAME \
    --dbname=$POSTGRESQL_DATABASE_NAME \
    -a -q -w -f ../components/azure-timescaledb/timescaledb.sql
