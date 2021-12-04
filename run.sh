#!/usr/bin/env bash

source .env

./wait-for-it.sh --host=$DB_CONTAINER --port=$MYSQL_PORT -t 60 -- echo "DB is ready"

echo "Seeding database: $SEED_DB"

if [ "$SEED_DB" == "true" ]; then
  python ./seed.py
fi

echo "Starting up app!"
strawberry server app

