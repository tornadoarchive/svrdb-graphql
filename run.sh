#!/usr/bin/env bash

source .env

./wait-for-it.sh --host=$DB_HOST --port=$MYSQL_PORT -t 60 -- echo "DB is ready"

echo "Seeding database: $SEED_DB"

python seed.py

echo "Starting up app!"
#strawberry server app
uvicorn app:app --host 0.0.0.0 --port 8000
