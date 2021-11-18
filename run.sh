#!/usr/bin/env bash

./wait-for-it.sh db:3306 -- echo "DB is ready"

echo "Seeding database: $SEED_DB"

if [ "$SEED_DB" == "true" ]; then
  python ./seed.py
fi

echo "Starting up app!"
strawberry server app

