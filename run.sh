#!/bin/bash

COUNTRY=$1
LOCALITY=$2

if [ -z "$COUNTRY" ] || [ -z "$LOCALITY" ]; then
    echo "Usage: $0 <COUNTRY> <LOCALITY>"
    echo "Example: $0 US 'New York'"
    exit 1
fi

echo "Starting OpenAQ data pipeline for country: $COUNTRY, locality: $LOCALITY"

# Start PostgreSQL
echo "Starting PostgreSQL..."
docker-compose up -d postgres

# Run the OpenAQ application
echo "Running OpenAQ data pipeline..."
export COUNTRY=$COUNTRY
export LOCALITY=$LOCALITY
docker-compose --profile run up openaq-app

echo "OpenAQ pipeline completed!"