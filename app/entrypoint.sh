#!/bin/sh

echo "Waiting for DBs ..."

while ! nc -z $POSTGRES_HOST $PGPORT; do
  sleep 0.1
done

echo "PostgreSQL started"

while ! nc -z $REDIS_HOST $REDIS_PORT; do
  sleep 0.1
done

echo "Redis started"

echo "Waiting for DBs OK"


exec "$@"
