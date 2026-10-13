#!/bin/sh
# Bootstrap Lakekeeper и регистрация warehouse поверх бакета MinIO
set -e
curl -s -X POST http://lakekeeper:8181/management/v1/bootstrap \
  -H 'Content-Type: application/json' -d '{"accept-terms-of-use": true}' || true
echo
curl -s -X POST http://lakekeeper:8181/management/v1/warehouse \
  -H 'Content-Type: application/json' -d @/init/warehouse.json || true
echo
