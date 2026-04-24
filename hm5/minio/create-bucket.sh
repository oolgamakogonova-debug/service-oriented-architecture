#!/bin/bash
set -e

mc alias set local http://minio:9000 minioadmin minioadmin123
mc mb --ignore-existing local/movie-analytics
mc anonymous set download local/movie-analytics

echo "Bucket movie-analytics created and configured."