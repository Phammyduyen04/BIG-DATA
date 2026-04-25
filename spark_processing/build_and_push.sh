#!/bin/bash
# ============================================================
# Script: Build và Push Docker Image ETL (ISOLATED)
# Must be run from within spark_processing/
# ============================================================

IMAGE_NAME="huytrongbeou1310/bigdata_spark_postgresql"
VERSION="v1.5.0-csv-bench"

echo ">>> [ISOLATED BUILD] Building Docker image: $IMAGE_NAME:$VERSION..."
# Context is the current folder (spark_processing/)
docker build -t $IMAGE_NAME:$VERSION .

if [ $? -eq 0 ]; then
    echo ">>> Build SUCCESS."
    echo ">>> Pushing image to Docker Hub..."
    docker push $IMAGE_NAME:$VERSION
    if [ $? -eq 0 ]; then
        echo ">>> Push SUCCESS."
        echo ">>> Next step on VM: kubectl apply -f k8s/"
    else
        echo ">>> ERROR: Push failed. Are you logged in to Docker Hub?"
    fi
else
    echo ">>> ERROR: Build failed."
fi
