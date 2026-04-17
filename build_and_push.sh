#!/bin/bash
# ============================================================
# Script: Build và Push Docker Image ETL
# ============================================================

IMAGE_NAME="huytrongbeou1310/bigdata_spark_postgresql"
VERSION="v1.0.0"

echo ">>> Building Docker image: $IMAGE_NAME:$VERSION..."
docker build -t $IMAGE_NAME:$VERSION .

if [ $? -eq 0 ]; then
    echo ">>> Build SUCCESS."
    echo ">>> Pushing image to Docker Hub..."
    docker push $IMAGE_NAME:$VERSION
    if [ $? -eq 0 ]; then
        echo ">>> Push SUCCESS."
        echo ">>> Next step: kubectl apply -f k8s/"
    else
        echo ">>> ERROR: Push failed. Are you logged in to Docker Hub?"
    fi
else
    echo ">>> ERROR: Build failed."
fi
