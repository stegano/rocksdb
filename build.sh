#!/bin/bash
set -e
export DOCKER_HOST=${DOCKER_HOST:-ssh://root@test-srv3.hq.bmux}

echo "Building image..."
docker build --iidfile prebuilds.iid .

echo "Extracting prebuilds from image..."
IMG=$(cat prebuilds.iid)
ID=$(docker create $IMG)
docker cp "$ID:/rocks-level/prebuilds" ./

echo "Cleaning up..."
docker rm $ID > /dev/null
rm prebuilds.iid

echo "All done!"