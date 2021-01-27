#!/bin/bash

docker-compose up -d

# a simple sleep in encoded here to wait for postgres
# in prod deployment will enhance it to include commands to check if postgres connection is live or not

sleep 20

pip install -r requirements.txt

python ./pipeline/main.py