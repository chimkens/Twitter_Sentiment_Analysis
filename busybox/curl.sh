#!/bin/bash
echo "curl entered"
curl -X POST "http://kafka-connect:8083/connectors" -H "Content-Type: application/json" -d @connector.json
echo "curl.sh ran"
