@echo off
docker pull apache/kafka:latest
docker run -d --name kafka -p 9092:9092 apache/kafka