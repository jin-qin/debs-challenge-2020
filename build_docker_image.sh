#!/bin/bash
# mvn clean package

cp ./docker/Dockerfile ./Dockerfile

docker build -t dsprocks2020/solution --no-cache .

rm ./Dockerfile
