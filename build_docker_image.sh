#!/bin/bash

mvn clean package

# docker rmi dsprocks2020/solution # uncomment this line to remove the old image.

cp ./docker/Dockerfile ./Dockerfile

docker build -t dsprocks2020/solution --no-cache .

rm ./Dockerfile
