#!/bin/bash

bash /opt/flink/bin/start-cluster.sh

bash /opt/flink/bin/flink run -c Main /app/debs-challenge-0.1.jar