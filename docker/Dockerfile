FROM dsprocks2020/flinkimage:base

ADD target/debs-challenge-0.1.jar /app/

WORKDIR /opt
ENTRYPOINT ["bash", "entrypoint.sh"]
