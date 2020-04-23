FROM dsprocks2020/flinkimage:4GB

ADD target/debs-challenge-0.1.jar /app/

WORKDIR /opt
ENTRYPOINT ["bash", "entrypoint.sh"]
