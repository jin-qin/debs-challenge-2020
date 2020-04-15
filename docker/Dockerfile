FROM dsprocks2020/flinkimage:base

ENV SERVER_IP="192.168.1.73"

ADD target/debs-challenge-0.1.jar /app/

WORKDIR /opt
ENTRYPOINT ["bash", "entrypoint.sh"]
