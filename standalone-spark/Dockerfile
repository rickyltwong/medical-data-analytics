FROM bitnami/spark:3.5.4-debian-12-r4

USER root

# RUN apt-get update && \
#     apt-get install -y net-tools nano && \
#     apt-get clean

RUN echo 'export _JAVA_OPTIONS="-Djava.net.preferIPv4Stack=true"' | tee -a /opt/bitnami/spark/conf/spark-env.sh 

