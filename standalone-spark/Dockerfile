FROM bitnami/spark:3.5.4-debian-12-r4

USER root

RUN apt-get update && \
    apt-get install curl -y && \
    #     apt-get install -y net-tools nano && \
    apt-get clean

RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.261/aws-java-sdk-bundle-1.12.261.jar && \
    mv hadoop-aws-*.jar aws-java-sdk-*.jar /opt/bitnami/spark/jars/


RUN echo 'export _JAVA_OPTIONS="-Djava.net.preferIPv4Stack=true"' | tee -a /opt/bitnami/spark/conf/spark-env.sh 

