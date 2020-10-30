# """
# Name: Dockerfile
# Description: Dockerfile
# Created by: Masato Shima
# Created on: 2020/10/30
# """

FROM centos:centos7.7.1908

WORKDIR /

RUN yum -y install python3 python3-pip \
 && yum clean all \
 && rm -rf /var/cache/yum/* \
 && pip install confluent-kafka

# ENTRYPOINT
ENTRYPOINT tail -f /dev/null

# End
