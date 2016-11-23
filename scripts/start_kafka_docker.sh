#!/bin/bash
git clone git://github.com/wurstmeister/kafka-docker/
cd kafka-docker
# HEAD is incompatible with circleci's docker
git checkout ff8f9d3cddaa25c02aa3a17567ac557334bc6b66
docker pull wurstmeister/zookeeper:latest
# if we don't build with --rm=false docker-compose up will fail on circle
docker build --rm=false .
docker-compose up -d
