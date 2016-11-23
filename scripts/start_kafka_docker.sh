#!/bin/bash
git clone git://github.com/wurstmeister/kafka-docker/
cd kafka-docker
git checkout ff8f9d3cddaa25c02aa3a17567ac557334bc6b66
docker-compose up -d
