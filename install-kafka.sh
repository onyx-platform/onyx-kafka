set -x
set -e
if [ ! -e kafka_2.9.2-0.8.2.1 ]; then
  wget http://mirror.olnevhost.net/pub/apache/kafka/0.8.2.1/kafka_2.9.2-0.8.2.1.tgz
  tar -xvf kafka_2.9.2-0.8.2.1.tgz
fi
