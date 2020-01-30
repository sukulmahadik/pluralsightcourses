export TOPIC_NAME="tweets"
export ZOOKEEPER_SERVERS="your comma separated zookeeper servers"
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --replication-factor 3 --partitions 4 --topic $TOPIC_NAME --zookeeper $ZOOKEEPER_SERVERS
