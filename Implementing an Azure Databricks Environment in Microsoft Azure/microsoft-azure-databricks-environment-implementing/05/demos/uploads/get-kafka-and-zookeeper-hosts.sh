# Set cluster variables
export KAFKA_USERNAME="..."
export KAFKA_PASSWORD="..."
export KAFKA_CLUSTERNAME="..."

# Get Kafka broker hostnames
export KAFKA_BROKERS=$(curl -u $KAFKA_USERNAME:$KAFKA_PASSWORD -G "https://$KAFKA_CLUSTERNAME.azurehdinsight.net/api/v1/clusters/$KAFKA_CLUSTERNAME/services/KAFKA/components/KAFKA_BROKER" | jq -r '["\(.host_components[].HostRoles.host_name):9092"] | join(",")')
echo $KAFKA_BROKERS

# Get Zookeeper node hostnames
export ZK_NODES=$(curl -u $KAFKA_USERNAME:$KAFKA_PASSWORD -G "https://$KAFKA_CLUSTERNAME.azurehdinsight.net/api/v1/clusters/$KAFKA_CLUSTERNAME/services/ZOOKEEPER/components/ZOOKEEPER_SERVER" | jq -r '["\(.host_components[].HostRoles.host_name):2181"] | join(",")')
echo $ZK_NODES