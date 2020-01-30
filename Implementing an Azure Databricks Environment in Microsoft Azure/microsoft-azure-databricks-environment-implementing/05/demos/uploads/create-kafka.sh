export RESOURCE_GROUP_NAME="streaming-data-rg"
export REGION="West US"
az group create --name $RESOURCE_GROUP_NAME --location "$REGION"
az group deployment create --resource-group $RESOURCE_GROUP_NAME --template-file kafka.json --parameters @kafka-params.json