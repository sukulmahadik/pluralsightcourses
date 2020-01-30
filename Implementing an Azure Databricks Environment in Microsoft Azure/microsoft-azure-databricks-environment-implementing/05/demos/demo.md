# Module Demo Streaming HDInsight Kafka Data into Azure Databricks
[Reference blog by Alena Hall](https://lenadroid.github.io/posts/kafka-hdinsight-and-spark-databricks.html) 
This document includes the script from the demo. It will follow closely to the publish video clips for the streaming data demo. You'll find steps and code in the text below, as well as following the reference blog above

## Pre-Requisites
Exercise files for course downloaded from (pluralsight.com)
CLI requirements
-[Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html#set-up-the-cli)
-Python

## Demo Building a Kafka cluster with Azure HDInsight
Before we get started building the Kafka cluster, I need to upload some demo files that I'll be using for the demo. Remeber, these are available for you in the exercise file downloads for this module.
I open Azure Cloud Shell in the Azure Portal, and I click on the file upload/download icon to 'Manage Files Shares'. I'll create a folder called StreamingDemo in the file share. Then I'll upload two JSON files and two shell script into the file share. Once complete, let's go back to Cloud Shell to finish our setup. Login to Azure CLI
First, Change to clouddrive folder in Cloud Shell.
I run ls and see the file I just uploaded. Let's take a look at each and discuss how they'll be used.
First enter code create-kafka.sh to view the shell script. This script:
    Sets environment variables for resource group and region name
    Creates a Resource Group called streaming-data-rg in the West US region using az group create, part of the Azure CLI toolset. I've included a link for more info on Azure CLI in the exercise files.
    Creates an Azure deployment based on JSON file. Notice I'm specifiy two JSON files for use for building my Azure deployment.
Now let's look at the JSON files.
First, enter  code kafka-params.json to view the JSON file where we set the Kafka cluster name. The original demo has the passwords for the cluster and SSH login in this file. I've removed them for security purposes so you'll be prompted for passwords when the shell script is run.

Next, enter code kafka.json to view the Azure Resource Manager (aka ARM) template for creating the azure deployment. As I browse the ARM template, notice it starts off with defining parameters and variables that will be used by the template. Then the real work happens in the resource section. This is where you define the resources. If you've ever heard the term 'Infrastructure as Code', this is an example of that. 
On lines 52 to 61, you see the template is defining a virtual network be built. Then a storage account and Kafka cluster are built. More info on arm templates in the exercise files.
Now let's run the create-kafka.sh shell script to begin the deployment. Note the prompts for passwords. Enter a secure and rememberable password.
run shel script - create-kafka.sh
> password: thisisanonsecPWD1234!
This will take a bit of time to complete. Once it completes we'll move onto configuring Kafka to be accessible using internal IP addresses. Normally, the Kafka brokers are set up to use internal host names.  
To access and manage the Kafka cluster, we need to browse to 
https://kafka-demokafkacluster.azurehdinsight.net

Use admin and the password set in the creation process.
Once in the portal, view information on Kafka by selecting Kafka from the list on the left.

I'll move onto the next demo of Setting up an Azure Databricks cluster.
Click configs to open the configuration page.
First, enter kafka-env in the Filter field on the upper right. Here the kafka-env template is modified to advertise IP addresses by added a code block to the end of the template.

## Demo - Configure Kafka to advertise IP addresses 

In this demo, the Kafka cluster is configured to advertise using IP address instead of hostnames.

To access and manage the Kafka cluster, we need to browse to the Azure HDInsight management portal with a web browser. The easiest way is clicking the URL link listed for the resource in the Azure Portal.

So click on Resource Group in the resource blade, scroll down to streaming-data-rg and click on it.
Scroll  to our HDInsight click, click the resource. And we see the URL listed 
So clicking the URL loads the website
Use admin and the password set in the creation process.
Once in the portal, view information on Kafka by selecting Kafka from the list on the left.
Click configs to open the configuration page.
First, enter kafka-env in the Filter field on the upper right. Here the kafka-env template is modified to advertise IP addresses by added a code block to the end of the template.
    IP_ADDRESS=$(hostname -i)
    echo advertised.listeners=$IP_ADDRESS
    sed -i.bak -e '/advertised/{/advertised@/!d;}' /usr/hdp/current/kafka-broker/conf/server.properties
    echo "advertised.listeners=PLAINTEXT://$IP_ADDRESS:9092" >> /usr/hdp/current/kafka-broker/conf/server.properties
Next, enter listeners in the Filter field on the upper right. Change the field by removing localhost and replacing it with 0.0.0.0.
Now save the configuration. Add what we changed to the notes
Click proceed anyway and Ok.
A restart is required at this time.
To prevent errors when restarting Kafka, use the Service Actions button and select Turn On Maintenance Mode. Select OK twice to complete this operation.
Now, use the Restart button and select Restart All Affected. Confirm the restart and click ok when complete.
Last piece is to remove the cluster from Maintenance mode through service actions.
Now, let's grab some information we'll need in the future: the IP addresses of the Kafka brokers and the Zookeeper servers.
Go to Summary, note the brokers are listed. Click on each one and capture the IP addresses. 
Next, click on ZooKeeper view then click each ZooKeeper server for the IPs. 
Now Kafka is ready for use. In the next demo, we’ll create a Kafka topic in HDInsight


## Demo: Create a Kafka topic in HDInsight
With our HDInsight Kafka installation ready to go, it's time to create a new topic in Kafka. This will be used to stored the retrieved tweets, and make them available to Azure Databricks.

Now switch over the the HDInsight Kafka cluster in the Azure Portal. on the resource blade, click resource groups and click on streaming-data-rg from the list.
From the list of resources, click on kafka-demokafkacluster. 
A Kafka topic is needed for our twiter data so that will be created using SSH.
In the resource settings blade, click on SSH+Cluster Login.
Choose the hostname from the dropdown, and click copy to get the ssh connection command.

Switch back to Azure Cloud shell. Enter the command 
>ssh sshuser@kafka-demokafkacluster-ssh.azurehdinsight.net
. You can paste with paste (ctrl+shift+v)

At the prompt, enter the following commands to create the topic.
> export TOPIC_NAME="tweets"
> export ZOOKEEPER_SERVERS="your comma separated zookeeper servers"
> export ZOOKEEPER_SERVERS="10.0.0.16,10.0.0.19,10.0.0.20"
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --replication-factor 3 --partitions 4 --topic $TOPIC_NAME --zookeeper $ZOOKEEPER_SERVERS

##demo Set up a Spark cluster using Azure Databricks
Either through the Azure Portal 
or
Try using script (Change script to create new RG)
In the Azure portal, we need to create a databricks workspace and a databricks cluster.
    -In the search bar, I enter Azure Databricks to find the service and begin the creation.
    -I enter the resource name of sparkcluster-ws
    -I choose my subscription
    -I add this into the existing resource group of streaming-data-rg
    -Then click create. This will create the workspace inside my existing resource group. Along with the workspace deployment will be the deployment of a separate virtual network (or Vnet) used by the workspace. This will be used in the next demo when we connect the Spark and Kafka service using virtual network peering.
> code ./create-databricks.sh
Once deployed, let's launch the workspace to create our cluster.
I'll click new cluster and create a cluster using the defaults. Now we can move on to virtual network peering when the cluster is created.

Create cluster streaming-data-cl01 w/ defaults

##Demo Peer the virtual networks
Now that we have our two clusters, they need to be accessible to each other in Azure. To do that, let's set up virtual network peering.
Start off in the azure Portal
First, go into the resource group and find the virtual network used by Kafka. Ours is kafkacluster-vnet.
In the Azure portal, click an Azure Databricks Service resource.

In the Settings blade, click the Virtual Network Peering tab.

Click the + Add Peering button.

Enter a name and then choose the vnet from the Virtual Network dropdown list. spark-peer

Make sure Allow virtual network access is set to enabled, and leave the the other two configuration options unchecked.

Click Add.

Step 2 we add the Azure Databricks vnet as a peer.
Click on Virtual Networks in the resource blade, click on the remote vnet.
Click on peering in the Settings blade and then Add.
Enter a name (kafka-peer) and choose the virtual network dropdown. In our list, we have a number of workers-vnets. These are virtual networks created by Databricks. Depending on what your environment looks like, many of these won't be there. 
Choose the one that contains the namne of the workspace, sparkcluster1-ws.
Enter the name of the clusters-peer for the peering.

Leave defaults for configuration and click OK.


## Demo Add libraries to the Spark cluster in Azure Databricks


### Create Library Workspace > shared > Create > Library
    Choose Maven
    Search Packages for Kafka
    com.tresata:spark-kafka_2.10:0.6.0
    Search for Twitter
    org.apache.bahir:spark-streaming-twitter_2.11:2.2.0

### Add Libraries to Cluster
Click the clusters icon Clusters Icon in the sidebar.
Click a cluster name.
Click the Libraries tab.
Click Install New.
In the Library Source button list, select Workspace.
Select a Workspace library.
Click Install.
To configure the library to be installed on all clusters:
Click the library.
Select the Install automatically on all clusters checkbox.
Click Confirm.

#demo Create a Twitter application
In this demo, I'll set up a twitter application that will be used to retrieve tweets. I do that through the Twitter for Developers platform. You need to apply for this so I've include the link to that in the exercise notes.

Go to developer.twitter.com and login

Click 'Create an App',
Enter a name, a description for the app, a URL for tweet attribution, and how it will be used.

I've already created an app so let's check it out and grab the keys need to access the app .

I'll copy both
#Steps to create acct
https://developer.twitter.com/en/apps

#This is an app that will be used to test streaming data with Azure HDInsight & Kafka going into Azure Databricks. It will retrieve tweets specific to Azure.

## Demo - Peering virtual networks
Demo Peer the virtual networks
Now that we have our two clusters, they need to be accessible to each other in Azure. To do that, let's set up virtual network peering. For our demo, Peering will be established between the Azure Databricks vnet of X and the HDInsight Kafka vnet of kafkacluster-vnet.
Start off in the azure Portal
First, go into the resource group and find the virtual network used by Kafka (if you don't know it) and make note of the name. Ours is kafkacluster-vnet.
In the Azure portal, click an Azure Databricks Service resource.
In the Settings blade, click the Virtual Network Peering tab.
Click the + Add Peering button.
Enter a name and then choose the vnet from the Virtual Network dropdown list. databricks-peer
Make sure Allow virtual network access is set to enabled, and leave the the other two configuration options unchecked.
Click Add.
Step 2 we add the Azure Databricks vnet as a peer in the HDInsight Kafka vnet.
Click on Virtual Networks in the resource blade, click on the Kafka vnet.
Click on peering in the Settings blade and then Add.
Enter a name (kafka-peer) and choose the virtual network dropdown. In our list, we have a number of workers-vnets. These are virtual networks created automatically by Databricks when a workspace is created. Depending on what your environment looks like, many of these won't be there. 
Choose the one that contains the name of the workspace, sparkcluster-ws.
The next text box will be greyed out with the name of spark-peer; the other peer we initiated in Azure Databricks.
Leave defaults for configuration and click OK.
Once it's complete, you'll see that the peering status is connected which means we should be good to continue.
Go back to the Azure Databricks workspace and verify Peering is set up properly, and peering status is connected. And it is.
Now are clusters are setup, and connected. Now it's time to start streaming data. Our next demo gets creating a Twitter app, and beginning the process of consuming data into the cluster.

## Demo - Author an Event Producer and 

Let's take a look at the KafkaProduce notebook.
Basically, this notebook will establish the connections between the Twitter application and Kafka, and begin streaming the selected tweets into the topic.

The notebook starts by importing needed libraries, and then setting the configuration of the Kafka brokers. This is where the IP Addresses for the brokers from the Kafka portal come into play.
Then the connection to Twitter is configure by adding the Consumer key & secret and the OAuth Access token & secret from the twitter app. As with any secrets, never store production secrets in public places. 
A twitter factory is built. This is what makes
the connection between Twitter and the Kafka cluster
Last piece is to Query twitter for tweets that include the hashtag #AzOps.
Let's run this.
In a few seconds, you'll see the events sent start to appear. This are all the tweets matching the query.
At this point, the producer notebook needs to keep running for the stream to continue.

## Demo - Consume events from Kafka using spark
Now let's take a look at the KafkaConsume notebook. This is where the tweets are fed into Azure Databricks from the Kafka topic.

Again, the notebook starts off with establishing the Kafka broker configuration. 
Once tools are imported, the connection to Kafka is setup in spark.readstream. Notice on line 10 where the topic being subscribed to is set.
Next, a value of KafkaData is defined. Basically this is the streamed data format into columns with all the information you see here.  Value represents the tweet content as you'll see.
Last item is to call KafkaData and use writeStream to send it to the console.
When the cell is run, it displays all the stream information.
Let's say you only want to see the tweets and the timestamp.
Just modify the select statement on line 21, and only include value and timestamp.
Re-Run the notebook cell and see the new displayed stream.
When we take a closer look at the stream and start scrolling, you notice that there are multiple batches of data. 
This process could go on indefinitely as long as the two notebooks are running, though this might be expensive and not provided the data as you need it.

And that is how streaming data is brought into Azure Databricks using HDInsight & Kafka.
