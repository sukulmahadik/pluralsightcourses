#Demos for Implementing an Azure Databricks Environment module

#Demo – Deploy a Workspace and Explore UI
	Demo reference: https://docs.microsoft.com/en-us/azure/azure-databricks/quickstart-create-databricks-workspace-portal

	Setup Azure CLI to use Databricks
	i. Create Token - 
		1. dapi723d550071fac0c7d0be758e4000f3ad
	ii. Open Bash – Go through process
		1. virtualenv -p /usr/bin/python2.7 databrickscli
		2.  source databrickscli/bin/activate
		3.   pip install databricks-cli
		4. Databricks -h
		5.   databricks configure --token
			a. dapif4ba61048cf9c58993e6d748eb3458ce
		6.   databricks -h
		7.  databricks workspace -h
		8.  databricks workspace list

#Demo: Working with Spark clusters
	Demo Reference : https://docs.azuredatabricks.net/user-guide/clusters/create.html

	1. Create a Cluster using Azure CLI
		source databrickscli/bin/activate
		code ./clouddrive/demo/intro/bd-cl02.json
		databricks clusters create --json-file ./clouddrive/demo/intro/bd-cl02.json
		Databricks clusters list

#Demo: Working with Notebooks
	Demo Reference: https://docs.azuredatabricks.net/user-guide/notebooks/notebook-manage.html


#Demo Working with Tables
	Demo Reference (Start on step 4): https://docs.azuredatabricks.net/getting-started/quick-start.html#step-4-create-a-table

#Demo: Working with Spark Jobs
	Demo Reference: https://docs.databricks.com/user-guide/jobs.html#jobs

	i. Commands available
		databricks jobs -h
	ii. List jobs
		databricks jobs list
	iii. View Job Information
		databricks jobs get –h
		databricks jobs get --job-id 1
	iv. Create Job Datbricks CLI in Azure Cloud Shell
		databricks jobs create --json-file clouddrive/demo/bd-job01.json
	v. Run Created Job
		1. databricks jobs run-now –h
		2. databricks jobs run-now --job-id 5

Jobs API

	curl -n \
-F filedata=@"SparkPi-assembly-0.1.jar" \
-F path="/docs/sparkpi.jar" \
-F overwrite=true \
https://westus.azuredatabricks.net/api/2.0/dbfs/put

curl -n \
-F filedata=@"SparkPi-assembly-0.1.jar" \
-F path="/docs/sparkpi.jar" \
-F overwrite=true \
https://westus.azuredatabricks.net/api/2.0/dbfs/put

curl -n \
-X POST -H 'Content-Type: application/json' \
-d '{
      "name": "SparkPi JAR job",
      "new_cluster": {
        "spark_version": "5.2.x-scala2.11",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": 2
        },
     "libraries": [{"jar": "dbfs:/docs/sparkpi.jar"}],
     "spark_jar_task": {
        "main_class_name":"org.apache.spark.examples.SparkPi",
        "parameters": "10"
        }
}' https://westus.azuredatabricks.net/api/2.0/jobs/create

curl -n \
-X POST -H 'Content-Type: application/json' \
-d '{ "job_id": <job-id> }' https://westus.azuredatabricks.net/api/2.0/jobs/run-now

curl -n https://westus.azuredatabricks.net/api/2.0/jobs/runs/get?run_id=<run-id> | jq
