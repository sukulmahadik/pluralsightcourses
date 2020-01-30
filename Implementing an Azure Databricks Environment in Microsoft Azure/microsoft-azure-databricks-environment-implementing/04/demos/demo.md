# Demo Batch Scoring of Apache Spark ML Models with Azure Databricks
This demo is based on the following online resources:
[Batch scoring of Spark machine learning models on Azure Databricks]() https://docs.microsoft.com/en-us/azure/architecture/reference-architectures/ai/batch-scoring-databricks)
[Batch scoring of Spark machine learning models - Github](http://bit.ly/ghbatch)

The video clips walk through all of the notebooks used in the batch scoring pipeline. To complete the demo on your own, follow the directions in the github repository above along with the video clips.

## Demo - Setting up a Batch Scoring Environment
1. Create workspace batch-scoring-ws
2. Create cluster batch-scoring-cl01
3. Configure CLI for use
> virtualenv -p /usr/bin/python2.7 databrickscli

> source databrickscli/bin/activate

> databricks configure –token
URL: <URL to Azure Databricks install>
key: <Access Key created in Azure Databricks>

## Demo - Clone Github Repo
    1. Log into Azure Cloud Shell
    2. Go to clouddrive
        > Cd clouddrive
    3. Clone github repo
        >git clone https://github.com/Azure/BatchSparkScoringPredictiveMaintenance.git
    4. Import databricks workspace into workspace for user felix@bentech.net
        > databricks workspace import_dir ./BatchSparkScoringPredictiveMaintenance/notebooks /Users/felix@bentech.net/notebooks
    5. Attach all notebooks to cluster
			i. Run all notebooks and come back


## Demo - Add Batch Scoring Job for Scoring Pipeline
[Reference for Demo](http://bit.ly/scoringjob)
1. Activate Virtual Environment and browse to clouddrive
    > source databrickscli/bin/activate

    > cd ./clouddrive 

    > databricks clusters list
2.  Open JSON file in code to edit
    1. Modify ClusterID and name
    2. Add Schedule code to JSON file
    > code ./BatchSParkScoringPredictiveMaintenance/jobs/3_CreateScoringPipeline.tmpl

    > "schedule": {
            "quartz_cron_expression": "0 30 7-18 ? * *",
            "timezone_id": "America/Los_Angeles"
        }
    > databricks jobs create --json-file ./BatchSParkScoringPredictiveMaintenance/jobs/3_CreateScoringPipeline.tmpl

    > databricks jobs list

    > databricks jobs run-now --job-id 17
