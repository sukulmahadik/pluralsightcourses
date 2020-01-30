// Databricks notebook source
// MAGIC %md
// MAGIC #Audience Notebook ETL Demo
// MAGIC This demo is based off the the demo located at docs.microsoft.com [Tutorial: Extract, transform, and load data by using Azure Databricks](https://docs.microsoft.com/en-us/azure/azure-databricks/databricks-extract-load-sql-data-warehouse).
// MAGIC 
// MAGIC This notebook contains all of the code from the article above. You will need to fill in the appropriate values specific to your environment that are laid out in the tutorial.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Important: In order for this demo to work you must complete all of the pre-requisites listed in the tutorial.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Mount a file system in the Azure Data Lake Storage Gen2 account
// MAGIC This set of code cells sets configurations within the spark cluster, and mounts a file system in the Azure Data Lake account.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Account Configuration
// MAGIC This code cell sets default service principal credentials for the specific ADLS Gen 2 account created for use in the Spark session. Use this code instead of the session configuration shown in the tutorial as it provides the value for the storagename that is required by code cells later in this demo.

// COMMAND ----------


val storageAccountName = "m3genstorev2"
val appID = "d14f029a-b06b-48c1-b41b-3c00b19f6b53"
val password = "6fPGfSIu:A9SwXc.+FZJ[ZII46ofaVRB"
val fileSystemName = "audience-fs"
val tenantID = "a1ff7bdd-b489-49a5-9204-cd9e7273a74b"

spark.conf.set("fs.azure.account.auth.type." + storageAccountName + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + storageAccountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + storageAccountName + ".dfs.core.windows.net", "" + appID + "")
spark.conf.set("fs.azure.account.oauth2.client.secret." + storageAccountName + ".dfs.core.windows.net", "" + password + "")
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + storageAccountName + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + tenantID + "/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://" + fileSystemName  + "@" + storageAccountName + ".dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ingest sample data into the Azure Data Lake Storage Gen2 account
// MAGIC This is where we add the audience data for radio stations

// COMMAND ----------

// MAGIC %sh wget -P /tmp https://raw.githubusercontent.com/Azure/usql/master/Examples/Samples/Data/json/radiowebsite/small_radio_json.json

// COMMAND ----------

// MAGIC %sh wget -P /tmp https://raw.githubusercontent.com/Azure/usql/master/Examples/Samples/Data/json/radiowebsite/small_radio_json.json
// MAGIC //Download JSON from github repo to /tmp directory in dbfs

// COMMAND ----------

// Copy temporary JSON file to Azure Data Lake Storage location
dbutils.fs.cp("file:///tmp/small_radio_json.json", "abfss://" + fileSystemName + "@" + storageAccountName + ".dfs.core.windows.net/")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Extract data from the Azure Data Lake Storage Gen2 account
// MAGIC This step extracts the data from the JSON file into a data frame in an Azure Databricks workspace

// COMMAND ----------

//Create a temporary data frame in Spark cluster from JSON file
val df = spark.read.json("abfss://audience-fs@m3genstorev2.dfs.core.windows.net/small_radio_json.json")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Verify data in ADLS

// COMMAND ----------

//View the contents of the data frame df
df.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##Transform data in Azure Databricks
// MAGIC The raw sample data **small_radio_json.json** file captures the audience for a radio station and has a variety of columns. During the Transform phase, we'll retrieve only specific information from the raw data including **firstName, lastName, gender, location,** and **level**

// COMMAND ----------

//Create a temporary data frame and show the contents of the data frame
val specificColumnsDf = df.select("firstname", "lastname", "gender", "location", "level")
specificColumnsDf.show()

// COMMAND ----------

//Another sample of selecting values from the data frame created
val specificColumnsDfv2 = df.select("artist", "location", "song","userid")
specificColumnsDfv2.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Modifying column headers from **level** to **subscription_type**
// MAGIC This step transform the column heading that will be loaded into the final data storage for use by the consuming application.

// COMMAND ----------

//Create a temporary data frame with modified header
val renamedColumnsDF = specificColumnsDf.withColumnRenamed("level", "subscription_type")
renamedColumnsDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Load transformed data into Azure SQL Data Warehouse
// MAGIC In this section, you upload the transformed data into Azure SQL Data Warehouse. You use the Azure SQL Data Warehouse connector for Azure Databricks to directly upload a dataframe as a table in a SQL data warehouse.

// COMMAND ----------

//Create values for temporary blob storage account used by Azure SQL Data Warehouse
//Was created during pre-requisites and must be present
val blobStorage = "m3demosa.blob.core.windows.net"
val blobContainer = "m3demo"
val blobAccessKey =  "0kghJTrk0pGJYO9u+tAC6ant4K5PHM29xA6BSxeh88TwoZ9Wu8xPtQSnyIUmbZaMMXZtAkIDVd/OiWs1JZ6tqQ=="

// COMMAND ----------

//Specify a temporary blob storage location for moving data between Azure Databricks and Azure SQL Data Warehouse
val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"

// COMMAND ----------

//store Azure Blob storage access keys in the configuration
//Keeps keys out of plain text in notebook
val acntInfo = "fs.azure.account.key."+ blobStorage
sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

// COMMAND ----------

//SQL Data Warehouse related settings
//Replace the 4 values below with values from pre-req creation process
val dwDatabase = "audience-data-dw"
val dwServer = "radio-demo-srv01.database.windows.net"
val dwUser = "radioadmin"
val dwPass = "ThisismySQL#DW01"
val dwJdbcPort =  "1433"
val dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
val sqlDwUrl = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass + ";$dwJdbcExtraOptions"
val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass

// COMMAND ----------

//load the transformed dataframe, renamedColumnsDF, as a table in a SQL data warehouse, called SampleTable.
spark.conf.set(
    "spark.sql.parquet.writeLegacyFormat",
    "true")

renamedColumnsDF.write.format("com.databricks.spark.sqldw").option("url", sqlDwUrlSmall).option("dbtable", "SampleTable")       .option( "forward_spark_azure_storage_credentials","True").option("tempdir", tempDir).mode("overwrite").save()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Verify Table Data in SQL Data Warehouse

// COMMAND ----------

// Load the table, Sample Table, for SQL data warehouse and display in notebook
val df = spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", sqlDwUrlSmall)
  .option("tempDir", tempDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "SampleTable")
  .load()

display(df)
