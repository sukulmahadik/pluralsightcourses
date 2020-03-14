// Session configuration
val appID = "0dfda946-bab4-4687-aefd-691f49da90f6"
val password = "i]+JvQ[Z/c4X285uz36qJwK2hzeQnQQJ"
val fileSystemName = "spark-fs"
val tenantID = "133f6972-44a7-4037-8eea-1d9afd1ebfc8"

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", "0dfda946-bab4-4687-aefd-691f49da90f6")
spark.conf.set("fs.azure.account.oauth2.client.secret", "i]+JvQ[Z/c4X285uz36qJwK2hzeQnQQJ")
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/133f6972-44a7-4037-8eea-1d9afd1ebfc8/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://test-spark@teststorage704.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

// Ingest sample data into Gen2 storage
%sh wget -P /tmp https://raw.githubusercontent.com/Azure/usql/master/Examples/Samples/Data/json/radiowebsite/small_radio_json.json

// Copy the JSON file into Gen2 storage
dbutils.fs.cp("file:///tmp/small_radio_json.json", "abfss://" + "spark-fs" + "@" + "psdatalake704" + ".dfs.core.windows.net/")

// Extract the data from Gen2
val df = spark.read.json("abfss://spark-fs@teststorage704.dfs.core.windows.net/small_radio_json.json")

// Show the data from contents
df.show()

// Transform the data
val specificColumnsDf = df.select("firstname", "lastname", "gender", "location", "level")
specificColumnsDf.show()

// Perform a second transformation
val renamedColumnsDF = specificColumnsDf.withColumnRenamed("level", "subscription_type")
renamedColumnsDF.show()

// Load data into Azure SQL DW

// Set up staging storage account
val blobStorage = "stagingstorage704.blob.core.windows.net"
val blobContainer = "test-staging"
val blobAccessKey =  "Q1cmQOuv762SB0XIGYqUHUfpuS2yw54XPEP5N3wJjEKfejVrSHnWUBJnjJVsMzySE1HKiNHBw+icI7GwBH3SzQ=="

// Specify a temp folder
val tempdir = "wasbs://" + "tempdir" + "@" + "stagingstorage704.blob.core.windows.net" +"/tempdir"

//Store access key in the configuration for security
val acntInfo = "fs.azure.account.key."+ blobStorage
sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

//SQL Data Warehouse related settings
val dwDatabase = "psdatawarehouse704"
val dwServer = "psdw704.database.windows.net"
// val dwUser = "tim"
// val dwPass = ""
val dwJdbcPort =  "1433"
val dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
val sqlDwUrl = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + "tim" + ";password=" + "xyz" + ";$dwJdbcExtraOptions"
val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + "tim" + ";password=" + "xyz"


// Load the transformed data frame - Azure SQL DW table is called SampleTable
spark.conf.set(
    "spark.sql.parquet.writeLegacyFormat",
    "true")

renamedColumnsDF.write.format("com.databricks.spark.sqldw").option("url", sqlDwUrlSmall).option("dbtable", "SampleTable")       .option( "forward_spark_azure_storage_credentials","True").option("tempdir", tempDir).mode("overwrite").save()

// Query dbo.SampleTable in SSMS

%md


