## Using Event Grid to do Structured Streaming into Azure Synapse Analytics 

This sample was inspired by the following blog post.

[Event Grid with Azure Data Factory](https://cloudarchitected.com/2019/03/tutorial-event-based-etl-with-azure-databricks)

Please review the article to get an understanding of the core architecture and components

```
! Note: The ABS-AQS mechanism only works with Azure Blob Storage accounts (where hierarchical namepaces is disabled)
If you would like to use Azure Data Lake Gen 2 with hierarchical namespaces, please review AutoLoader
```
### Configure Azure Storage
1.) Create an Azure Blob Storage Account in Azure using the default options. Create a new container called inbound-data. This storage account will be used to receive streaming events. 

### Create a Queue
1.) On the blade, select Queue and create a new Queue. As the name enter inbound-data-events

### Create Event Subscription

1.) Create a new Event Subscription by selecting Events in the Blade. As the name enter inbound-data.  In the Defined Event Types, deselect all options and select Blob Created only

2.) Select Endpoint Type Storage Queues

<img src="https://github.com/kavarral/Images/blob/master/EventSubscription.PNG?raw=true" />

4.) Click on Select an Endpoint, select the storage account where you created the queue. In this case, it will be the same as the above.  Select the Storage Queue you created from the Drop down in this case it is called inbound-data-events, confirm the selection. This will return you to the Create Event Subscription blade.

<img src="https://github.com/kavarral/Images/blob/master/StorageQueue.PNG?raw=true" />

5.) Click on Filters and enable subject filtering

6.) Enter the following in Subject Begins With:

/blobServices/default/containers/inbound-data/blobs/processed/

7.) Click on Create

### Azure Synapse Analytics

1.) Create an Azure Synapse Analytics SQL Pool

2.) Create the following table schema

```
CREATE TABLE [dbo].[site_temperatures]
(
	[site_id] [bigint] NULL,
	[highest_temp] [bigint] NULL,
	[count] [float] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
```

### Azure Databricks
### This example uses Scala 

1.) Create a new Databricks instance and create a new cluster.  For the purposes of this sample, a standard cluster using DS3_v2 virtual machines with min 2 worker nodes and max 8 worker nodes will suffice. No additional libraries are required on the cluster. 

2.) Create a new Scala Notebook called Generator. The Generator will generate random JSON files into the container called inbound-data.

3.) Copy and paste your code into the notebook 

```
! Note:  For the purposes of this demo, we will use the storage account key.

It is strongly recommended that you use service principals, Secrets, Azure Key Vault to store your keys securely and make secure connections
```

Enter your storage account name and the storage account key. 
```
val storageAccount = "<storage account name>"
val storageAccountKey = "<storage account key>"
val credentials = Map(s"fs.azure.account.key.$storageAccount.blob.core.windows.net" ->
 storageAccountKey)
 
```

Mount the storage if not already mounted

```
dbutils.fs.mount(
   source = s"wasbs://inbound-data@$storageAccount.blob.core.windows.net/",
   mountPoint = "/mnt/inbound-data",
   extraConfigs = credentials)
```

```
 import org.apache.spark.sql.ForeachWriter
 import org.apache.spark.sql.Row
 import java.io.File
 import org.apache.commons.io.FileUtils
 import java.util.UUID.randomUUID
 import java.nio.charset.StandardCharsets

 val inputDf = spark.readStream
   .format("rate")
   .option("rowsPerSecond", 1)
   .load()

 inputDf.createOrReplaceTempView("stream")

 val dest = "/mnt/inbound-data/processed"

 val lineToJsonFileWriter = new ForeachWriter[Row] {
 
   override def process(value: Row): Unit = { 
     val outputFile = new File(s"/dbfs/${dest}/" + randomUUID().toString + ".json")
     val json = value.getString(0)
     FileUtils.writeStringToFile(outputFile, json, StandardCharsets.UTF_8)
   } 

   override def close(errorOrNull: Throwable): Unit = { 
   } 
 
   override def open(partitionId: Long, version: Long): Boolean = { 
     FileUtils.forceMkdir(new File(dest)) 
     true 
   } 
 }
```

```
val eventDf = spark.sql("""select to_json(
   struct(value as row_id, 
   int(RAND()*100) as site_id, 
   int(RAND() * 1000)/10 as temperature)
   ) as json from stream""")

 val streamingQuery = eventDf
    .writeStream
    .foreach(lineToJsonFileWriter)
    .start()
```

Create a new Scala Notebook called Processor. The Processor will read the JSON files that are generated and stream them into an Azure Synapse Analytics SQL Pool.
Note Storage account and storage key refer to the same storage account that you are generator data into (as per previous step)

```
 import org.apache.spark.sql._
 import org.apache.spark.sql.functions._
 import org.apache.spark.sql.types._
 import org.apache.spark.sql.streaming._

 val storageAccount = "<Storage Account Name>"
 val storageAccountKey = "<Storage Account Key>"

 val schema = StructType(Seq(
   StructField("row_id", LongType),
   StructField("site_id", LongType),
   StructField("temperature", DoubleType)
))

spark.conf.set(
   s"fs.azure.account.key.$storageAccount.blob.core.windows.net",
storageAccountKey)
 spark.conf.set("spark.sql.shuffle.partitions", "1")

 val stream = spark.readStream
   .format("abs-aqs")
   .option("fileFormat", "json")
   .option("queueName", "inbound-data-events")
   .option("connectionString", s"DefaultEndpointsProtocol=https;AccountName=$storageAccount;AccountKey=$storageAccountKey;EndpointSuffix=core.windows.net")
   .schema(schema)
   .load()
```

You can change the schema to fit other scenarios.
Change the "fileFormat" in the readstream command to other file formats such as parquet or CSV

```
display(stream)

```

Write the stream

```
! Note:  For the purposes of this demo, values such as keys, passwords and usernames are hardcoded. 
It is strongly recommended that you use service principals, Secrets, Azure Key Vault to store your keys securely and make secure connections

```

Create a new storage account that will host the checkpoint files. Create a new container called outputstreaming and a second container called temp for the tempdir location
Mount the storage if not already mounted

```
val streaminglogcontainer = "outputstreaming"
val streaminglogstorageaccount = <storage account name>
val streaminglogaccountkey = "<storage account key>"

val credentials = Map(s"fs.azure.account.key.$streaminglogstorageaccount.blob.core.windows.net" ->
 streaminglogaccountkey)

dbutils.fs.mount(
   source = s"wasbs://outputstreaming@$streaminglogstorageaccount.blob.core.windows.net/",
   mountPoint = "/mnt/outputstreaming",
   extraConfigs = credentials)
   
```

Set the context of the base path and checkpoint file location

```
val basePath ="/mnt/kvstreamingtestlog" 
val outputDeltaDirPath=basePath+"/delta"
val checkpointPath = basePath + "/checkpoint"       
```

Write the stream

```


//Set up the Blob storage account access key in the notebook session conf for storage account where checkpoint files and temp dir will be located
//Using this approach, the account access key is set in the session configuration associated with the notebook that runs the command.
//This configuration does not affect other notebooks attached to the same cluster. spark is the SparkSession object provided in the notebook.

spark.conf.set(
  "fs.azure.account.key.<Storage Account Name>.blob.core.windows.net",
  "<Storage Account Key>")
  
 stream
   .writeStream
   .option("url", "jdbc:sqlserver:/<database endpoint>:1433;database=<Database Name>;user=<User Name>;password=<Password> ;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")
   .option("tempDir", "wasbs://temp@<storage account>.blob.core.windows.net/tempdir")
   .option("forwardSparkAzureStorageCredentials", "true")
   .option("dbTable", "dbo.site_temperatures")
   .option("checkpointLocation", checkpointPath)
   .format("com.databricks.spark.sqldw")
   .option("numStreamingTempDirsToKeep", 50)
   .start()
```

Refer to the below link for more details on streaming into Azure Synapse Analytics
[Azure Synapse Analytics](https://docs.databricks.com/data/data-sources/azure/synapse-analytics.html)









