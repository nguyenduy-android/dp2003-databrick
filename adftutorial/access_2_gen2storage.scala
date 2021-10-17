// Databricks notebook source
// MAGIC %md
// MAGIC <h1>My Note</h1>
// MAGIC https://www.sqlshack.com/accessing-azure-blob-storage-from-azure-databricks/
// MAGIC </br>
// MAGIC https://ilearndlstorgen2.blob.core.windows.net/databrickparquets/dbo.DimCustomer.parquet
// MAGIC </br>
// MAGIC https://docs.databricks.com/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access.html
// MAGIC </br>
// MAGIC https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-get-started

// COMMAND ----------

//dbutils.widgets.removeAll()

// COMMAND ----------

val containerName = "databrickparquets"
val storageAccountName = "ilearndlstorgen2"

spark.conf.set(s"fs.azure.account.key.${storageAccountName}.dfs.core.windows.net","D4G1YaXhIdZLKEoOnPJcuE1QN2OUltGY/PxmJkaRIEQ3/ieWPjREUyNvazP7raEqzRqauGidaEY5zCTyE1/euQ==")
spark.conf.set(s"fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls(s"abfss://mydatabrick@${storageAccountName}.dfs.core.windows.net/")
spark.conf.set(s"fs.azure.createRemoteFileSystemDuringInitialization", "false")

// COMMAND ----------

val df = spark.read.parquet(s"abfss://${containerName}@${storageAccountName}.dfs.core.windows.net/dbo.DimCustomer.parquet")

// COMMAND ----------

display(df.limit(1))

// COMMAND ----------

//spark.conf.set(s"fs.azure.account.auth.type.${storageAccountName}.dfs.core.windows.net", "SAS")
//spark.conf.set(s"fs.azure.sas.token.provider.type.${storageAccountName}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebbfs.oauth2.ClientCredsTokenProvider")

// COMMAND ----------

//spark.sparkContext.hadoopConfiguration.set(s"fs.azure.account.key.${storageAccountName}.dfs.core.windows.net", "D4G1YaXhIdZLKEoOnPJcuE1QN2OUltGY/PxmJkaRIEQ3/ieWPjREUyNvazP7raEqzRqauGidaEY5zCTyE1/euQ==")
