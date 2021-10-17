// Databricks notebook source
// DBTITLE 1,Note
// MAGIC %md
// MAGIC https://www.sqlshack.com/accessing-azure-blob-storage-from-azure-databricks/
// MAGIC </br>
// MAGIC https://ilearndlstorgen2.blob.core.windows.net/databrickparquets/dbo.DimCustomer.parquet
// MAGIC </br>
// MAGIC https://docs.databricks.com/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access.html
// MAGIC </br>
// MAGIC https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-get-started
// MAGIC </br>

// COMMAND ----------

//dbutils.widgets.removeAll()

// COMMAND ----------

// MAGIC %run ./includes/session

// COMMAND ----------

val path = s"abfss://${containerName}@${storageAccountName}.dfs.core.windows.net/dbo.DimCustomer.parquet"

// COMMAND ----------

val df = spark.read.parquet(path)

// COMMAND ----------

display(df.limit(1))

// COMMAND ----------

df.count()
