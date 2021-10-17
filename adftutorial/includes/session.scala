// Databricks notebook source
//spark.sparkContext.hadoopConfiguration.set(s"fs.azure.account.key.${storageAccountName}.dfs.core.windows.net", "D4G1YaXhIdZLKEoOnPJcuE1QN2OUltGY/PxmJkaRIEQ3/ieWPjREUyNvazP7raEqzRqauGidaEY5zCTyE1/euQ==")

// COMMAND ----------

//spark.conf.set(s"fs.azure.account.auth.type.${storageAccountName}.dfs.core.windows.net", "SAS")
//spark.conf.set(s"fs.azure.sas.token.provider.type.${storageAccountName}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebbfs.oauth2.ClientCredsTokenProvider")

// COMMAND ----------

val containerName = "databrickparquets"
val storageAccountName = "ilearndlstorgen2"

spark.conf.set(s"fs.azure.account.key.${storageAccountName}.dfs.core.windows.net","D4G1YaXhIdZLKEoOnPJcuE1QN2OUltGY/PxmJkaRIEQ3/ieWPjREUyNvazP7raEqzRqauGidaEY5zCTyE1/euQ==")
spark.conf.set(s"fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls(s"abfss://mydatabrick@${storageAccountName}.dfs.core.windows.net/")
spark.conf.set(s"fs.azure.createRemoteFileSystemDuringInitialization", "false")
