// Databricks notebook source
dbutils.widgets.text("dbSchema","dbo")
dbutils.widgets.text("dbTable","DimGeography")

// COMMAND ----------

//jdbc:sqlserver://ilearnsqlserver.database.windows.net:1433;database=ilearnsqldb;user=duynct@ilearnsqlserver;password={your_password_here};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;
val jdbcUsername = "duynct"
val jdbcPassword = "Link@12345"
val jdbcPort = "1433"
val jdbcDatabase = "ilearnsqldb"
val dbServerName = "ilearnsqlserver"

val dbSchema = dbutils.widgets.get("dbSchema")
val dbTable = dbutils.widgets.get("dbTable")

val hostname = s"${dbServerName}.database.windows.net"
val jdbcUrl = s"jdbc:sqlserver://${hostname}:${jdbcPort};database=${jdbcDatabase}"
val jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

// COMMAND ----------

// MAGIC %run ./includes/session

// COMMAND ----------

import org.apache.spark.sql.DataFrame

def writeToDb(df: DataFrame, dbTable: String, dbSchema: String = "dbo",mode: String = "overwrite") : Unit = {
  
  val destinationTable = s"${dbSchema}.${dbTable}"
  df.write
          .format("jdbc")
          .mode("overwrite")
          .option("url",jdbcUrl)
          .option("dbtable",destinationTable)
          .option("user",jdbcUsername)
          .option("password",jdbcPassword).save()
}

// COMMAND ----------

def loadParquet(path: String): DataFrame = {
  spark.read.format("parquet").load(path)
}

// COMMAND ----------

import io.delta.tables.DeltaTable
import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.streaming.Trigger


// COMMAND ----------

import java.nio.file.Paths
import java.time.format.DateTimeFormatter
import java.time.OffsetDateTime

val containerName = "databrickparquets"
val storageAccountName = "ilearndlstorgen2"
val pathToProcess = s"abfss://${containerName}@${storageAccountName}.dfs.core.windows.net/dbo.Person.parquet"
val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSS")
val startTimeStamp = OffsetDateTime.now().format(dateTimeFormatter)

// COMMAND ----------

val tableName = Paths.get(pathToProcess)
 val sourceDF = loadParquet(pathToProcess).withColumn("rawLayerProcessingTime",current_timestamp())

// COMMAND ----------

display(sourceDF.limit(10))

// COMMAND ----------

try{
  val tableName = Paths.get(pathToProcess)
  val sourceDF = loadParquet(pathToProcess).withColumn("rawLayerProcessingTime",current_timestamp())
  writeToDb(sourceDF, dbTable, dbSchema)
}catch{
  case e: Throwable => println(s"error processing landing location: ${pathToProcess}. Exception thrown: " + e)
}
