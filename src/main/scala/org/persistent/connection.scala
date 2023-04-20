package org.persistent

import org.persistent.mainApp.createSparkSession
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.json4s.scalap.Failure.error

import java.io.File

class connection {

  def checkConfigFile(Json_file_path: String, file_type: String): Any = {
    try {
      val file = new File(Json_file_path)
      if (file.exists() && file.isFile()) {
        return "Success"
      }
      else {
        // File does not exist or is not a regular file
        throw new Exception("File does not exist")
      }
    } catch {
      case e: Exception => {
        println("An error occurred: " + e.getMessage)
      }
    }
  }

  def readConfigFile(Json_file_path: String, file_type: String): DataFrame = {
    val spark = createSparkSession()
    val Json_fileData = spark.read.format(file_type).option("multiline", true).load(Json_file_path)
    return Json_fileData
  }

  def getSrcConnections(configFileData: DataFrame): DataFrame = {
    val spark = createSparkSession()
    var src_ct_df = spark.emptyDataFrame
    import spark.implicits._
    val srcConnectionTypeData = configFileData.filter(configFileData("type") === "source").select("connectionType").distinct()
      .map(f => f.getString(0)).collect().toList
    if (srcConnectionTypeData(0).toLowerCase == "filesystem") {
      val df = configFileData.filter(configFileData("type") === "source").select("filePath", "fileType")
      val src_file_path = df.select("filePath").distinct().map(f => f.getString(0)).collect().toList(0)
      val src_file_type = df.select("fileType").distinct().map(f => f.getString(0)).collect().toList(0)
      val file = new File(src_file_path)
      if (file.exists() && file.isFile() && src_file_type == "csv" || src_file_type == "parquet") {
        src_ct_df = configFileData.filter(configFileData("type") === "source").select("connectionType")
      }
      else {
        throw new Exception("Error: Invalid file path or file type")
      }
    }
    else {
      src_ct_df = configFileData.filter(configFileData("type") === "source").select("connectionType")
    }
    return src_ct_df
  }

  def getTarConnections(configFileData: DataFrame): DataFrame = {
    val tar_ct_df = configFileData.filter(configFileData("type") === "target").select("connectionType")
    return tar_ct_df
  }

}
