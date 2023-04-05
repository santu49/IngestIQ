package org.persistent

import org.persistent.mainApp.createSparkSession
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File

class connection {

  def checkConfigFile(Json_file_path: String, file_type: String): Any = {
    try {
      val file = new File(Json_file_path)
      if (file.exists() && file.isFile()) {
        return "Success";
      }
      else {
        // File does not exist or is not a regular file
        throw new Exception("File does not exist")
      }
    } catch {
      case e: Exception => {
        println("An error occurred: " + e.getMessage);
      }
    }
  }

  def readConfigFile(Json_file_path: String, file_type: String): DataFrame = {
    val spark = createSparkSession();
    val Json_fileData = spark.read.format(file_type).option("multiline", true).load(Json_file_path);
    return Json_fileData;
  }
  def getSrcConnections(configFileData: DataFrame): DataFrame = {
    val src_ct_df = configFileData.filter(configFileData("type") === "source").select("connectionType");
    return src_ct_df;
  }
  def getTarConnections(configFileData: DataFrame): DataFrame = {
    val tar_ct_df = configFileData.filter(configFileData("type") === "target").select("connectionType");
    return tar_ct_df;
  }

}
