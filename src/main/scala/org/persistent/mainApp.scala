package org.persistent

import org.apache.spark.sql.SparkSession

object mainApp {

  def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataProject")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");
    return spark;
  }

  def main(args: Array[String]): Unit = {
//    println("Hello")
    val spark = createSparkSession();
    val config_Json_file_path = "src/main/resources/configFile.json";
    val file_type = "json";
    val connectionOBJ= new connection
    val check = connectionOBJ.checkConfigFile(config_Json_file_path, file_type);
    println(check);


  }

}
