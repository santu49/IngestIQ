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

    val spark = createSparkSession();
    val config_Json_file_path = "src/main/resources/configFile.json";
    val file_type = "json";
    val connectionOBJ = new connection
    val readDataOBJ = new readData
    val loadDataOBJ = new loadData
    val check = connectionOBJ.checkConfigFile(config_Json_file_path, file_type);
    //    println(check);
    var configFileData = spark.emptyDataFrame;
    if (check == "Success") {
      configFileData = connectionOBJ.readConfigFile(config_Json_file_path, file_type);
      //      configFileData.show();
      val srcConnectionData = connectionOBJ.getSrcConnections(configFileData);
      //      srcConnectionData.show()
      val tarConnectionData = connectionOBJ.getTarConnections(configFileData);
      //      tarConnectionData.show()
      import spark.implicits._
      val srcConnectionTypeData = srcConnectionData.select("connectionType").distinct()
        .map(f => f.getString(0)).collect().toList;
      //      println(srcConnectionTypeData)
      val tarConnectionTypeData = tarConnectionData.select("connectionType").distinct()
        .map(f => f.getString(0)).collect().toList;
      //      print(tarConnectionTypeData)

      var srcFileData = spark.emptyDataFrame
      //for source
      if (srcConnectionTypeData(0) == "fileSystem") {
        srcFileData = readDataOBJ.getDataFromFile(configFileData);
        //                  srcFileData.show(10);
        //          write here
      } else if (srcConnectionTypeData(0) == "postgreSQL") {
        srcFileData = readDataOBJ.getDataFromPostGre(configFileData);
        //        srcFileData.show(10);
      }
      // for target
      if (tarConnectionTypeData(0) == "postgreSQL") {
        val message = loadDataOBJ.putDataInPostgreSQL(configFileData, srcFileData);
        println(message);
      } else if (tarConnectionTypeData(0) == "fileSystem") {
        val message = loadDataOBJ.putDataInLocalFile(configFileData, srcFileData);
        println(message);
      } else if (tarConnectionTypeData(0) == "AwsPostgreSQL") {
        val message = loadDataOBJ.putDataInAWSPostgreSQL(configFileData, srcFileData);
        println(message);

      }


    }


  }

}
