package org.persistent

import org.apache.spark.sql.SparkSession

import scala.io.StdIn.readLine

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

    val spark = createSparkSession()
    val config_Json_file_path = "src/main/resources/configFile.json"
    val file_type = "json"
    val connectionOBJ = new connection
    val readDataOBJ = new readData
    val loadDataOBJ = new loadData
    val check = connectionOBJ.checkConfigFile(config_Json_file_path, file_type)
    //    println(check);
    var configFileData = spark.emptyDataFrame
    if (check == "Success") {
      configFileData = connectionOBJ.readConfigFile(config_Json_file_path, file_type)
      //      configFileData.show();
      val srcConnectionData = connectionOBJ.getSrcConnections(configFileData)
      //      srcConnectionData.show()
      val tarConnectionData = connectionOBJ.getTarConnections(configFileData)
      //      tarConnectionData.show()
      import spark.implicits._
      val srcConnectionTypeData = srcConnectionData.select("connectionType").distinct()
        .map(f => f.getString(0)).collect().toList
      //            println(srcConnectionTypeData)
      val tarConnectionTypeData = tarConnectionData.select("connectionType").distinct()
        .map(f => f.getString(0)).collect().toList
      //      print(tarConnectionTypeData)
      var srcFileData = spark.emptyDataFrame

      //---------------------csv to csv----------------------
      if (srcConnectionTypeData(0).toLowerCase == "filesystem" && tarConnectionTypeData(0).toLowerCase == "filesystem") {
        val srcConnectionFileType = configFileData.filter(configFileData("type") === "source").select("fileType").distinct()
          .map(f => f.getString(0)).collect().toList
        //        print(srcConnectionFileType)
        val tarConnectionFileType = configFileData.filter(configFileData("type") === "target").select("fileType").distinct()
          .map(f => f.getString(0)).collect().toList
        //        print(tarConnectionFileType)
        if (srcConnectionFileType(0).toLowerCase == "csv" && tarConnectionFileType(0).toLowerCase == "csv") {
          srcFileData = readDataOBJ.getDataFromFile(configFileData)
          //          srcFileData.show(10)
          println("Do you want to load same data? (yes/no)")
          val res = readLine
          if (res.toLowerCase == "no") {
            //            srcFileData.show(10)
            val message = loadDataOBJ.modifiedData(configFileData, srcFileData)
            println(message)
            //            print("getting res")
            sys.exit
          }

        }

      }
      // ----------------for source----------------------
      if (srcConnectionTypeData(0).toLowerCase == "filesystem") {
        srcFileData = readDataOBJ.getDataFromFile(configFileData);
        //        srcFileData.show(10);
        //          write here
      } else if (srcConnectionTypeData(0).toLowerCase == "postgresql") {
        srcFileData = readDataOBJ.getDataFromPostGre(configFileData);
        //        srcFileData.show(10);
      } else if (srcConnectionTypeData(0).toLowerCase == "mysql") {
        srcFileData = readDataOBJ.getDataFromMYSQL(configFileData);
        //        srcFileData.show(10);
      } else if (srcConnectionTypeData(0).toLowerCase == "mongodb") {
        srcFileData = readDataOBJ.getDataFromMongoDB(configFileData);
        //                srcFileData.show(10);
      }

      //----------------------- for target---------------------
      if (tarConnectionTypeData(0).toLowerCase == "postgresql") {
        val message = loadDataOBJ.putDataInPostgreSQL(configFileData, srcFileData);
        println(message);
      } else if (tarConnectionTypeData(0).toLowerCase == "filesystem") {
        val message = loadDataOBJ.putDataInLocalFile(configFileData, srcFileData);
        println(message);
      } else if (tarConnectionTypeData(0).toLowerCase == "awspostgresql") {
        val message = loadDataOBJ.putDataInAWSPostgreSQL(configFileData, srcFileData);
        println(message);
      } else if (tarConnectionTypeData(0).toLowerCase == "mysql") {
        val message = loadDataOBJ.putDataInMYSQL(configFileData, srcFileData);
        println(message);
      } else if (tarConnectionTypeData(0).toLowerCase == "mongodb") {
        val message = loadDataOBJ.putDataInMongoDB(configFileData, srcFileData);
        println(message);

      }
    }
  }
}
