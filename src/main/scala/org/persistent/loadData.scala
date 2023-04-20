package org.persistent

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.persistent.mainApp.createSparkSession

import java.net.URLEncoder
import java.sql.DriverManager
import java.util.Properties
import scala.io.StdIn.readLine

class loadData {

  def putDataInLocalFile(configFileData: DataFrame, srcFileData: DataFrame): String = {
    val spark = createSparkSession()
    val df = configFileData.filter(configFileData("type") === "target").select("filePath", "fileType")
    import spark.implicits._
    val tar_file_path = df.select("filePath").distinct().map(f => f.getString(0)).collect().toList(0)
    val tar_file_type = df.select("fileType").distinct().map(f => f.getString(0)).collect().toList(0)
    //    srcFileData.show()
    srcFileData.coalesce(1).write.format(tar_file_type).option("header", "true").save(tar_file_path)
    return "Successfully loaded data"

  }


  def putDataInPostgreSQL(configFileData: DataFrame, src_file_data: DataFrame): String = {
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "target").select("userName", "password", "dataBaseName", "schemaName", "tableName")
    import spark.implicits._
    val post_userName = df.select("userName").distinct().map(f => f.getString(0)).collect().toList(0)
    val post_password = df.select("password").distinct().map(f => f.getString(0)).collect().toList(0)
    val post_databaseName = df.select("dataBaseName").distinct().map(f => f.getString(0)).collect().toList(0)
    val post_Schema_Name = df.select("schemaName").distinct().map(f => f.getString(0)).collect().toList(0)
    val post_table_name = df.select("tableName").distinct().map(f => f.getString(0)).collect().toList(0)
    val pgConnectionType = new Properties()
    pgConnectionType.setProperty("user", s"$post_userName")
    pgConnectionType.setProperty("password", s"$post_password")
    val tableUrl = s"\"$post_Schema_Name\".$post_table_name"
    val url = s"jdbc:postgresql://localhost:5432/$post_databaseName"
    src_file_data.write
      .mode(SaveMode.Append)
      .jdbc(url, s"$tableUrl", pgConnectionType)

    return "Successfully loaded data"
  }


  def putDataInAWSPostgreSQL(configFileData: DataFrame, src_file_data: DataFrame): String = {
    val spark = createSparkSession()
    val df = configFileData.filter(configFileData("type") === "target")
      .select("userName", "password", "dataBaseName", "schemaName", "tableName")
    import spark.implicits._
    val post_userName = df.select("userName").distinct().map(f => f.getString(0)).collect().toList(0)
    val post_password = df.select("password").distinct().map(f => f.getString(0)).collect().toList(0)
    val post_databaseName = df.select("dataBaseName").distinct().map(f => f.getString(0)).collect().toList(0)
    val post_Schema_Name = df.select("schemaName").distinct().map(f => f.getString(0)).collect().toList(0)
    val post_table_name = df.select("tableName").distinct().map(f => f.getString(0)).collect().toList(0)

    val pgConnectionType = new Properties()
    pgConnectionType.setProperty("user", s"$post_userName")
    pgConnectionType.setProperty("password", s"$post_password")

    val url = "jdbc:postgresql://database-1.ckaqa1w4hirj.ap-south-1.rds.amazonaws.com:5432/"
    val tableUrl = s"\"$post_Schema_Name\".$post_table_name"
    val conn = DriverManager.getConnection(url, post_userName, post_password)
    val statement = conn.createStatement()
    statement.executeUpdate(s"CREATE DATABASE $post_databaseName")
    val url1 = s"jdbc:postgresql://database-1.ckaqa1w4hirj.ap-south-1.rds.amazonaws.com:5432/$post_databaseName"
    val conn1 = DriverManager.getConnection(url1, post_userName, post_password)
    val statement1 = conn1.createStatement()
    statement1.executeUpdate(s"CREATE SCHEMA $post_Schema_Name")
    // write file to destination
    src_file_data.write
      .mode(SaveMode.Overwrite)
      .jdbc(url1, s"$tableUrl", pgConnectionType)

    return "Successfully loaded data from local file to AWS PostgreSql"
  }


  def putDataInMYSQL(configFileData: DataFrame, src_file_data: DataFrame): String = {
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "target").select("userName", "password", "dataBaseName", "schemaName", "tableName")
    import spark.implicits._
    val mysql_userName = df.select("userName").distinct().map(f => f.getString(0)).collect().toList(0)
    val mysql_password = df.select("password").distinct().map(f => f.getString(0)).collect().toList(0)
    val mysql_databaseName = df.select("dataBaseName").distinct().map(f => f.getString(0)).collect().toList(0)
    val mysql_Schema_Name = df.select("schemaName").distinct().map(f => f.getString(0)).collect().toList(0)
    val mysql_table_name = df.select("tableName").distinct().map(f => f.getString(0)).collect().toList(0)
    val mySqlConnectionType = new Properties()
    mySqlConnectionType.setProperty("user", s"$mysql_userName")
    mySqlConnectionType.setProperty("password", s"$mysql_password")
    //val tableUrl = s"\"$mysql_Schema_Name\".$mysql_table_name"
    val url = s"jdbc:mysql://localhost:3306/$mysql_databaseName"
    src_file_data.write
      .mode(SaveMode.Append)
      .jdbc(url, s"$mysql_table_name", mySqlConnectionType)

    return "Successfully loaded data"
  }

  def modifiedData(configFileData: DataFrame, src_file_data: DataFrame): String = {
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "target").select("filePath", "fileType")
    import spark.implicits._
    val tar_file_path = df.select("filePath").distinct().map(f => f.getString(0)).collect().toList(0)
    val tar_file_type = df.select("fileType").distinct().map(f => f.getString(0)).collect().toList(0)
    println("What are you want to change: ")
    println("1. Changes in rows")
    println("2. Changes in column")
    println("3. Changes in rows and column both")
    println("   Choose one option: ")
    val res = readLine
    if (res == "1") {
      println("Enter number of rows you want: ")
      val rows = readLine().toInt;
      val modifiedData = src_file_data.limit(rows)
      modifiedData.coalesce(1).write.format(tar_file_type).option("header", "true").option("inferSchema", "true").save(tar_file_path)
    } else if (res == "2") {
      val columns_count = readLine("Enter the number of columns you want: ").toInt
      val col_array = new Array[String](columns_count)
      println("Enter columns names(same as your csv files): ")
      for (i <- 0 until columns_count) {
        col_array(i) = readLine
      }
      val modifiedData =selectColumns(src_file_data, col_array: _*)
      modifiedData.coalesce(1).write.format(tar_file_type).option("header", "true").option("inferSchema", "true").save(tar_file_path)
    }else if(res=="3"){
      println("Enter number of rows you want: ")
      val rows = readLine().toInt;
      val rowModifiedData = src_file_data.limit(rows)
      val columns_count = readLine("Enter the number of columns you want: ").toInt
      val col_array = new Array[String](columns_count)
      println("Enter columns names: ")
      for (i <- 0 until columns_count) {
        col_array(i) = readLine
      }
      val modifiedData =selectColumns(rowModifiedData, col_array: _*)
      modifiedData.coalesce(1).write.format(tar_file_type).option("header", "true").option("inferSchema", "true").save(tar_file_path)

    }
    return "Successfully loaded Data"
  }

  def selectColumns(df: DataFrame, columns: String*): DataFrame = {
    val columnExprs = columns.map(col)
    val selectedColumns = df.select(columnExprs: _*)
    return selectedColumns;
  }

  def putDataInMongoDB(configFileData: DataFrame, src_file_data: DataFrame): String ={
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "target").select("userName", "password", "serverDetails", "dataBaseName", "collection")
    import spark.implicits._
    val mongoDB_userName = df.select("userName").distinct().map(f => f.getString(0)).collect().toList(0)
    val mongoDB_password = df.select("password").distinct().map(f => f.getString(0)).collect().toList(0)
    val mongoDB_serverDetails = df.select("serverDetails").distinct().map(f => f.getString(0)).collect().toList(0)
    val mongoDB_databaseName = df.select("dataBaseName").distinct().map(f => f.getString(0)).collect().toList(0)
    val mongoDB_collection = df.select("collection").distinct().map(f => f.getString(0)).collect().toList(0)
    val encodedPassword = URLEncoder.encode(mongoDB_password, "UTF-8")
    src_file_data.write.format("mongodb")
      .option("spark.mongodb.input.uri", s"mongodb://+$mongoDB_userName:$encodedPassword@$mongoDB_serverDetails:27017")
      .option("spark.mongodb.database", s"$mongoDB_databaseName")
      .option("spark.mongodb.collection", s"$mongoDB_collection")
      .mode("append")
      .save()

    return "Successfully loaded Data"

  }


}
