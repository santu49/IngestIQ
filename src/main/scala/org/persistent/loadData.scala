package org.persistent
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.persistent.mainApp.createSparkSession

import java.sql.DriverManager
import java.util.Properties

class loadData {


  def putDataInLocalFile(configFileData:DataFrame,srcFileData:DataFrame):String={
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "target").select("filePath", "fileType");
    import spark.implicits._
    val tar_file_path = df.select("filePath").distinct().map(f => f.getString(0)).collect().toList(0);
    val tar_file_type = df.select("fileType").distinct().map(f => f.getString(0)).collect().toList(0);
    //    val src_file_data = spark.read.format(src_file_type).option("header", "true").load(src_file_path);
    //    srcFileData.show()
    srcFileData.coalesce(1).write.format(tar_file_type).option("header","true").save(tar_file_path);

    return "Successfully loaded data"

  }


  def putDataInPostgreSQL(configFileData: DataFrame, src_file_data: DataFrame): String = {
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "target").select("userName", "password", "dataBaseName", "schemaName", "tableName");
    import spark.implicits._
    val post_userName = df.select("userName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_password = df.select("password").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_databaseName = df.select("dataBaseName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_Schema_Name = df.select("schemaName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_table_name = df.select("tableName").distinct().map(f => f.getString(0)).collect().toList(0);

    val pgConnectionType = new Properties();
    pgConnectionType.setProperty("user", s"$post_userName");
    pgConnectionType.setProperty("password", s"$post_password");
    val tableUrl = s"\"$post_Schema_Name\".$post_table_name"
    val url = s"jdbc:postgresql://localhost:5432/$post_databaseName"
    src_file_data.write
      .mode(SaveMode.Append)
      .jdbc(url, s"$tableUrl", pgConnectionType)

    return "Successfully loaded data";
  }



  def putDataInAWSPostgreSQL(configFileData: DataFrame, src_file_data: DataFrame): String = {
    val spark = createSparkSession();

    val df = configFileData.filter(configFileData("type") === "target")
      .select("userName", "password", "dataBaseName", "schemaName", "tableName");
    import spark.implicits._
    val post_userName = df.select("userName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_password = df.select("password").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_databaseName = df.select("dataBaseName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_Schema_Name = df.select("schemaName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_table_name = df.select("tableName").distinct().map(f => f.getString(0)).collect().toList(0);

    val pgConnectionType = new Properties();
    pgConnectionType.setProperty("user", s"$post_userName");
    pgConnectionType.setProperty("password", s"$post_password");

    val url = "jdbc:postgresql://database-1.ckaqa1w4hirj.ap-south-1.rds.amazonaws.com:5432/";
    val tableUrl = s"\"$post_Schema_Name\".$post_table_name";
    val conn = DriverManager.getConnection(url, post_userName, post_password);
    val statement = conn.createStatement();
    statement.executeUpdate(s"CREATE DATABASE $post_databaseName")
    val url1 = s"jdbc:postgresql://database-1.ckaqa1w4hirj.ap-south-1.rds.amazonaws.com:5432/$post_databaseName"
    val conn1 = DriverManager.getConnection(url1, post_userName, post_password)
    val statement1 = conn1.createStatement()
    statement1.executeUpdate(s"CREATE SCHEMA $post_Schema_Name");
    // write file to destination
    src_file_data.write
      .mode(SaveMode.Overwrite)
      .jdbc(url1, s"$tableUrl", pgConnectionType)

    return "Successfully loaded data from local file to AWS PostgreSql";
  }

}
