package org.persistent

import org.apache.spark.sql.DataFrame
import org.persistent.mainApp.createSparkSession

import java.util.Properties

class readData {

  def getDataFromFile(configFileData: DataFrame): DataFrame = {
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "source").select("filePath", "fileType");
    import spark.implicits._
    val src_file_path = df.select("filePath").distinct().map(f => f.getString(0)).collect().toList(0);
    val src_file_type = df.select("fileType").distinct().map(f => f.getString(0)).collect().toList(0);
    val src_file_data = spark.read.format(src_file_type).option("header", "true").option("inferSchema", "true").load(src_file_path);
    return src_file_data;
  }

  def getDataFromPostGre(configFileData: DataFrame): DataFrame = {
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "source").select("userName", "password", "dataBaseName", "schemaName", "tableName");
    import spark.implicits._
    val post_userName = df.select("userName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_password = df.select("password").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_databaseName = df.select("dataBaseName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_Schema_Name = df.select("schemaName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_table_name = df.select("tableName").distinct().map(f => f.getString(0)).collect().toList(0);

    //    val pgConnectionType = new Properties();
    //    pgConnectionType.setProperty("user", s"$post_userName");
    //    pgConnectionType.setProperty("password", s"$post_password");
    val tableUrl = s"\"$post_Schema_Name\".$post_table_name"
    val url = s"jdbc:postgresql://localhost:5432/$post_databaseName"
    val src_ct_df = spark.read
      .format("jdbc")
      .option("url", s"$url")
      .option("dbtable", s"$tableUrl")
      .option("user", s"$post_userName")
      .option("password", s"$post_password")
      .option("driver", "org.postgresql.Driver")
      .option("inferSchema", "true")
      .load()
    //    src_ct_df.show(10);
    return src_ct_df;
  }

  def getDataFromMYSQL(configFileData: DataFrame): DataFrame = {
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "source").select("userName", "password", "dataBaseName", "schemaName", "tableName");
    import spark.implicits._
    val mysql_userName = df.select("userName").distinct().map(f => f.getString(0)).collect().toList(0);
    val mysql_password = df.select("password").distinct().map(f => f.getString(0)).collect().toList(0);
    val mysql_databaseName = df.select("dataBaseName").distinct().map(f => f.getString(0)).collect().toList(0);
    //val mysql_Schema_Name = df.select("schemaName").distinct().map(f => f.getString(0)).collect().toList(0);
    val mysql_table_name = df.select("tableName").distinct().map(f => f.getString(0)).collect().toList(0);

    val mySqlConnectionType = new Properties();
    mySqlConnectionType.setProperty("user", s"$mysql_userName");
    mySqlConnectionType.setProperty("password", s"$mysql_password");
    //val tableUrl = s"\"$mysql_Schema_Name\".$post_table_name"
    //val url = s"jdbc:mysql://localhost:3306/$post_databaseName"

    val src_ct_df = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", s"jdbc:mysql://localhost:3306/$mysql_databaseName")
      .option("dbtable", s"$mysql_table_name")
      .option("user", s"$mysql_userName")
      .option("password", s"$mysql_password")
      .option("inferSchema", "true")
      .load()
    return src_ct_df;
  }


}
