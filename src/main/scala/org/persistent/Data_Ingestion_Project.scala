package Data_Ingestion

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File
import java.sql.DriverManager
import java.util.Properties
import scala.io.StdIn.readLine

object Data_Ingestion_Project {
  def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataProject")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");
    return spark;
  }

  def readConfigFile(Json_file_path: String, file_type: String): DataFrame = {
    val spark = createSparkSession();
    val Json_fileData = spark.read.format(file_type).option("multiline", true).load(Json_file_path);
    return Json_fileData;
  }


  def getSrcConnections(configFileData: DataFrame): DataFrame = {
    val src_ct_df = configFileData.filter(configFileData("type") === "source").select("fileType");
    return src_ct_df;
  }

  def getTarConnections(configFileData: DataFrame): DataFrame = {
    val tar_ct_df = configFileData.filter(configFileData("type") === "target").select("connectionType");
    return tar_ct_df;
  }

  def getDataFromFile(configFileData: DataFrame): DataFrame = {
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "source").select("filePath", "fileType");
    import spark.implicits._
    val src_file_path = df.select("filePath").distinct().map(f => f.getString(0)).collect().toList(0);
    val src_file_type = df.select("fileType").distinct().map(f => f.getString(0)).collect().toList(0);
    val src_file_data = spark.read.format(src_file_type).option("header", "true").load(src_file_path);
    return src_file_data;
  }

  def putDataInPostgreSQL(configFileData: DataFrame, src_file_data: DataFrame): String = {
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

    // Set up the database connection
    val url = s"jdbc:postgresql://localhost:5432/"
    val taleUrl = s"\"$post_Schema_Name\".$post_table_name"
    val conn = DriverManager.getConnection(url, post_userName, post_password)

    val dbName = s"$post_databaseName"
    val dbExistsQuery = s"SELECT 1 FROM pg_database WHERE datname = '$dbName'"
    val dbExists = conn.createStatement().executeQuery(dbExistsQuery).next()

    val schemaName = s"$post_Schema_Name"
    val schemaExistsQuery = s"SELECT 1 FROM pg_namespace WHERE nspname = '$schemaName'"
    val schemaExists = conn.createStatement().executeQuery(schemaExistsQuery).next()

    if(dbExists && schemaExists) {
      val db_url = s"jdbc:postgresql://localhost:5432/$post_databaseName"
      src_file_data.write
        .mode(SaveMode.Overwrite)
        .jdbc(db_url, s"$taleUrl", pgConnectionType)
    } else if (!dbExists) {
      println("YOUR DATABASE & SCHEMA DOESN'T EXISTS")
      println("Do You Want to CREATE DATABASE & SCHEMA")
      println("If YES type -> y else type -> x")
      val create_Database_Schema=readLine()

      if(create_Database_Schema == "y") {
        val createDbQuery = s"CREATE DATABASE $dbName"
        conn.createStatement().execute(createDbQuery)

        val db_url = s"jdbc:postgresql://localhost:5432/$post_databaseName"
        val db_conn = DriverManager.getConnection(db_url, post_userName, post_password)

        val createSchemaQuery = s"CREATE SCHEMA $post_Schema_Name"
        db_conn.createStatement().execute(createSchemaQuery)

        src_file_data.write
          .mode(SaveMode.Overwrite)
          .jdbc(db_url, s"$taleUrl", pgConnectionType)
      }
      else return "We are Not able to Load data -> " +
        "Please Provide Correct DATABASE & SCHEMA"

    } else if (dbExists) {
      if (!schemaExists) {
        println("YOUR SCHEMA DOESN'T EXISTS")
        println("Do You Want to CREATE SCHEMA")
        println("If YES type -> y else type -> x")
        val create_Schema = readLine()

        if (create_Schema == "y") {
          val db_url = s"jdbc:postgresql://localhost:5432/$post_databaseName"
          val db_conn = DriverManager.getConnection(db_url, post_userName, post_password)

          val createSchemaQuery = s"CREATE SCHEMA $post_Schema_Name"
          db_conn.createStatement().execute(createSchemaQuery)

          src_file_data.write
            .mode(SaveMode.Overwrite)
            .jdbc(db_url, s"$taleUrl", pgConnectionType)
        } else {
          return "We are Not able to Load data -> " +
            "Please Provide Correct SCHEMA"
        }
      }
    }

    return "File SuccessFully Loaded !"
  }

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession();
    val config_Json_file_path = "D:\\Technothon\\Demo_3\\src\\main\\scala\\JsonData.json";
    val file_type = "json";
    val file = new File(config_Json_file_path)

    // checking file is there or not
    if (file.exists() && file.isFile()) {
      // val objConfi = new configFile;
      val configFileData = readConfigFile(config_Json_file_path, file_type);
      // configFileData
      //configFileData.show();
      // val objConnection = new connection;
      val srcConectuonData = getSrcConnections(configFileData);
      val tarConectuonData = getTarConnections(configFileData);

      //srcConectuonData.show();
      //tarConectuonData.show();

      import spark.implicits._
      val srcConectionTypeData = srcConectuonData.select("fileType").distinct()
        .map(f => f.getString(0)).collect().toList;
      val tarConectionTypeData = tarConectuonData.select("connectionType").distinct()
        .map(f => f.getString(0)).collect().toList;
      // println(tarConectionTypeData(0));

      // creating empty dataframe
      var srcFileData = spark.emptyDataFrame
      if (srcConectionTypeData(0) == "csv" || srcConectionTypeData(0) == "parquet") {
        srcFileData = getDataFromFile(configFileData);
      }
      if (tarConectionTypeData(0) == "postgreSQL") {
        val message = putDataInPostgreSQL(configFileData, srcFileData);
        println(message);
      }
    }
  }
}