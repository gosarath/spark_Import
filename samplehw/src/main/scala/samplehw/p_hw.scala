package samplehw

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.{StructType,IntegerType,StringType,StructField};
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;
import java.util.Properties;
import java.sql.DriverManager
import java.sql.Connection
import com.databricks.spark.csv

object p_hw {
    def main(args: Array[String]){
    println("Hello World")
        var connection:Connection = null
    val driver = "oracle.jdbc.OracleDriver"
    //val url = "jdbc:mysql://localhost/mysql"
    //val username = "root"
    //val password = "root"
    try {
      
// Start - CSV File record count        
    // Read a file 
      
      val folder ="D:\\Spark\\spark-2.4.3-bin-hadoop2.7\\scala ide\\projects\\samplehw\\dataFile\\";
      val inputFileName = "Account1.csv";
      val importFilePath = folder.concat(inputFileName);
      val importFormatErrorFilePath = folder.concat("FormatError");//.concat(inputFileName);
      val importInvalidErrorFilePath = folder.concat("InvalidError");//.concat(inputFileName);
      
      val sparkConf = new SparkConf()
          .setAppName("Example Spark App")
          .setMaster("local[*]")  // Delete this line when submitting to a cluster
      val sparkContext = new JavaSparkContext(sparkConf);
      val stringJavaRDD = sparkContext.textFile(importFilePath);
      System.out.println("Number of lines in file = " + stringJavaRDD.count());
// end - CSV File record count      
      
// Start - Read CSV File      
      val sqlContext = new SQLContext(sparkContext);
      
      //custom schema definition of a file
      val AccountSchema = StructType(Array(
          StructField("PartyNumber",StringType,true),
          StructField("OrganizationName",StringType,true),
          StructField("YearEstablished",IntegerType,true),
          StructField("YearIncorporated",IntegerType,true),
          StructField("StockSymbol",StringType,true),
          StructField("CEOName",StringType,true),
          StructField("ControlYear",IntegerType,true),
          StructField("DUNSNumber",IntegerType,true),
          StructField("DomesticUltimateDUNSNumber",IntegerType,true),
          StructField("GlobalUltimateDUNSNumber",IntegerType,true),
          StructField("_corrupt_record", StringType, true) // used to find malformed rows
          ))
      
      val df = sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header","true")
          //.option("inferSchema","true") // auto find the schema
          .schema(AccountSchema)  // use defined schema
          .load(importFilePath)
          //.select($"rownum", $"name") // used to choose a subset of columns
          
      // print schema
      df.printSchema();
      
      // cache - https://forums.databricks.com/questions/271/should-i-always-cache-my-rdds.html
      df.rdd.cache();
      
      // print file
      df.rdd.foreach(println)
      
      // register data frame as db template
      df.registerTempTable("Account")
      
      // find malformed rows 
      val badRows = df.filter("_corrupt_record is not null");
      badRows.cache();//.show();
      
      badRows.write.format("com.databricks.spark.csv").save(importFormatErrorFilePath)
      
      //read data as sql
      //sqlContext.sql("SELECT * from Account where rownum >= 100").collect.foreach(println)
      
      //sqlContext.sql("SELECT * from Account where rownum >= 100 GROUP BY name").collect.foreach(println)
      
      //sqlContext.sql("SELECT * from Account").collect.foreach(println)
      
 // End - Read CSV File
      
 
 // Start - Validate
      
      // filter null partynumbers
      val invalidDF = df.filter("PartyNumber is NULL")
      invalidDF.cache();//.show()
      invalidDF.write.format("com.databricks.spark.csv").save(importInvalidErrorFilePath)
      
      val validRowsDF = df.filter("PartyNumber is not NULL").select("PartyNumber","OrganizationName","YearEstablished",
          "YearIncorporated","StockSymbol","CEOName","ControlYear","DUNSNumber","DomesticUltimateDUNSNumber","GlobalUltimateDUNSNumber")
      validRowsDF.cache()
      //validRowsDF.show()
      
 // End - Validate
      
 // Start - Write in to DB
      val connectionProperties = new Properties()
      connectionProperties.put("driver", "oracle.jdbc.OracleDriver")
      connectionProperties.put("numPartitions", "2") // parallel threads
      
      validRowsDF.write
      .mode(SaveMode.Append)
      .jdbc("jdbc:oracle:thin:fusion/welcome1@slc14hiy.us.oracle.com:1521/r13csmcf_b", "fusion.stable", connectionProperties)
      
       System.out.println("write successful");
 // End - Write in to DB      
      
/*
// Start - Read DB
      //Option 1 :: Read DB 
    val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
    val jdbcDF = spark.read
				  .format("jdbc")
				  .option("url", "jdbc:oracle:thin:fusion/welcome1@slc14hiy.us.oracle.com:1521/r13csmcf_b")
				  .option("dbtable", "fusion.stable")
				  .option("driver", "oracle.jdbc.OracleDriver")
				  .load();
    
    jdbcDF.printSchema();
    System.out.println("READ from DB table successful");
		jdbcDF.show();    


		//Option 2 :: Read DB
    val url = "jdbc:oracle:thin:fusion/welcome1@slc14hiy.us.oracle.com:1521/r13csmcf_b"
	  val table = "MKT_IMP_JOBS"

      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT name FROM MKT_IMP_JOBS")
      while ( resultSet.next() ) {
        val name = resultSet.getString("name")
        println("host, user = " + name + ", ")
      }
      */
    } catch {
      case e => e.printStackTrace
      System.out.println(e.printStackTrace);
    }
    connection.close()
// End - Read CSV File
    
  }
}