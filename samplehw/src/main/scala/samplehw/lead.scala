package samplehw

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.{ StructType, IntegerType, StringType, LongType, StructField };
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;
import java.util.Properties;
import java.sql.DriverManager
import java.sql.Connection
import com.databricks.spark.csv
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.functions._
import java.io.File;

object lead {

  // declaration and definition of function
  def fileRecordCount(sparkContext: JavaSparkContext, importFilePath: String): Long =
    {
      // Start - CSV File record count
      // Read a file
      val stringJavaRDD = sparkContext.textFile(importFilePath);
      var fCount: Long = stringJavaRDD.count();
      return fCount;
      // end - CSV File record count
    }

  def main(args: Array[String]) {
    println("Hello World")
    var connection: Connection = null
    val driver = "oracle.jdbc.OracleDriver"
    //val url = "jdbc:mysql://localhost/mysql"
    //val username = "root"
    //val password = "root"
    try {

      val folder = "D:\\Spark\\spark-2.4.3-bin-hadoop2.7\\scala ide\\projects\\samplehw\\dataFile\\";
      val inputFileName = "Lead1.csv";
      val importFilePath = folder.concat(inputFileName);
      val importFormatErrorFilePath = folder.concat("FormatError"); //.concat(inputFileName);
      val importInvalidErrorFilePath = folder.concat("InvalidError"); //.concat(inputFileName);
      val accountFilePath = "D:\\Spark\\spark-2.4.3-bin-hadoop2.7\\scala ide\\projects\\samplehw\\dataFile\\hz_parties_1.6M.csv"
      val sparkConf = new SparkConf()
        .setAppName("Example Spark App")
        .setMaster("local[*]") // Delete this line when submitting to a cluster
      /*
          .config("spark.executor.memory", "70g")
          .config("spark.driver.memory", "50g")
          .config("spark.memory.offHeap.enabled",true)
          .config("spark.memory.offHeap.size","16g")

          */
      val sparkContext = new JavaSparkContext(sparkConf);

      System.out.println("Number of lines in file = " + fileRecordCount(sparkContext, importFilePath));

      val spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()

      // Start - Read CSV File
      val sqlContext = new SQLContext(sparkContext);

      //custom schema definition of a file
      val LeadSchema = StructType(Array(
        StructField("leadNumber", StringType, true),
        StructField("customerId", StringType, true),
        StructField("primaryContactId", IntegerType, true),
        StructField("leadName", IntegerType, true),
        StructField("dealSize", StringType, true),
        StructField("city", StringType, true),
        StructField("state", IntegerType, true),
        StructField("Country", IntegerType, true),
        StructField("_corrupt_record", StringType, true) // used to find malformed rows
      ))

      val df = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        //.option("inferSchema","true") // auto find the schema
        .schema(LeadSchema) // use defined schema
        .load(importFilePath)
      //.select($"rownum", $"name") // used to choose a subset of columns

      // print schema
      println("Input File Schema : ");
      df.printSchema();
      df.rdd.cache();
      // print file
      df.rdd.foreach(println)

      // register data frame as db template
      df.registerTempTable("LeadInFile")

      // find malformed rows
      val badRows = df.filter("_corrupt_record is not null");
      badRows.cache(); //.show();

      badRows.write.format("com.databricks.spark.csv").save(importFormatErrorFilePath)

      //read data as sql
      //sqlContext.sql("SELECT * from Account where rownum >= 100").collect.foreach(println)

      //sqlContext.sql("SELECT * from Account where rownum >= 100 GROUP BY name").collect.foreach(println)

      //sqlContext.sql("SELECT * from Account").collect.foreach(println)

      // End - Read CSV File

      // Start - Validate

      // filter null partynumbers
      val invalidDF = df.filter("LeadNumber is NULL")
      invalidDF.cache(); //.show()
      invalidDF.write.format("com.databricks.spark.csv").save(importInvalidErrorFilePath)

      df.unpersist();
      invalidDF.unpersist();

      val validRowsDF = df.filter("LeadNumber is not NULL").select("leadNumber", "customerId", "primaryContactId",
        "leadName", "dealSize", "city", "state", "Country")
      validRowsDF.cache()
      //validRowsDF.show()

      // End - Validate

      // Get parties
      // Read from file
      //val fileExists = (new File(accountFilePath)).exists()
      //if (fileExists) 
      //{
        val partyIDDF = sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema","true") // auto find the schema
          //.schema(LeadSchema) // use defined schema
          .load(accountFilePath)
        partyIDDF.show();
      //} 
      
      //else {

        /*
    			val partyIDDF = spark.read
				  .format("jdbc")
				  .option("url", "jdbc:oracle:thin:fusion/welcome1@slc14hiy.us.oracle.com:1521/r13csmcf_b")
				  .option("dbtable", "fusion.hz_parties")
				  .option("driver", "oracle.jdbc.OracleDriver")
				  .load()
				  .select("party_id")
				  .where("STATUS like 'A' and creation_date > '20-JUL-19 06.09.21.331000000 PM'")
				  //.orderBy("creation_date");

    			partyIDDF.printSchema()
    			partyIDDF.show()
    			println(" Party count : " + partyIDDF.count())
    			partyIDDF.write.format("csv").save(accountFilePath)
    		
*/
      /*
        val url = "jdbc:oracle:thin:fusion/welcome1@slc14hiy.us.oracle.com:1521/r13csmcf_b"
        val rowList = new scala.collection.mutable.MutableList[Row]
        var cRow: Row = null

        // make the connection
        Class.forName(driver)
        connection = DriverManager.getConnection(url)
        // create the statement, and run the select query
        val statement = connection.createStatement()
        val partyResultSet = statement.executeQuery("select TO_CHAR(party_id) from fusion.hz_parties where STATUS like 'A' and creation_date > sysdate - 100")

        //Looping resultset
        var incr = 0;
        while (partyResultSet.next()) {
          //    adding one columns into a "Row" object
          cRow = RowFactory.create(partyResultSet.getObject(1)) //, resultSet.getObject(2))
          //    adding each rows into "List" object.
          incr += 1;
          println("party id = " + partyResultSet.getObject(1) + " :: " + incr)
          rowList += (cRow)
        }

        val PartySchema = StructType(Array(
          StructField("party_id", StringType, true)))
        //creates a dataframe
        val partyIDDF = sqlContext.createDataFrame(sparkContext.parallelize(rowList, 1), PartySchema)
        partyIDDF.show()
        partyIDDF.write.format("csv").save(accountFilePath)

        
        //}
       */
      
      // validate parties

      // ALL join
         val joinTypes = Seq("inner", "outer", "full", "full_outer", "left", "left_outer", "right", "right_outer", "left_semi", "left_anti")

      /*
    joinTypes foreach {joinType =>
        println(s"${joinType.toUpperCase()} JOIN")
        validRowsDF.join(right = partyIDDF, validRowsDF("customerId") <=> partyIDDF("party_id"), joinType = joinType).show()
    }


    val innerDF = validRowsDF.join(right = partyIDDF, validRowsDF("customerId") <=> partyIDDF("party_id"), joinType = "inner");
    val outerDF = validRowsDF.join(right = partyIDDF, validRowsDF("customerId") <=> partyIDDF("party_id"), joinType = "outer");
    val fullDF = validRowsDF.join(right = partyIDDF, validRowsDF("customerId") <=> partyIDDF("party_id"), joinType = "full");
    val full_outerDF = validRowsDF.join(right = partyIDDF, validRowsDF("customerId") <=> partyIDDF("party_id"), joinType = "full_outer");
    val leftDF = validRowsDF.join(right = partyIDDF, validRowsDF("customerId") <=> partyIDDF("party_id"), joinType = "left");
    val left_outerDF = validRowsDF.join(right = partyIDDF, validRowsDF("customerId") <=> partyIDDF("party_id"), joinType = "left_outer");
    val rightDF = validRowsDF.join(right = partyIDDF, validRowsDF("customerId") <=> partyIDDF("party_id"), joinType = "right");
    val right_outerDF = validRowsDF.join(right = partyIDDF, validRowsDF("customerId") <=> partyIDDF("party_id"), joinType = "right_outer");
    */
      println("Finding valid records using Join")
      val validPartyDF = validRowsDF.join(right = partyIDDF, validRowsDF("customerId") <=> partyIDDF("party_id"), joinType = "left_semi");
      println("COMPLETE : Finding valid records using Join")
      validPartyDF.show();

      println("Finding in-valid records using Join")
      val left_antiDF = validRowsDF.join(right = partyIDDF, validRowsDF("customerId") <=> partyIDDF("party_id"), joinType = "left_anti");
      println("COMPLETE : Finding in valid records using Join")
      left_antiDF.show();

      /*
    println("Inner")
    innerDF.show();
    println("outerDF")
    outerDF.show();
    println("fullDF")
    fullDF.show();
    println("full_outerDF")
    full_outerDF.show();
    println("leftDF")
    leftDF.show();
    println("left_outerDF")
    left_outerDF.show();
    println("rightDF")
    rightDF.show();
    println("right_outerDF")
    right_outerDF.show();
    println("left_semiDF")
    left_semiDF.show();
    println("left_antiDF")
    left_antiDF.show();
    */
      println(" Writing data to Invalid File")
      left_antiDF.write.mode(SaveMode.Append).format("com.databricks.spark.csv").save(importInvalidErrorFilePath)

      //release memory
      left_antiDF.unpersist();
      partyIDDF.unpersist();
      validRowsDF.unpersist();
      /*

// Get Leads

    val leadResultSet = statement.executeQuery("select LEAD_NUMBER from fusion.MKL_LM_LEADS")

    //Looping resultset
    incr=0;
    rowList.clear();
    while (leadResultSet.next()) {
   //    adding one columns into a "Row" object
       cRow = RowFactory.create(leadResultSet.getObject(1))//, resultSet.getObject(2))
   //    adding each rows into "List" object.
       incr += 1;
       println("LeadNumber = " + leadResultSet.getObject(1) + " :: " + incr)
       rowList += (cRow)
    }




    val LeadNumSchema = StructType(Array(
          StructField("leadNumber",StringType,true)))
    //creates a dataframe
    val leadNumDF = sqlContext.createDataFrame(sparkContext.parallelize(rowList ,2), LeadNumSchema)
    leadNumDF.show()
    */

      // Add default columns
      validPartyDF.registerTempTable("LeadInput")

      val renameInputDF = sqlContext.sql("select LeadNumber as LEAD_NUMBER, customerId as CUSTOMER_ID,primaryContactId as PRIMARY_CONTACT_ID,leadName as LEAD_NAME,dealSize as DEAL_SIZE,city,state from LeadInput");
      println(renameInputDF.columns);
      renameInputDF.printSchema();
      renameInputDF.show();

      val unique_df = renameInputDF.withColumn("LEAD_ID", monotonicallyIncreasingId)
        .withColumn("CREATION_DATE", lit("01-JAN-06 12.00.00.000000000 AM"))
        .withColumn("LAST_UPDATE_DATE", lit("01-JAN-06 12.00.00.000000000 AM"))
        .withColumn("CREATED_BY", lit("GS"))
        .withColumn("LAST_UPDATED_BY", lit("GS"));

      unique_df.printSchema()
      unique_df.cache().show();

      validPartyDF.unpersist();
      renameInputDF.unpersist();

      // import leads
      // Start - Write in to DB
      val connectionProperties = new Properties()
      connectionProperties.put("driver", "oracle.jdbc.OracleDriver")
      connectionProperties.put("numPartitions", "10") // parallel threads

      unique_df.write
        .mode(SaveMode.Append)
        .jdbc("jdbc:oracle:thin:fusion/welcome1@slc14hiy.us.oracle.com:1521/r13csmcf_b", "fusion.MKL_LM_LEADS", connectionProperties)

      System.out.println("write successful");

      unique_df.unpersist();

      /*
 // Start - Write in to DB
      val connectionProperties = new Properties()
      connectionProperties.put("driver", "oracle.jdbc.OracleDriver")
      connectionProperties.put("numPartitions", "2") // parallel threads

      validRowsDF.write
      .mode(SaveMode.Append)
      .jdbc("jdbc:oracle:thin:fusion/welcome1@slc14hiy.us.oracle.com:1521/r13csmcf_b", "fusion.stable", connectionProperties)

       System.out.println("write successful");
 // End - Write in to DB
*/

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
      case e =>
        e.printStackTrace
        System.out.println(e.printStackTrace);
        connection.close()
    }

  }
}