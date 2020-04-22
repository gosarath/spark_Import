package samplehw

object trail {
      /*
      val dbTable =
        "(select TO_CHAR(party_id) from fusion.hz_parties where STATUS like 'A' and creation_date > sysdate - 100) as MKL_LEADS";

      val connectionProperties1 = new Properties()
      connectionProperties1.put("driver", "oracle.jdbc.OracleDriver")
      
      val jdbcDF1 = 
        spark.read.jdbc("jdbc:oracle:thin:fusion/welcome1@slc14hiy.us.oracle.com:1521/r13csmcf_b", dbTable, "PARTY_ID", 0, 999999999, 10, connectionProperties1);

      jdbcDF1.printSchema();
      System.out.println("READ from DB table successful");
		  jdbcDF1.show(); 
		  */
  
  /*
      val jdbcDF = spark.read
				  .format("jdbc")
				  .option("url", "jdbc:oracle:thin:fusion/welcome1@slc14hiy.us.oracle.com:1521/r13csmcf_b")
				  .option("dbtable", "(select TO_CHAR(party_id) from fusion.hz_parties where STATUS like 'A' and creation_date > sysdate - 100) as MKL_LEADS")
				  .option("driver", "oracle.jdbc.OracleDriver")
				  .load();
    
      jdbcDF.printSchema();
      System.out.println("READ from DB table successful");
		  jdbcDF.show(); 
    */  
}