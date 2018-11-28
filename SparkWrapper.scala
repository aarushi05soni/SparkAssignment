package com.hashmap
import org.apache.spark.sql.{SQLContext, SparkSession}

class SparkWrapper(appName:String, master:String) {

  def getSparkObject :SparkSession ={

    val spark: SparkSession = SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    spark
  }

}
