package com.hashmap

import java.io.FileNotFoundException

import com.hashmap._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, types}
import org.apache.spark.sql.functions._

object Application extends App{
  val spark: SparkSession = new SparkWrapper("sample","local").getSparkObject
  try
    {
      import spark.implicits._

    val dataQuality: DataFrame = spark.read.option("header","true").option("inferSchema","true").csv("hdfs://sandbox-hdp.hortonworks.com:8020//IndiaAffectedWaterQualityAreas.csv")
    val filteredDataQuality = dataQuality.filter(!col("State Name").contains("*") || !col("District Name").contains("*") || !col("Block Name").contains("*") || !col("Panchayat Name").contains("*") || !col("Village Name").contains("*") || !col("Habitation Name").contains("*") || !col("Quality Parameter").contains("*") || !col("Year").contains("*"))
    //filteredDataQuality.show(false)
    val putDefaultValue = udf(f = (input: Int) => {
      if (input==null) {
        -999
      }
      else
      {
        input
      }
    })

      val replace = udf(f = (input: String) => {
        if ((input==0 || input == null) && (input.contains("(") || input.contains(")"))) {
          input.replace("(","").replace(")","")
        }
        else
        {
          input
        }
      })
    val dataZipcodes = spark.read.option("header","true").option("inferSchema","true").csv("hdfs://sandbox-hdp.hortonworks.com:8020//DistrictsCodes2001.csv")
    val filteredZipCodes = dataZipcodes.filter(!col("Name of the State/Union territory and Districts").contains("*"))
      //.withColumn("State Name", replace(col("State Name")))
      .withColumn("State Code" , putDefaultValue(col("State Code")))
      .withColumn("District Code", putDefaultValue(col("District Code")))
    //filteredZipCodes.show(false)



    val joined = filteredDataQuality.join(filteredZipCodes, filteredDataQuality("State Name") === filteredZipCodes("Name of the State/Union territory and Districts"), "inner")
      //.filter(col("State Name")=== null)
        .select(col("State Name"),col("District Name"), col("State Code"), col("District Code"), col("Block Name"), col("Panchayat Name"), col("Village Name"), col("Habitation Name"), col("Quality Parameter"), col("Year"))
        .withColumnRenamed("State Name","State_Name")
        .withColumnRenamed("District Name", "District_Name")
        .withColumnRenamed("State Code","State_Code")
        .withColumnRenamed("District Code","district_code")
        .withColumnRenamed("Block Name", "block_Name")
        .withColumnRenamed("Panchayat Name", "Panchayat_Name")
        .withColumnRenamed("Village Name","village_name")
        .withColumnRenamed("Habitation Name", "Habitaion_Name")
        .withColumnRenamed("Quality Parameter","quality_parameter")

    //joined.printSchema()
    //joined.show(40,false)

     joined.write.format("orc").mode(SaveMode.Append).saveAsTable("WaterQuality")

      val frequencyData = joined.groupBy("village_name","quality_parameter","Year").count()
      val finaldata=frequencyData.select(
        col("village_name"),
        col("quality_parameter"),
        col("Year"),
        col("count").alias("frequency"))
      finaldata.write.format("orc").mode(SaveMode.Append).saveAsTable("frequency")

    }catch
      {
        case ex: NullPointerException => throw new NullPointerException("NullPointerException in VesselEncroachment"+ex.getMessage)
        case ex: FileNotFoundException => throw new FileNotFoundException("VesselEncroachment File Not Found"+ex.getMessage)
        case ex: Exception => throw new Exception(ex.getMessage)
      }


}
