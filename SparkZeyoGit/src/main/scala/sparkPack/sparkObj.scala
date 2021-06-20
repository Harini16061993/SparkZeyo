package sparkPack
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object sparkObj {
  
  def main(args:Array[String]):Unit={ 	

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._



					println("========================raw df==================")

					val rawdf = spark.read.format("json").option("multiLine","true").load("file:///C://data//arrayjson_arrgen.json")
					rawdf.show()
					rawdf.printSchema()

					println("====================flatten df==========")

					val flattendf = rawdf.withColumn("Students",explode(col("Students"))).select(
							col("Students.gender"),
							col("Students.name"),
							col("address.Permanent_address"),
							col("address.temporary_address"),
							col("first_name"),
							col("second_name")
							)
					flattendf.show()
					flattendf.printSchema()
					
					println("====================complex df==========")

					val complexdf = flattendf.groupBy("Permanent_address","temporary_address","first_name","second_name")
					.agg(
							collect_list(
									struct(
											col("gender"),
											col("name")							        
											)
									).alias("Students")
							)

					val finaldf = complexdf.select(
					    col("Students"),
					    struct(
							col("Permanent_address"),
							col("temporary_address")
							).alias("address"),
							col("first_name"),
							col("second_name")
							)
							
							finaldf.show()
							finaldf.printSchema()

    }
  }