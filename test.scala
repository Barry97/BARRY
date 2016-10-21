package main

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
 


object Barry{

	def main(args: Array[String]) {
		val conf = new SparkConf()
		.setAppName("WordCount")
		val sc = new SparkContext(conf)
		val test = sc.textFile("hdfs://sldifrdwbhn01.fr.intranet:8020/tmp/BARRY/simple1.txt")
		test.flatMap { line  =>
		line.split(" ") }
		.map { word => 
		(word, 1)
		}
		.reduceByKey(_ +_)
		.saveAsTextFile("hdfs://sldifrdwbhn01.fr.intranet:8020/tmp/BARRY/simple10.count.txt")
	}
}


