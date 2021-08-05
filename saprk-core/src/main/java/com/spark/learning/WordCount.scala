package com.spark.learning

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sparkContext = new SparkContext(sparkConf)
    val lines = sparkContext.textFile("data")
    val array = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortByKey(true).collect()
    array.foreach(println(_))

  }

}
