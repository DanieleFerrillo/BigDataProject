package com.example.parser

import org.apache.spark.{SparkConf, SparkContext}

object Parser {
  def main(args: Array[String]): Unit = {

    val sc = new SparkConf().setAppName("ParserProject").setMaster("local[2]")
    val sparkContext = new SparkContext(sc)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    val jsonStr = "C:\\Users\\Daniele\\Desktop\\LYNX\\Lynx 2° anno\\Big Data\\PROGETTO\\2018-03-01-0.json.gz"

    val jsonDF = sqlContext.read.json(jsonStr)

    jsonDF.show()

  }
}

