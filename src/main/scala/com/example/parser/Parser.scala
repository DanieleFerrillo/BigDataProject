package com.example.parser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


object Parser {
  def main(args: Array[String]): Unit = {

    val sc = new SparkConf().setAppName("ParserProject").setMaster("local[2]")
    val sparkContext = new SparkContext(sc)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    val jsonStr = "C:\\Users\\Daniele\\Desktop\\LYNX\\Lynx 2° anno\\Big Data\\PROGETTO\\2018-03-01-0.json.gz"

    val df = sqlContext.read.json(jsonStr)
    //df.show()

    //Trovare i singoli «actor»
    val actor = df.select("actor.*").distinct()
    actor.show()

    //Trovare i singoli «author», presente all’interno dei commit;
    val payload = df.select("payload.*")
    val commits = payload.select(explode(col("commits")))
    val dfComm = commits.select("col.*")
    val distinctAuthor = dfComm.select("author").distinct()
    distinctAuthor.show()

    //Trovare i singoli «repo»
    val repo = df.select("repo.*").distinct()
    repo.show()

    //Trovare i vari tipi di evento «type»;
    val `type` = df.select("type").distinct()
    `type`.show()

    //Contare il numero di «actor»;
    val nActor = actor.count()
    println(nActor)

    //Contare il numero di «repo»;
    val nRepo = repo.count()
    println(nRepo)

  }
}

