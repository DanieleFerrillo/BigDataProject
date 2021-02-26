package com.example.parser

import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{max, _}
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

import scala.reflect.macros.whitebox


object Parser {
  def main(args: Array[String]): Unit = {

    val sc = new SparkConf().setAppName("ParserProject").setMaster("local[2]")
    val sparkContext = new SparkContext(sc)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    val jsonStr = "C:\\Users\\Daniele\\Desktop\\LYNX\\Lynx 2° anno\\Big Data\\PROGETTO\\2018-03-01-0.json.gz"

    val df = sqlContext.read.json(jsonStr)

    val hiveContext = new HiveContext(sparkContext)
    import hiveContext.implicits._

    val newDf = df.withColumnRenamed("public", "publicField")
    val dfEvent = newDf.as[Event].toDF()

    /*
        //Trovare i singoli «actor»
        val actor = df.select("actor.*").distinct()
        actor.show()

        //Trovare i singoli «author», presente all’interno dei commit;
        val payload = df.select("payload.*")
        val commits = payload.select(explode(col("commits")))
        val dfComm = commits.select("col.*")
        val distinctAuthor = dfComm.select("author").distinct()
        distinctAuthor.show() //TODO verificare correttezza

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
     */


    /*
        //Contare il numero di «event» (con «event» intendo l’oggeto principale del json parsato) per ogni «actor»;
        val dfActorEvents = dfEvent.select($"actor", count($"*").over(Window.partitionBy($"actor")) as "ActorEventsCounter")
        //dfActorEvents.show()

        //Contare il numero di «event», divisi per «type» e «actor»;
        val dfTypeActorEvents = dfEvent.select($"type", $"actor", count($"*").over(Window.partitionBy($"type", $"actor")) as "TypeActorEventsCounter")
        //dfTypeActorEvents.show()

        //Contare il numero di «event», divisi per «type», «actor», «repo»;
        val dfTypeActorRepoEvents = dfEvent.select($"type", $"actor", $"repo", count($"*").over(Window.partitionBy($"type", $"actor", $"repo")) as "TypeActorRepoEventsCounter")
        //dfTypeActorRepoEvents.show() //TODO verificare correttezza

        //Contare il numero di «event», divisi per «type», «actor», «repo» e secondo(il secondo è inteso come per tutto quel secondo. Quindi bisogna trasformare il timestamp in modo da avere solo il valore dei secondi, poi raggruppiamo solo su questo campo.);
        val dfEventWithSecond = dfEvent.withColumn("second", second(col("created_at")))
        //val prova1 = dfEventWithSecond.select($"type", $"actor", $"repo", $"second", count($"*").over(Window.partitionBy($"type", $"actor", $"repo", $"second")) as "EventsCounter").show()
        //val prova2 = dfEventWithSecond.groupBy($"type", $"actor.login", $"repo",$"second").count().show()

        //Trovare il massimo/minimo numero di «event» per secondo;
        val dfNumEventXSecond = dfEventWithSecond.select($"second", count($"*").over(Window.partitionBy($"second")) as "Counter")
        //val dfMaxNumEventXSecond = dfNumEventXSecond.agg(max("Counter") as "MaxEventXSecondCounter").show()
        //val dfMinNumEventXSecond = dfNumEventXSecond.agg(min("Counter") as "MinNumEventXSecondCounter").show()

        //Trovare il massimo/minimo numero di «event» per «actor»;
        val dfNumEventXActor = dfEvent.select($"actor", count($"*").over(Window.partitionBy($"actor")) as "Counter")
        //val dfMaxNumEventXActor = dfNumEventXActor.agg(max("Counter") as "MaxEventXActorCounter").show()
        //val dfMinNumEventXActor = dfNumEventXActor.agg(min("Counter") as "MinEventXActorCounter").show()

        //Trovare il massimo/minimo numero di «event» per «repo»;
        val dfNumEventXRepo = dfEvent.select($"repo", count($"*").over(Window.partitionBy($"repo")) as "Counter")
        //val dfMaxNumEventXRepo = dfNumEventXRepo.agg(max("Counter") as "MaxEventXRepoCounter").show()
        //val dfMinNumEventXRepo = dfNumEventXRepo.agg(min("Counter") as "MinEventXRepoCounter").show()

        //Trovare il massimo/minimo numero di «event» per secondo per «actor»;
        val dfNumEventXSecondActor = dfEventWithSecond.select($"second", $"actor", count($"*").over(Window.partitionBy($"second", $"actor")) as "Counter")
        //val dfMaxNumEventXSecondActor = dfNumEventXSecondActor.agg(max("Counter") as "MaxEventXSecondActorCounter").show()
        //val dfMinNumEventXSecondActor = dfNumEventXSecondActor.agg(min("Counter") as "MinEventXSecondActorCounter").show()

        //Trovare il massimo/minimo numero di «event» per secondo per «repo»;
        val dfNumEventXSecondRepo = dfEventWithSecond.select($"second", $"repo", count($"*").over(Window.partitionBy($"second", $"repo")) as "Counter")
        //val dfMaxNumEventXSecondRepo = dfNumEventXSecondRepo.agg(max("Counter") as "MaxEventXSecondRepoCounter").show()
        //val dfMinNumEventXSecondRepo = dfNumEventXSecondRepo.agg(min("Counter") as "MinEventXSecondRepoCounter").show()

        //Trovare il massimo/minimo numero di «event» per secondo per «repo» e «actor»;
        val dfNumEventXSecondRepoActor = dfEventWithSecond.select($"second", $"repo", $"actor", count($"*").over(Window.partitionBy($"second", $"repo", $"actor")) as "Counter")
        //val dfMaxNumEventXSecondRepoActor = dfNumEventXSecondRepoActor.agg(max("Counter") as "MaxEventXSecondRepoActorCounter").show()
        //val dfMinNumEventXSecondRepoActor = dfNumEventXSecondRepoActor.agg(min("Counter") as "MinEventXSecondRepoActorCounter").show()
    */


    ///////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////
    ////                                            RDD                                            ////
    ///////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////

    val jsonRdd = newDf.as[Event].rdd

    /*
        //Trovare i singoli «actor»;
        val rddActor = jsonRdd.map(_.actor)
        //rddActor.foreach(println)

        //Trovare i singoli «author», presente all’interno dei commit;
        //val rddCommit = newDf.as[Commit].rdd.map(_.author).distinct()
        //val rddAuthor = rddCommit.map(x=>x.author).distinct()
        //rddCommit.foreach(println) //TODO da riimplementare

        //Trovare i singoli «repo»
        val rddRepo = jsonRdd.map(_.repo)
        //rddRepo.foreach(println)

        //Trovare i vari tipi di evento «type»;
        val rddType = jsonRdd.map(_.`type`)
        //rddType.foreach(println)

        //Contare il numero di «actor»;
        val rddActorCount = rddActor.count()
        //println(rddActorCount)

        //Contare il numero di «repo»;
        val rddRepoCount = rddRepo.count()
        //println(rddRepoCount)
    */


    /*
        //Contare il numero di «event» (con «event» intendo l’oggeto principale del json parsato) per ogni «actor»;
        //// METODO A ////
        val pairRddActorEvents = jsonRdd.map(x => (x.actor, x)).groupByKey().map(x => (x._1, x._2.size))
        //pairRddActorEvents.foreach(println)

        //// METODO B ////
        val pairRddActorEvents_2 = jsonRdd.map(x => (x.actor, 1)).reduceByKey(_ + _)
        //pairRddActorEvents_2.foreach(println


        //Contare il numero di «event», divisi per «type» e «actor»;
        //// METODO A ////
        val pairRddTypeActorEvents = jsonRdd.map(x => ((x.`type`, x.actor), x)).groupByKey().map(x => (x._1, x._2.size))
        //pairRddTypeActorEvents.foreach(println)

        //// METODO B ////
        val pairRddTypeActorEvents_2 = jsonRdd.map(x => ((x.`type`, x.actor.login), 1)).reduceByKey(_ + _)
        //pairRddTypeActorEvents_2.foreach(println)


        //Contare il numero di «event», divisi per «type», «actor», «repo»;
        //// METODO A ////
        val pairRddTypeActorRepoEvents = jsonRdd.map(x => ((x.`type`, x.actor, x.repo), x)).groupByKey().map(x => (x._1, x._2.size))
        //pairRddTypeActorRepoEvents.foreach(println)

        //// METODO B ////
        val pairRddTypeActorRepoEvents_2 = jsonRdd.map(x => ((x.`type`, x.actor.login, x.repo.id), 1)).reduceByKey(_ + _)
        //pairRddTypeActorRepoEvents_2.foreach(println)


        //Contare il numero di «event», divisi per «type», «actor», «repo» e secondo(il secondo è inteso come per tutto quel secondo. Quindi bisogna trasformare il timestam in modo da avere solo il valore dei secondi, poi raggruppiamo solo su questo campo.);
        //// METODO A ////
        val pairRddTypeActorRepoSecondEvents = jsonRdd
          .map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.`type`, x.actor.id, x.repo.name), x))
          .groupByKey().map(x => (x._1, x._2.size))
        //pairRddTypeActorRepoSecondEvents.foreach(println)

        //// METODO B ////
        val pairRddTypeActorRepoSecondEvents_2 = jsonRdd
          .map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.`type`, x.actor.id, x.repo.name), 1))
          .reduceByKey(_ + _)
        //pairRddTypeActorRepoSecondEvents_2.foreach(println)


        //Trovare il massimo/minimo numero di «event» per secondo;
        val rddNumEventXSecond = jsonRdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute), 1))
          .reduceByKey(_ + _).map(x => (x._2, x._1))
        //println(rddNumEventXSecond.max())
        //println(rddNumEventXSecond.min())

        //Trovare il massimo/minimo numero di «event» per «actor»;
        val rddNumEventXActor = jsonRdd.map(x => (x.actor, 1)).reduceByKey(_ + _).map(x => (x._2, x._1.toString))
        //println(rddNumEventXActor.max())
        //println(rddNumEventXActor.min())

        //Trovare il massimo/minimo numero di «event» per «repo»;
        val rddNumEventXRepo = jsonRdd.map(x => (x.repo, 1)).reduceByKey(_ + _).map(x => (x._2, x._1.toString))
        //println(rddNumEventXRepo.max())
        //println(rddNumEventXRepo.min())

        //Trovare il massimo/minimo numero di «event» per secondo per «actor»;
        val rddNumEventXSecondActor = jsonRdd.map(x => (((new DateTime(x.created_at.getTime).getSecondOfMinute), x.actor), 1))
          .reduceByKey(_ + _).map(x => (x._2, x._1.toString()))
        //println(rddNumEventXSecondActor.max())
        //println(rddNumEventXSecondActor.min())

        //Trovare il massimo/minimo numero di «event» per secondo per «repo»;
        val rddNumEventXSecondRepo = jsonRdd.map(x => (((new DateTime(x.created_at.getTime).getSecondOfMinute), x.repo), 1))
          .reduceByKey(_ + _).map(x => (x._2, x._1.toString()))
        //println(rddNumEventXSecondRepo.max())
        //println(rddNumEventXSecondRepo.min())

        //Trovare il massimo/minimo numero di «event» per secondo per «repo» e «actor»;
        val rddNumEventXSecondRepoActor = jsonRdd.map(x => (((new DateTime(x.created_at.getTime).getSecondOfMinute), x.repo, x.actor), 1))
          .reduceByKey(_ + _).map(x => (x._2, x._1.toString()))
        //println(rddNumEventXSecondRepoActor.max())
        //println(rddNumEventXSecondRepoActor.min())
    */


  }
}

