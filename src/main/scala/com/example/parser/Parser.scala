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
    val dfActor = dfEvent.select("actor.*")
    dfActor.distinct().show()

    //Trovare i singoli «author», presente all’interno dei commit;
    //// METODO A ////
    val dfPayload = dfEvent.select("payload.*")
    val dfCommit = dfPayload.select(explode($"commits")).select("col.*")
    val dfAuthor = dfCommit.select("author")
    dfAuthor.distinct().show()

    //// METODO B ////
    val dfAuthor2 = dfEvent.select(explode($"payload.commits.author") as "author2")
    dfAuthor2.distinct().show()

    //Trovare i singoli «repo»
    val dfRepo = dfEvent.select("repo.*")
    dfRepo.distinct().show()

    //Trovare i vari tipi di evento «type»;
    val dfType = dfEvent.select("type")
    dfType.distinct().show()

    //Contare il numero di «actor»;
    val dfNumActor = dfActor.distinct().count()
    println(dfNumActor)

    //Contare il numero di «repo»;
    val dfNumRepo = dfRepo.distinct().count()
    println(dfNumRepo)
*/


    val dfEventWithSecond = dfEvent.withColumn("second", second(col("created_at")))

/*
    //Contare il numero di «event» (con «event» intendo l’oggeto principale del json parsato) per ogni «actor»;
    val dfActorEvents = dfEvent.select($"actor", count($"*").over(Window.partitionBy($"actor")) as "ActorEventsCounter")
    dfActorEvents.distinct().show()

    //Contare il numero di «event», divisi per «type» e «actor»;
    val dfTypeActorEvents = dfEvent.select($"type", $"actor", count($"*").over(Window.partitionBy($"type", $"actor")) as "TypeActorEventsCounter")
    dfTypeActorEvents.distinct().show()

    //Contare il numero di «event», divisi per «type», «actor», «repo»;
    val dfTypeActorRepoEvents = dfEvent.select($"type", $"actor", $"repo", count($"*").over(Window.partitionBy($"type", $"actor", $"repo")) as "TypeActorRepoEventsCounter")
    dfTypeActorRepoEvents.distinct().show()

    //Contare il numero di «event», divisi per «type», «actor», «repo» e secondo(il secondo è inteso come per tutto quel secondo. Quindi bisogna trasformare il timestamp in modo da avere solo il valore dei secondi, poi raggruppiamo solo su questo campo.);
    //// METODO A ////
    val dfTypeActorRepoSecondEvents = dfEventWithSecond.select($"type", $"actor", $"repo", $"second", count($"*").over(Window.partitionBy($"type", $"actor", $"repo", $"second")) as "TypeActorRepoSecondEventsCounter")
    dfTypeActorRepoSecondEvents.distinct().show()

    //// METODO B ////
    val dfTypeActorRepoSecondEvents2 = dfEventWithSecond.groupBy($"type", $"actor.login", $"repo", $"second").count()
    dfTypeActorRepoSecondEvents2.distinct().show()

    //Trovare il massimo/minimo numero di «event» per secondo;
    val dfNumEventXSecond = dfEventWithSecond.select($"second", count($"*").over(Window.partitionBy($"second")) as "Counter")
    val dfMaxNumEventXSecond = dfNumEventXSecond.agg(max("Counter") as "MaxEventXSecondCounter").show()
    val dfMinNumEventXSecond = dfNumEventXSecond.agg(min("Counter") as "MinNumEventXSecondCounter").show()

    //Trovare il massimo/minimo numero di «event» per «actor»;
    val dfNumEventXActor = dfEvent.select($"actor", count($"*").over(Window.partitionBy($"actor")) as "Counter")
    val dfMaxNumEventXActor = dfNumEventXActor.agg(max("Counter") as "MaxEventXActorCounter").show()
    val dfMinNumEventXActor = dfNumEventXActor.agg(min("Counter") as "MinEventXActorCounter").show()

    //Trovare il massimo/minimo numero di «event» per «repo»;
    val dfNumEventXRepo = dfEvent.select($"repo", count($"*").over(Window.partitionBy($"repo")) as "Counter")
    val dfMaxNumEventXRepo = dfNumEventXRepo.agg(max("Counter") as "MaxEventXRepoCounter").show()
    val dfMinNumEventXRepo = dfNumEventXRepo.agg(min("Counter") as "MinEventXRepoCounter").show()

    //Trovare il massimo/minimo numero di «event» per secondo per «actor»;
    val dfNumEventXSecondActor = dfEventWithSecond.select($"second", $"actor", count($"*").over(Window.partitionBy($"second", $"actor")) as "Counter")
    val dfMaxNumEventXSecondActor = dfNumEventXSecondActor.agg(max("Counter") as "MaxEventXSecondActorCounter").show()
    val dfMinNumEventXSecondActor = dfNumEventXSecondActor.agg(min("Counter") as "MinEventXSecondActorCounter").show()

    //Trovare il massimo/minimo numero di «event» per secondo per «repo»;
    val dfNumEventXSecondRepo = dfEventWithSecond.select($"second", $"repo", count($"*").over(Window.partitionBy($"second", $"repo")) as "Counter")
    val dfMaxNumEventXSecondRepo = dfNumEventXSecondRepo.agg(max("Counter") as "MaxEventXSecondRepoCounter").show()
    val dfMinNumEventXSecondRepo = dfNumEventXSecondRepo.agg(min("Counter") as "MinEventXSecondRepoCounter").show()

    //Trovare il massimo/minimo numero di «event» per secondo per «repo» e «actor»;
    val dfNumEventXSecondRepoActor = dfEventWithSecond.select($"second", $"repo", $"actor", count($"*").over(Window.partitionBy($"second", $"repo", $"actor")) as "Counter")
    val dfMaxNumEventXSecondRepoActor = dfNumEventXSecondRepoActor.agg(max("Counter") as "MaxEventXSecondRepoActorCounter").show()
    val dfMinNumEventXSecondRepoActor = dfNumEventXSecondRepoActor.agg(min("Counter") as "MinEventXSecondRepoActorCounter").show()
*/


    val dfEventWithCommit = dfEvent.withColumn("commit", explode($"payload.commits"))
    val dfEventWithCommitAndSecond = dfEventWithCommit.withColumn("second", second(col("created_at")))

/*
    //Contare il numero di «commit»;
    val dfCommitCount = dfEventWithCommit.select(count($"commit").over(Window.partitionBy()) as "CommitsCounter")
    dfCommitCount.distinct().show()

    //Contare il numero di «commit» per ogni «actor»;
    val dfActorCommits = dfEventWithCommit.select($"actor", count($"commit").over(Window.partitionBy($"actor")) as "ActorCommitsCounter")
    dfActorCommits.distinct().show()
    println(dfActorCommits.distinct().count())

    //Contare il numero di «commit», divisi per «type» e «actor»;
    val dfTypeActorCommits = dfEventWithCommit.select($"type", $"actor", count($"commit").over(Window.partitionBy($"type", $"actor")) as "TypeActorCommitsCounter")
    dfTypeActorCommits.distinct().show()

    //Contare il numero di «commit», divisi per «type», «actor» e «event»;
    val dfTypeActorEventCommits = dfEventWithCommit.select($"type" as "Type", $"actor" as "Actor", $"*", count($"commit").over(Window.partitionBy($"Type", $"Actor", $"actor", $"created_at", $"id", $"org", $"payload", $"publicField", $"repo", $"type")) as "TypeActorEventCommitsCounter")
    dfTypeActorEventCommits.distinct().show() //TODO da chiedere

    //Contare il numero di «commit», divisi per «type», «actor» e secondo;
    val dfTypeActorSecondCommits = dfEventWithCommitAndSecond.select($"type", $"actor", $"second", count($"commit").over(Window.partitionBy($"type", $"actor", $"second")) as "TypeActorSecondCounter")
    dfTypeActorSecondCommits.distinct().show()

    //Trovare il massimo/minimo numero di «commit» per secondi;
    val dfNumCommitXSecond = dfEventWithCommitAndSecond.select($"second", count($"commit").over(Window.partitionBy($"second")) as "Counter")
    val dfMaxNumCommitXSecond = dfNumCommitXSecond.agg(max("Counter") as "MaxNumCommitXSecondCounter").show()
    val dfMinNumCommitXSecond = dfNumCommitXSecond.agg(min("Counter") as "MinNumCommitXSecondCounter").show()

    //Trovare il massimo/minimo numero di «commit» per «actor»;
    val dfNumCommitXActor = dfEventWithCommit.select($"actor", count($"commit").over(Window.partitionBy($"actor")) as "Counter")
    val dfMaxNumCommitXActor = dfNumCommitXActor.agg(max("Counter") as "MaxNumCommitXActorCounter").show()
    val dfMinNumCommitXActor = dfNumCommitXActor.agg(min("Counter") as "MinNumCommitXActorCounter").show()

    //Trovare il massimo/minimo numero di «commit» per «repo»;
    val dfNumCommitXRepo = dfEventWithCommit.select($"repo", count($"commit").over(Window.partitionBy($"repo")) as "Counter")
    val dfMaxNumCommitXRepo = dfNumCommitXRepo.agg(max("Counter") as "MaxNumCommitXRepoCounter").show()
    val dfMinNumCommitXRepo = dfNumCommitXRepo.agg(min("Counter") as "MinNumCommitXRepoCounter").show()

    //Trovare il massimo/minimo numero di «commit» per secondo per «actor»;
    val dfNumCommitXSecondActor = dfEventWithCommitAndSecond.select($"second", $"actor", count($"commit").over(Window.partitionBy($"second", $"actor")) as "Counter")
    val dfMaxNumCommitXSecondActor = dfNumCommitXSecondActor.agg(max("Counter") as "MaxNumCommitXSecondActorCounter").show()
    val dfMinNumCommitXSecondActor = dfNumCommitXSecondActor.agg(min("Counter") as "MinNumCommitXSecondActorCounter").show()

    //Trovare il massimo/minimo numero di «commit» per secondo per «repo»;
    val dfNumCommitXSecondRepo = dfEventWithCommitAndSecond.select($"second", $"repo", count($"commit").over(Window.partitionBy($"second", $"repo")) as "Counter")
    val dfMaxNumCommitXSecondRepo = dfNumCommitXSecondRepo.agg(max("Counter") as "MaxNumCommitXSecondRepoCounter").show()
    val dfMinNumCommitXSecondRepo = dfNumCommitXSecondRepo.agg(min("Counter") as "MinNumCommitXSecondRepoCounter").show()

    //Trovare il massimo/minimo numero di «commit» per secondo per «repo» e «actor»;
    val dfNumCommitXSecondRepoActor = dfEventWithCommitAndSecond.select($"second", $"repo", $"actor", count($"commit").over(Window.partitionBy($"second", $"repo", $"actor")) as "Counter")
    val dfMaxNumCommitXSecondRepoActor = dfNumCommitXSecondRepoActor.agg(max("Counter") as "MaxNumCommitXSecondRepoActorCounter").show()
    val dfMinNumCommitXSecondRepoActor = dfNumCommitXSecondRepoActor.agg(min("Counter") as "MinNumCommitXSecondRepoActorCounter").show()
*/


/*
    //Contare il numero di «actor» attivi per ogni «secondo»;
    val dfSecondActors = dfEventWithSecond.select($"second", count($"actor").over(Window.partitionBy($"second")) as "SecondActorsCounter")
    dfSecondActors.distinct().show()

    //Contare il numero di «actor», divisi per «type» e «secondo»;
    val dfTypeSecondActors = dfEventWithSecond.select($"type", $"second", count($"actor").over(Window.partitionBy($"type", $"second")) as "TypeSecondActorsCounter")
    dfTypeSecondActors.distinct().show()

    //Contare il numero di «actor», divisi per «repo»,«type» e «secondo»;
    val dfRepoTypeSecondActors = dfEventWithSecond.select($"repo", $"type", $"second", count($"actor").over(Window.partitionBy($"repo", $"type", $"second")) as "RepoTypeSecondActorsCounter")
    dfRepoTypeSecondActors.distinct().show()

    //Il massimo/minimo numero di «actor» attivi per secondo;
    val dfNumActorXSecond = dfEventWithSecond.select($"second", count($"actor").over(Window.partitionBy($"second")) as "Counter")
    val dfMaxNumActorXSecond = dfNumActorXSecond.agg(max("Counter") as "MaxNumActorXSecondCounter").show()
    val dfMinNumActorXSecond = dfNumActorXSecond.agg(min("Counter") as "MinNumActorXSecondCounter").show()

    //Il massimo/minimo numero di «actor» attivi per secondo, «type»;
    val dfNumActorXSecondType = dfEventWithSecond.select($"second", $"type", count($"actor").over(Window.partitionBy($"second", $"type")) as "Counter")
    val dfMaxNumActorXSecondType = dfNumActorXSecondType.agg(max("Counter") as "MaxNumActorXSecondTypeCounter").show()
    val dfMinNumActorXSecondType = dfNumActorXSecondType.agg(min("Counter") as "MinNumActorXSecondTypeCounter").show()

    //Il massimo/minimo numero di «actor» attivi per secondo, «type» e «repo»;
    val dfNumActorXSecondTypeRepo = dfEventWithSecond.select($"second", $"type", $"repo", count($"actor").over(Window.partitionBy($"second", $"type", $"repo")) as "Counter")
    val dfMaxNumActorXSecondTypeRepo = dfNumActorXSecondTypeRepo.agg(max("Counter") as "MaxNumActorXSecondTypeRepoCounter").show()
    val dfMinNumActorXSecondTypeRepo = dfNumActorXSecondTypeRepo.agg(min("Counter") as "MinNumActorXSecondTypeRepoCounter").show()
*/


    ///////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////
    ////                                            RDD                                            ////
    ///////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////


    val jsonRdd = newDf.as[Event].rdd

/*
    //Trovare i singoli «actor»;
    val rddActor = jsonRdd.map(_.actor).distinct()
    rddActor.take(20).foreach(println)

    //Trovare i singoli «author», presente all’interno dei commit;
    val rddCommit = dfCommit.as[Commit].rdd
    val rddAuthor = rddCommit.map(_.author).distinct()
    rddAuthor.take(20).foreach(println)

    //Trovare i singoli «repo»
    val rddRepo = jsonRdd.map(_.repo).distinct()
    rddRepo.take(20).foreach(println)

    //Trovare i vari tipi di evento «type»;
    val rddType = jsonRdd.map(_.`type`).distinct()
    rddType.foreach(println)

    //Contare il numero di «actor»;
    val rddActorCount = rddActor.distinct().count()
    println(rddActorCount)

    //Contare il numero di «repo»;
    val rddRepoCount = rddRepo.distinct().count()
    println(rddRepoCount)
*/


/*
    //Contare il numero di «event» (con «event» intendo l’oggeto principale del json parsato) per ogni «actor»;
    //// METODO A ////
    val rddActorEvents = jsonRdd.map(x => (x.actor, x)).groupByKey().map(x => (x._1, x._2.size))
    rddActorEvents.take(20).foreach(println)

    //// METODO B ////
    val rddActorEvents_2 = jsonRdd.map(x => (x.actor, 1)).reduceByKey(_ + _)
    rddActorEvents_2.take(20).foreach(println)

    //Contare il numero di «event», divisi per «type» e «actor»;
    //// METODO A ////
    val rddTypeActorEvents = jsonRdd.map(x => ((x.`type`, x.actor), x)).groupByKey().map(x => (x._1, x._2.size))
    rddTypeActorEvents.take(20).foreach(println)

    //// METODO B ////
    val rddTypeActorEvents_2 = jsonRdd.map(x => ((x.`type`, x.actor), 1)).reduceByKey(_ + _)
    rddTypeActorEvents_2.take(20).foreach(println)

    //Contare il numero di «event», divisi per «type», «actor», «repo»;
    //// METODO A ////
    val rddTypeActorRepoEvents = jsonRdd.map(x => ((x.`type`, x.actor, x.repo), x)).groupByKey().map(x => (x._1, x._2.size))
    rddTypeActorRepoEvents.take(20).foreach(println)

    //// METODO B ////
    val rddTypeActorRepoEvents_2 = jsonRdd.map(x => ((x.`type`, x.actor, x.repo), 1)).reduceByKey(_ + _)
    rddTypeActorRepoEvents_2.take(20).foreach(println)

    //Contare il numero di «event», divisi per «type», «actor», «repo» e secondo(il secondo è inteso come per tutto quel secondo. Quindi bisogna trasformare il timestam in modo da avere solo il valore dei secondi, poi raggruppiamo solo su questo campo.);
    //// METODO A ////
    val rddTypeActorRepoSecondEvents = jsonRdd
      .map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.`type`, x.actor, x.repo), x))
      .groupByKey().map(x => (x._1, x._2.size))
    rddTypeActorRepoSecondEvents.take(20).foreach(println)

    //// METODO B ////
    val rddTypeActorRepoSecondEvents_2 = jsonRdd
      .map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.`type`, x.actor, x.repo), 1))
      .reduceByKey(_ + _)
    rddTypeActorRepoSecondEvents_2.take(20).foreach(println)

    //Trovare il massimo/minimo numero di «event» per secondo;
    val rddNumEventXSecond = jsonRdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute), 1))
      .reduceByKey(_ + _).map(x => (x._2, x._1))
    println(rddNumEventXSecond.max())
    println(rddNumEventXSecond.min())

    //Trovare il massimo/minimo numero di «event» per «actor»;
    val rddNumEventXActor = jsonRdd.map(x => (x.actor, 1)).reduceByKey(_ + _).map(x => (x._2, x._1.toString))
    println(rddNumEventXActor.max())
    println(rddNumEventXActor.min())

    //Trovare il massimo/minimo numero di «event» per «repo»;
    val rddNumEventXRepo = jsonRdd.map(x => (x.repo, 1)).reduceByKey(_ + _).map(x => (x._2, x._1.toString))
    println(rddNumEventXRepo.max())
    println(rddNumEventXRepo.min())

    //Trovare il massimo/minimo numero di «event» per secondo per «actor»;
    val rddNumEventXSecondActor = jsonRdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.actor), 1))
      .reduceByKey(_ + _).map(x => (x._2, x._1.toString()))
    println(rddNumEventXSecondActor.max())
    println(rddNumEventXSecondActor.min())

    //Trovare il massimo/minimo numero di «event» per secondo per «repo»;
    val rddNumEventXSecondRepo = jsonRdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo), 1))
      .reduceByKey(_ + _).map(x => (x._2, x._1.toString()))
    println(rddNumEventXSecondRepo.max())
    println(rddNumEventXSecondRepo.min())

    //Trovare il massimo/minimo numero di «event» per secondo per «repo» e «actor»;
    val rddNumEventXSecondRepoActor = jsonRdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo, x.actor), 1))
      .reduceByKey(_ + _).map(x => (x._2, x._1.toString()))
    println(rddNumEventXSecondRepoActor.max())
    println(rddNumEventXSecondRepoActor.min())
*/


    //TODO chiedere come lavorare con i singoli commit da rdd
    //Errors: org.apache.spark.SparkException: Cannot use map-side combining with array keys
    //java.lang.NullPointerException

    val test = jsonRdd.map(x=>x.payload.commits.foreach(x=>x) != null)
    //test.foreach(println)

    //VERSIONE SOPRA LAVORA CON ARRAY DI COMMIT, QUELLA SOTTO LAVORA CON DF E TRASFORMA IN RDD
    //Contare il numero di «commit»;
    val nCommit = jsonRdd.map(x=>x.payload.commits)
    //println(nCommit.count())

    //println(dfEventWithCommit.rdd.count())


    //Contare il numero di «commit» per ogni «actor»;
    val nCommitActor = jsonRdd.map(x => (x.actor, x.payload.commits)).groupByKey().map(x => (x._1, x._2.size))
    //nCommitActor.take(20).foreach(println)

    val rddActorCommits = dfEventWithCommit.select($"actor", count($"commit").over(Window.partitionBy($"actor")) as "ActorCommitsCounter").distinct().rdd
    //rddActorCommits.take(20).foreach(println)


    //Contare il numero di «commit», divisi per «type» e «actor»;
    val nCommitTypeActor = jsonRdd.map(x => ((x.actor, x.`type`), x.payload.commits)).groupByKey().map(x => (x._1, x._2.size))
    //nCommitTypeActor.take(20).foreach(println)

    val rddTypeActorCommits = dfEventWithCommit.select($"type", $"actor", count($"commit").over(Window.partitionBy($"type", $"actor")) as "TypeActorCommitsCounter").distinct().rdd
    //rddTypeActorCommits.take(20).foreach(println)


    //Contare il numero di «commit», divisi per «type», «actor» e «event»;
    val nCommitTypeActorEvent = jsonRdd.map(x => ((x.`type`, x.actor, x), x.payload.commits)).groupByKey().map(x => (x._1, x._2.size))
    //nCommitTypeActorEvent.take(20).foreach(println)

    val rddTypeActorEventCommits = dfEventWithCommit.select($"type" as "Type", $"actor" as "Actor", $"*", count($"commit")
      .over(Window.partitionBy($"Type", $"Actor", $"actor", $"created_at", $"id", $"org", $"payload", $"publicField", $"repo", $"type")) as "TypeActorEventCommitsCounter")
        .distinct().rdd
    //rddTypeActorEventCommits.take(20).foreach(println)


    //Contare il numero di «commit», divisi per «type», «actor» e secondo;
    val nCommitTypeActorSecond = jsonRdd.map(x => ((x.`type`, x.actor, new DateTime(x.created_at.getTime).getSecondOfMinute), x.payload.commits)).groupByKey().map(x => (x._1, x._2.size))
    //nCommitTypeActorSecond.take(20).foreach(println)

    val rddTypeActorSecondCommits = dfEventWithCommitAndSecond.select($"type", $"actor", $"second", count($"commit")
      .over(Window.partitionBy($"type", $"actor", $"second")) as "TypeActorSecondCounter").distinct().rdd
    //rddTypeActorSecondCommits.take(20).foreach(println)


    //TODO Trovare il massimo/minimo numero di «commit» per secondi;
    val rddNumCommitXSecond = jsonRdd.distinct().map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute), x.payload.commits.foreach(x=>x)))
      .reduceByKey(_ + _.toString).map(x => (x._2, x._1))
    //println(rddNumCommitXSecond.max())
    //println(rddNumCommitXSecond.min())

    //TODO Trovare il massimo/minimo numero di «commit» per «actor»;
    val rddNumCommitXActor = jsonRdd.map(x=>(x.actor, x.payload.commits.foreach(_.toString)))
      .groupByKey().map(x => (x._2, x._1.toString))
    //println(rddNumCommitXActor.max())
    //println(rddNumCommitXActor.min()

    //TODO Trovare il massimo/minimo numero di «commit» per «repo»;
    val rddNumCommitXRepo = jsonRdd.map(x=>(new DateTime(x.created_at.getTime).getSecondOfMinute, x.payload.commits.size))
      .groupByKey().map(x => (x._2, x._1))
    //println(rddNumCommitXRepo.max())
    //println(rddNumCommitXRepo.min())

    //TODO Trovare il massimo/minimo numero di «commit» per secondo per «actor»;
    //TODO Trovare il massimo/minimo numero di «commit» per secondo per «repo»;
    //TODO Trovare il massimo/minimo numero di «commit» per secondo per «repo» e «actor»;



/*
    //Contare il numero di «actor» attivi per ogni «secondo»;
    val rddSecondActor = jsonRdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute), x.actor)).groupByKey().map(x => (x._1, x._2.size))
    rddSecondActor.take(20).foreach(println)

    //Contare il numero di «actor», divisi per «type» e «secondo»;
    val rddTypeSecondActor = jsonRdd.map(x => ((x.`type`, new DateTime(x.created_at.getTime).getSecondOfMinute), x.actor)).groupByKey().map(x => (x._1, x._2.size))
    rddTypeSecondActor.take(20).foreach(println)

    //Contare il numero di «actor», divisi per «repo»,«type» e «secondo»;
    val rddRepoTypeSecondActor = jsonRdd.map(x => ((x.repo, x.`type`, new DateTime(x.created_at.getTime).getSecondOfMinute), x.actor)).groupByKey().map(x => (x._1, x._2.size))
    rddRepoTypeSecondActor.take(20).foreach(println)

    //Il massimo/minimo numero di «actor» attivi per secondo;
    val rddNumActorXSecond = jsonRdd.map(x => (((new DateTime(x.created_at.getTime).getSecondOfMinute)), x.actor)).groupByKey().map(x => (x._2.size, x._1))
    println(rddNumActorXSecond.max())
    println(rddNumActorXSecond.min())

    //Il massimo/minimo numero di «actor» attivi per secondo, «type»;
    val rddNumActorXSecondType = jsonRdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.`type`), x.actor)).groupByKey().map(x => (x._2.size, x._1))
    println(rddNumActorXSecondType.max())
    println(rddNumActorXSecondType.min())

    //Il massimo/minimo numero di «actor» attivi per secondo, «type» e «repo»;
    val rddNumActorXSecondTypeRepo = jsonRdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.`type`, x.repo), x.actor)).groupByKey().map(x => (x._2.size, x._1.toString()))
    println(rddNumActorXSecondTypeRepo.max())
    println(rddNumActorXSecondTypeRepo.min())
*/

  }
}

