  package com.exercise.books

  import org.apache.spark.sql.Row
  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.hive.HiveContext
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


  object Main {

    val conf = new SparkConf().setAppName("MySparkApp").setMaster("local[2]")
    val sparkContext = new SparkContext(conf)
    sparkContext.setCheckpointDir("C:\\Users\\Daniele\\IdeaProjects\\LynxBigData\\DAG")

    def main(args: Array[String]): Unit = {
      val autore = Autore(1, "Mario", "Rossi", 35)
      val autore_second = Autore(2, "Giuseppe", "Verdi", 46)

      val libro1 = Libro(1, "libroA", 123, 1, "1978")
      val libro2 = Libro(2, "libroB", 239, 2, "1989")
      val libro3 = Libro(3, "libroC", 987, 1, "1999")
      val libro4 = Libro(4, "libroD", 765, 2, "1980")
      val libro5 = Libro(5, "libroE", 198, 1, "1978")

      val autori = Array(autore, autore_second)
      val libri = Array(libro1, libro2, libro3,libro3, libro4, libro5)

      val rddAutori = sparkContext.parallelize(autori)
      val rddLibri = sparkContext.parallelize(libri)

      //libri raggruppati per autore
      val libriTupla2 = rddLibri.map(x => (x.idAutore,x)).distinct()
      val libriAutore = libriTupla2.groupByKey()
      libriTupla2.checkpoint()
      libriAutore.foreach(println)

      //libri di autore conteneti il maggior numero di pagine
      val rddMaxPage = libriTupla2.reduceByKey((libro1:Libro, libro2:Libro) => {
        if(libro1.numeroPagine>libro2.numeroPagine)
          libro1
        else
          libro2
      })
      rddMaxPage.foreach(println)

      //numero totale di pagine scritte da un autore
      def seqOp(pagine:Int, libro1: Libro):Int ={
        pagine + libro1.numeroPagine
      }
      def comOp(pagine:Int, pagine2:Int):Int ={
        pagine + pagine2
      }
      val rddTotPage = libriTupla2.aggregateByKey(0)(seqOp,comOp)
      ////////////////////////////////////////////////////////////////////
      val rddTotPage2 = libriTupla2.aggregateByKey(0)(_+_.numeroPagine,_+_)
      ////////////////////////////////////////////////////////////////////
      val rddAutorePagine = rddLibri.map(x => (x.idAutore,x.numeroPagine))
      //val rddTotPage3 = rddAutorePagine.reduceByKey((x,y)=> (x+y))
      val rddTotPage3 = rddAutorePagine.reduceByKey(_+_)
      ////////////////////////////////////////////////////////////////////
      val rddPagesAuthor1 = rddLibri.filter(x => x.idAutore == 1).map(x=>(x.numeroPagine)).sum()
      val paginePrimoAutore = (autore, rddPagesAuthor1)
      val rddPagesAuthor2 = rddLibri.filter(x => x.idAutore == 2).map(x=>(x.numeroPagine)).sum()
      val pagineSecondoAutore = (autore_second, rddPagesAuthor2)
      //println(paginePrimoAutore, pagineSecondoAutore)
      ////////////////////////////////////////////////////////////////////
      rddTotPage.foreach(println)

      //Stampa del DAG
      println(rddTotPage.toDebugString)

      /////////////////////////////////////////////////////ESERCITAZIONE 2////////////////////////////////////////////////////////////////

      val rddTupla3 = rddLibri.map(x=>(x.idLibro,x.idAutore,x.annoDiPubblicazione))

      val rddTupla3Filtered = rddTupla3.filter(x=> x._3 > "1978")

      val rddTupla3Distinct = rddTupla3.distinct()

      val rddFirstWritten = libriTupla2.reduceByKey((libro1:Libro, libro2:Libro) => {
        if(libro1.annoDiPubblicazione<libro2.annoDiPubblicazione)
          libro1
        else
          libro2
      })
      rddFirstWritten.foreach(println)

      val newRdd = rddLibri.map(x=>(x.annoDiPubblicazione,x))
      val newRdd2 = newRdd.groupByKey()
      newRdd2.foreach(println)
      val newRdd3 = newRdd2.map(x=>x._2)
      newRdd3.foreach(println)
      //TODO verificare se l'ultimo punto è fatto correttamente


      /////////////////////////////////////////DATAFRA////////////////////////////////////////

      val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
      val hiveContext = new HiveContext(sparkContext)
      import hiveContext.implicits._

      //Creazione DataFrame partendo da rddLibri
      val myRDD = rddLibri.distinct()
      val dfFromRdd = myRDD.toDF()
      dfFromRdd.show()

      //Creazione DataFrame partendo da uno schema
      val schema = StructType(Seq(
        StructField("idLibro", IntegerType, false),
        StructField("nomeLibro", StringType, false),
        StructField("numeroPagine", IntegerType, false),
        StructField("idAutore", IntegerType, false),
        StructField("annoDiPubblicazione", StringType, false)))

      val row = Row(1, "libroA", 1, 1, "1978")
      val row2 = Row(2, "libroB", 2, 2, "1989")
      val row3 = Row(3, "libroC", 3, 1, "1999")
      val row4 = Row(4, "libroD", 4, 2, "1980")
      val row5 = Row(5, "libroE", 5 , 1, "1978")

      val libriRow = Array(row, row2, row3, row4, row5)
      val rddLibriRow = sparkContext.parallelize(libriRow)

      val dfFromSchema = sqlContext.createDataFrame(rddLibriRow,schema)
      dfFromSchema.show()


      //Recuperare il max numero di pagine per autore (con le window function)

      val result = dfFromRdd.select($"idLibro", $"idAutore", $"numeroPagine",
        max($"numeroPagine").over(Window.partitionBy($"idAutore")) as "maxPagesPerAuthor")

      val result2 = dfFromRdd.select($"idLibro", $"numeroPagine", $"idAutore",
        sum($"numeroPagine").over(Window.partitionBy($"idAutore")) as "maxPagesPerAuthor")

      result.show()

      //Aggiungere all’elenco di libri-autori la differenza di pagine di ogni libro con il max per autore

      val diffPagesAuthor = result.select($"idLibro", $"idAutore", $"numeroPagine", $"maxPagesPerAuthor")
        .withColumn("diffPagesPerAuthor",$"maxPagesPerAuthor" - $"numeroPagine")

      val diffPagesAuthor2 = result.select($"idLibro", $"idAutore", $"numeroPagine", $"maxPagesPerAuthor",
        ($"maxPagesPerAuthor").minus(($"numeroPagine")) as "diffPagesPerAuthor")

      diffPagesAuthor2.show()

      //Associare ad ogni libro il libro precedente e successivo pubblicato dallo stesso autore

      val libriHistory = dfFromRdd.select($"idAutore", $"idLibro", $"nomeLibro", $"annoDiPubblicazione",
        lag($"nomeLibro",1).over(Window.partitionBy($"idAutore").orderBy($"annoDiPubblicazione")) as "Libro precedente",
        lag($"annoDiPubblicazione",1).over(Window.partitionBy($"idAutore").orderBy($"annoDiPubblicazione")) as "AnnoP",
        lead($"nomeLibro",1).over(Window.partitionBy($"idAutore").orderBy($"annoDiPubblicazione")) as "Libro successivo",
        lead($"annoDiPubblicazione",1).over(Window.partitionBy($"idAutore").orderBy($"annoDiPubblicazione")) as "AnnoS")

      libriHistory.show()

      ////////////////////////////////////////////DATASET/////////////////////////////////////////////////////////////////////

      val dataset = dfFromRdd.as[Libro]
      dataset.show()



    }

  }
