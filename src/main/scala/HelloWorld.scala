import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {

  def main(args: Array[String]): Unit = {
    println("Hello World!");

    val conf = new SparkConf().setAppName("SparkApp").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

    distData.mapPartitionsWithIndex((index: Int, it: Iterator[Int])
    => it.toList.map(x => if (index == 0) {
        println(x + " " + index)
      }).iterator).collect


    // Creare un RDD dei numeri da 1 a 100
    val array100 = 1 to 100
    val rdd100 = sc.parallelize(array100)
    rdd100.foreach(println)

    // Calcolare il quadrato di ogni numero
    val rddSquare = rdd100.map(x => x*x)
    rddSquare.foreach(println)

    //Rimuovere i numeri che non sono divibili per 3
    val rddFiltered = rdd100.filter(x => x % 3 == 0)

    //Rimuovere i numeri che non sono divibili per 3 con placeholder
    val rddFilteredWithPlaceholder = (rdd100.filter(_ % 3 == 0))
    rddFilteredWithPlaceholder.foreach(println)

    //Creare un rdd con i numeri da 1 a 5.
    // Creare poi un RDD dove al posto di ogni elemento precedente, è presente un Array degli elementi da 0 a n
    println("RDD in RDD")
    val rddOfRddMap = distData.map(x => 1 to x)
    rddOfRddMap.foreach(println)

    val rddOfRddFlatMap = distData.flatMap(x =>1 to x)
    rddOfRddFlatMap.foreach(println)

    //Rimuovere i valori duplicati
    val rddDistint = rddOfRddFlatMap.distinct()
    rddDistint.foreach(println)

    //Somma di un RDD
    val rdd100Sum = rdd100.sum()
    println(rdd100Sum)












  }

}
