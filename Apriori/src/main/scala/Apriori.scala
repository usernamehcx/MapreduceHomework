import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks

object Apriori {
  val cf = new SparkConf()
  cf.setAppName("Apriori")
  cf.setMaster("local")
  val sc = new SparkContext(cf)

  def solution(inPath: String, outPath: String, minSup: Float): Unit = {
    val txtFile = sc.textFile(inPath)
    val transactions: RDD[Set[String]] = txtFile.map(line => line.split(" ").toSet)
    //    println(transactions.collect().mkString(","))

    var itemsets: Array[Set[String]] = transactions.flatMap(line => line).distinct().collect().map(Set(_)) // item sets

    val mydata = transactions.collect()
    println(itemsets.mkString(","))

    // the number of transactions
    val transLen = mydata.length
    // frequent set
    var L = Array[Array[(Set[String], Float)]]()

    var count = 0
    val loop = new Breaks
    loop.breakable {
      while (itemsets.nonEmpty && itemsets.length > 1) {
        count += 1

        //get candidatedate
        var candidate = sc.parallelize(itemsets)
          .map(a =>
            if (mydata.count(data => a.subsetOf(data)) / transLen.toFloat >= minSup) {
              Tuple2(a, mydata.count(data => a.subsetOf(data)) / transLen.toFloat)
            } else {
              (-1, -1)
            }
          )
          .filter(_ != (-1, -1)).collect()
        println(candidate.mkString("\n"))

        val k = candidate.map {
          case a: (Set[String], Float) => a.asInstanceOf[(Set[String], Float)]
        }

        L = L :+ k

        // prepare for next iteration
        var nextItemSets = Set[Set[String]]()
        for (i <- k.indices) {
          for (j <- i + 1 until k.length) {
            val unionSet = k(i)._1.|(k(j)._1)
            nextItemSets += unionSet
          }
        }
        itemsets = nextItemSets.toArray
        //                loop.break
      }
    }
    var result = Array[String]()
    var str = ""
    for (item <- L) {
      for (i <- item) {
        str = f"${i._1.mkString(",")}%s: ${i._2}%.2f"
        println(str)
        result = result :+ str
      }
      println()
    }
    sc.parallelize(result).saveAsTextFile(outPath)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3){
      println("Useage: <inputPath> <outPath> <minSupp>")
      System.exit(0)
    }
    solution(args(0), args(1), args(2).toFloat)
  }
}
