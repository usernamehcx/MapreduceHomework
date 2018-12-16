import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.control.Breaks

object Apriori {
  def main(args: Array[String]): Unit = {
    val cf = new SparkConf()
    cf.setAppName("Apriori")
    cf.setMaster("local")
    val sc = new SparkContext(cf)

    val minSup = 0.5
    val txtFile = sc.textFile("hdfs://localhost:9000/hadoop/data.txt")
    val mydata = txtFile.collect()
    var data = Set[Set[String]]() // file data
    var itemsets = Set[Set[String]]() // item sets

    // get itemsets
    for (i <- mydata.indices) {
      val d = mydata(i).split(" ")
      data += d.toSet
      for (j <- d) {
        itemsets += Set[String](j)
      }
    }

    // the number of transactions
    val transLen = data.size

    val loop = new Breaks
    loop.breakable {
      while (itemsets.nonEmpty && itemsets.size > 1) {

        //get candidate
        val candidate = mutable.HashMap[Set[String], Int]()
        for (itemset <- itemsets) {
          for (transaction <- data) {
            if (itemset.&(transaction).equals(itemset)) {
              if (candidate.contains(itemset)) {
                candidate(itemset) += 1
              } else {
                candidate(itemset) = 1
              }
            }
          }
        }

        // get frequent k item set
        val L_k = candidate.filter(l => l._2 / transLen.toFloat >= minSup)

        // prepare for next iteration

        println(candidate.mkString("#"))
        println(L_k.mkString("@"))
        loop.break
      }
    }
    println(itemsets.mkString(":"))
    println(data.mkString(","))


    //    println(data(0).mkString(","))
    //
  }
}
