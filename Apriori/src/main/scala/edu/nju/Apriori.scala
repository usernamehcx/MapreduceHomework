package edu.nju

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks

object Apriori {
  // build SparkContext
  val cf = new SparkConf()
  cf.setAppName("Apriori")
  cf.setMaster("local")
  val sc = new SparkContext(cf)

  def solution(inPath: String, outPath: String, minSup: Float): Unit = {
    val txtFile = sc.textFile(inPath)
    val transactions: RDD[Set[String]] = txtFile.map(line => line.split(" ").toSet)
    // candidate 1-frequent set
    val iss: Array[Set[String]] = transactions.flatMap(line => line).distinct().collect().map(Set(_)) // item sets
    // broadcast transactions and candidate 1-frequent set
    val tran = sc.broadcast(transactions.collect())
    val biss = sc.broadcast(iss)
    val mydata = tran.value
    var itemsets = biss.value
    println(itemsets.mkString(","))

    // the number of transactions
    val transLen = mydata.length

    // all the frequent set
    var L = Array[(Set[String], Float)]()

    var count = 0
    val loop = new Breaks
    loop.breakable {
      // when candidate itemsets is empty or its length is no greater than 1, the loop will break
      while (itemsets.nonEmpty && itemsets.length > 1) {
        count += 1
        /*
          get k-frequent set
          map: emit <itemset, support> for itemset in candidate itesets, when support of itemset is not less than minSup
          reduce: remove redundant itemset
         */
        // mydata is a global data set
        val L_k = sc.parallelize(itemsets)
          .map(a =>
            if (mydata.count(data => a.subsetOf(data)) / transLen.toFloat >= minSup) {
              (a, mydata.count(data => a.subsetOf(data)) / transLen.toFloat)
            } else {
              (Set[String](), -1.toFloat)
            }
          )
          .filter(_._2 != -1.toFloat)
          .reduceByKey((x, _) => x)
          .collect()
        println(L_k.mkString("\n"))

        //reformat k-frequent set
        val re_L_k = L_k.map { a: (Set[String], Float) => a }

        // append k-frequent set to L
        if (re_L_k.nonEmpty) {
          L = re_L_k
        }

        /*
          prepare next iteration: get (k+1)-candidate set
          first we need to broadcast L_k to let spark workers know the entire set of L_k
          map:
            for itemset in L_k:
              for item in itemset:
                findSet(item, L_k)

          findSet: item is added into each itemset of L_k. return a set containing all the changed itemsets
          reduce: remove redundant itemset
         */
        val b_L_k = sc.broadcast(L_k).value
        var next = Set[Set[String]]()
        try {
          next = sc.parallelize(L_k)
            .map(itemset => itemset._1
              .map(item => findSet(item, b_L_k))
              .reduce((x, y) => if (y != null && x != null) x ++ y else null)
            )
            .reduce((x, y) => if (y != null && x != null) x ++ y else null)
            .collect(pf = PartialFunction.apply(a => a))
        }
        catch {
          case foo: UnsupportedOperationException => println("empty collection")
          // handling any other exception that might come up
          case unknown => println("Got this unknown exception: " + unknown)
        }
        finally {
          itemsets = next.toArray
        }

      }
    }
    //  append frequent set and its support, and save them to hdfs
    var result = Array[String]()
    println(s"极大频繁项集的项数为${L.length}项：")
    var str = ""
    for (i <- L) {
        str = f"[${i._1.mkString(",")}%s]: ${i._2}%.2f"
        println(str)
        result = result :+ str
    }
    sc.parallelize(result).saveAsTextFile(outPath)
  }

  /*
    findSet: item is added into each itemset of L_k. return a set containing all the changed itemsets
   */
  def findSet(item: String, b_L_k: Array[(Set[String], Float)]): Set[Set[String]] = {
    var setList = Set[Set[String]]()
    for (j <- b_L_k) {
      val unionSet = j._1 + item
      if (unionSet.size == j._1.size + 1) {
        setList += unionSet
      }
    }
    setList
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: spark-submit --class edu.nju.Apriori --master spark://localhost:60123 mapreduce_2.11-0.1.jar <inputPath> <outPath> <minSupp>")
      System.exit(0)
    }
    solution(args(0), args(1), args(2).toFloat)
  }
}
