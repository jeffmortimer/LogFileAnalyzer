package com.github.jeffmortimer.logfileanalyzer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.log4j._

object LogFileAnalyzer extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc: SparkContext = new SparkContext("local[*]", "LogFileAnalyzer")

  // The LogFileLoader parses the Apache log file to create key/value pairs, where the key is the date/time
  // concatenated with the ip address, and the value is the integer 1.
  val logLines: RDD[(String, Int)] = LogFileLoader.load(sc, "apache-access-log.txt")
  // println("============= Top 20 rows of input data =============")
  // logLines.take(20).foreach(println)
  // println("\r\n")

  // Now use reduce to add up the counts, filter out the "non-offenders", and sort by the key.
  val logLinesByFileCount: RDD[(String, Int)] = logLines.reduceByKey((x, y) => x + y)
    .filter(x => x._2 > 1)
    .sortByKey()
  println("============= Possible DDoS attacks =============")
  logLinesByFileCount.foreach(println)

}
