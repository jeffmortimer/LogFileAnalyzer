package com.github.jeffmortimer.logfileanalyzer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object LogFileLoader {

  def load(sc: SparkContext, filename: String): RDD[(String, Int)] = {
    val lines: RDD[String] = sc.textFile(filename)
    val rdd: RDD[(String, Int)] = lines.map(parseLine)
    rdd
  }

  // From the line of the apache log file, return a key value tuple where the key is the
  // date/time and ip address concatenated together, and the value is the integer 1.
  // We'll use reduce to count up the number of times that particular ip address appears
  // for that particular second.
  def parseLine(line: String): (String, Int) = {
    val fields = line.split(" ")
    val ipAddress = fields(0)
    // The date/time in the log file looks like this: [25/May/2015:23:11:15 +0000]
    val timeWithBrackets = fields(3) + " " + fields(4)
    val timeNoBrackets = timeWithBrackets.substring(1, timeWithBrackets.length - 1)
    (timeNoBrackets + " " + ipAddress, 1)
  }

}
