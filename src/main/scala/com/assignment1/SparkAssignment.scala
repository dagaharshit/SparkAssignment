package com.assignment1

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.{Success, Try}

object SparkAssignment extends App {

  val conf = new SparkConf().setAppName("appName").setMaster("local")
  val sc = new SparkContext(conf)

  val pageCounts = sc.textFile("/home/knoldus/Downloads/pagecounts-20151201-220000", 4).cache()

  val answer2 = pageCounts.take(10)

  println("===>> Answer2 ::\n ")
  answer2.foreach { res => println(res) }

  val answer3 = pageCounts.count

  println("===>> Answer3 :: " + answer3)

  val answer4 = pageCounts.map(_.split(" ")).filter { res =>
    res(0) == "en" }

  val answer5 = answer4.count

  println("===>> Answer5 ::" + answer5)

  val answer6 = pageCounts.filter { line =>
    val reqCount = line.split(" ")(2)
    reqCount.toLong >= 200000
  }

  println("===> Total number of records for Question6 :: " + answer6.count)

  sc.stop()
}
