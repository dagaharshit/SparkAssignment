package com.assignment

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkAssignment2 extends App {

  val conf = new SparkConf().setAppName("appName").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().appName("").getOrCreate()

  import spark.implicits._

  val df = spark.read.option("header", true).csv("/home/knoldus/Downloads/Fire_Department_Calls_for_Service.csv")
  val result1 = df.select(df("Call Type")).distinct

  println(s"Call Types:::: ${result1.show(result1.count.toInt, false)}")
  val result2 = df.select(df("Call Type"), df("Incident Number")).groupBy(df("Call Type")).count
  result2.show

  val fireDepartmentDf = df.map { row =>
    val dateFormat = new SimpleDateFormat("mm/dd/yyyy")
    val date = new Timestamp(dateFormat.parse(row.getAs[String](4)).getTime)
    val neighbour = row.getAs[String](31)
    FireDepartment(row.getAs[String](0), date, neighbour)
  }.toDF

  fireDepartmentDf.createOrReplaceTempView("fire_department")

  val result3 = spark.sql("SELECT DATEDIFF(MAX(callDate), MIN(callDate)) FROM fire_department").as[Int]
  result3.show(false)
  fireDepartmentDf.select(fireDepartmentDf("callDate"))

  val result4 = spark.sql("SELECT count(*) from fire_department where callDate between " +
    "(select DATE_SUB(MAX(callDate), 7) from fire_department) AND (SELECT MAX(callDate) from fire_department)")

  // solution 5
  spark.sql("select count(callNumber), neighbour from fire_department group by neighbour order by count(callNumber) desc").show(false)
}

case class FireDepartment(callNumber: String, callDate: java.sql.Timestamp, neighbour: String)
