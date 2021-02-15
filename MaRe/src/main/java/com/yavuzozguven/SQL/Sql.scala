package com.yavuzozguven.SQL

import org.apache.spark.{SparkConf, SparkContext}

object Sql extends App {
  val conf = new SparkConf().setMaster("local").setAppName("Project")
  val sc = new SparkContext(conf)


  val rdd = sc.textFile("players_20.csv")
  val res = new com.yavuzozguven.MaRe(rdd)
    .map(
      inputMountPoint = com.yavuzozguven.TextFile("/input"),
      outputMountPoint = com.yavuzozguven.TextFile("/output"),
      imageName = "sql",
      command = "java -jar sql.jar > output")
    .rdd.collect()

  res.foreach(println(_))

}
