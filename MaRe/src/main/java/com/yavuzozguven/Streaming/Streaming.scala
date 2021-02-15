package com.yavuzozguven.Streaming

import org.apache.spark.{SparkConf, SparkContext}

object Streaming extends App{
  val conf = new SparkConf().setMaster("local").setAppName("Project")
  val sc = new SparkContext(conf)


  val rdd = sc.textFile("players_20.csv")
  val res = new com.yavuzozguven.MaRe(rdd)
      .map(
        inputMountPoint = com.yavuzozguven.TextFile("/input"),
        outputMountPoint = com.yavuzozguven.TextFile("/output"),
        imageName = "anomaly",
        command = "java -jar project.jar > output")
      .rdd.collect()

    res.foreach(println(_))
}
