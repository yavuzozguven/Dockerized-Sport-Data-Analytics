package com.yavuzozguven

import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {
  val conf = new SparkConf().setMaster("local").setAppName("Project")
  val sc = new SparkContext(conf)


  val rdd = sc.textFile("players_20.csv")
  /*val res = new com.yavuzozguven.MaRe(rdd)
    .map(
      inputMountPoint = com.yavuzozguven.TextFile("/sql"),
      outputMountPoint = com.yavuzozguven.TextFile("/out"),
      imageName = "sql",
      command = "java -jar sql.jar > out")
    .rdd.collect()

  res.foreach(println(_))*/


  /*val res = new com.yavuzozguven.MaRe(rdd)
    .map(
      inputMountPoint = com.yavuzozguven.TextFile("/dna"),
      outputMountPoint = com.yavuzozguven.TextFile("/count.txt"),
      imageName = "mllib",
      command = "java -jar mllib.jar > count.txt")
    reduce(
      inputMountPoint = com.yavuzozguven.TextFile("/counts"),
      outputMountPoint = com.yavuzozguven.TextFile("/sum"),
      imageName = "ubuntu",
      command = "awk '{s+=$1} END {print s}' /counts > /sum")
    .rdd.collect()*/

  /*val res = new com.yavuzozguven.MaRe(rdd)
    .map(
      inputMountPoint = com.yavuzozguven.TextFile("/dna"),
      outputMountPoint = com.yavuzozguven.TextFile("/count.txt"),
      imageName = "mllib",
      command = "java -cp mllib.jar com.yavuzozguven.Classification > count.txt")
    .rdd.collect()*/


  val res = new MaRe(rdd)
    .map(
      inputMountPoint = TextFile("/input"),
      outputMountPoint = TextFile("/output"),
      imageName = "graphx",
      command = "java -jar project.jar avg > output")
    .rdd.collect()

  res.foreach(println(_))


}
