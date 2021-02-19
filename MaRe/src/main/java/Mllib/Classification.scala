package Mllib

import Mare.{MaRe, TextFile}
import org.apache.spark.{SparkConf, SparkContext}

object Classification extends App {
  val conf = new SparkConf().setMaster("local").setAppName("Project")
  val sc = new SparkContext(conf)


  val rdd = sc.textFile("tennis.csv")
  val res = new MaRe(rdd)
    .map(
      inputMountPoint = TextFile("/input"),
      outputMountPoint = TextFile("/output"),
      imageName = "mllib",
      command = "java -cp project.jar Classification > output")
    .rdd.collect()

  res.foreach(println(_))
}
