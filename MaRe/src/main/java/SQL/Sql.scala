package SQL

import Mare.{MaRe, TextFile}
import org.apache.spark.{SparkConf, SparkContext}

object Sql extends App {
  val conf = new SparkConf().setMaster("local").setAppName("Project")
  val sc = new SparkContext(conf)


  val rdd = sc.textFile("players_20.csv")
  val res = new MaRe(rdd)
    .map(
      inputMountPoint = TextFile("/input"),
      outputMountPoint = TextFile("/output"),
      imageName = "sql",
      command = "java -jar sql.jar > output")
    .rdd.collect()

  res.foreach(println(_))

}
